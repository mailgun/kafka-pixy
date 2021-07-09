package mapper

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/prettyfmt"
)

// T maintains mapping of workers that generate requests to executors. An
// executor is associated with a particular Kafka broker. It aggregates worker
// requests, marshals them to a Kafka protocol packet, sends the packet to the
// associated broker, waits for response and fans replies out to the workers.
// An external resolver is used to determine worker to broker-executor
// assignments.
//
// Mapper triggers reassignment whenever one of the following events happen:
//   * it is signaled that a new worker has been spawned via `OnWorkerSpawned()`;
//   * it is signaled that an existing worker has stopped via `OnWorkerStopped()`;
//   * a worker explicitly requested reassignment via `TriggerReassign()`
//   * an executor reported connection error via `BrokerFailed()`.
//
// Executors are spawned on demand when a broker is resolved to a worker for
// the first time. It is guaranteed that a executor is stopped only after all
// workers that used to be assigned to it have either been stopped or assigned
// another to other executors.
type T struct {
	actDesc     *actor.Descriptor
	cfg         *config.Proxy
	resolver    Resolver
	eventsCh    chan event
	assignments map[Worker]Executor
	references  map[Executor]int
	connections map[*sarama.Broker]Executor
	stopCh      chan none.T
	wg          sync.WaitGroup
}

// Resolver defines an interface to resolve a broker connection that should
// serve requests of a particular worker, and to create an executor for a
// broker connection.
type Resolver interface {
	// ResolveBroker returns a broker connection that should be used to
	// determine an executor assigned to the specified worker.
	ResolveBroker(worker Worker) (*sarama.Broker, error)

	// SpawnExecutor spawns an executor for the specified connection.
	SpawnExecutor(brokerConn *sarama.Broker) Executor
}

// Worker represents an entity that makes requests via an assigned broker
// executor.
type Worker interface {
	// assignment returns a channel that the worker expects broker assignments
	// at. Implementations have to ensure that the channel has a non zero buffer
	// and that they read from this channel as soon as the value becomes
	// available, for mapper will drop assignments in case the write to the
	// channel may block.
	Assignment() chan<- Executor
}

// Executor represents an entity that executes requests of workers
// via a particular broker connection.
type Executor interface {
	// BrokerConn returns a broker connection used by the executor.
	BrokerConn() *sarama.Broker

	// Stop synchronously stops the executor.
	Stop()
}

type eventType int

type event struct {
	t eventType
	w Worker
}

const (
	evWorkerSpawned eventType = iota
	evWorkerStopped
	evReassignNeeded
)

var (
	eventNames = []string{"spawned", "stopped", "outdated"}

	testMode       bool
	testSkipEvents int32
)

func (et eventType) String() string {
	return eventNames[et]
}

// Spawn creates a mapper instance and starts its internal goroutines.
func Spawn(parentActDesc *actor.Descriptor, cfg *config.Proxy, resolver Resolver) *T {
	m := &T{
		actDesc:     parentActDesc.NewChild("mapper"),
		cfg:         cfg,
		resolver:    resolver,
		eventsCh:    make(chan event, cfg.Consumer.ChannelBufferSize),
		assignments: make(map[Worker]Executor),
		references:  make(map[Executor]int),
		connections: make(map[*sarama.Broker]Executor),
		stopCh:      make(chan none.T),
	}
	actor.Spawn(m.actDesc, &m.wg, m.run)
	return m
}

func (m *T) OnWorkerSpawned(w Worker) {
	m.eventsCh <- event{evWorkerSpawned, w}
}

func (m *T) OnWorkerStopped(w Worker) {
	m.eventsCh <- event{evWorkerStopped, w}
}

func (m *T) TriggerReassign(w Worker) {
	m.eventsCh <- event{evReassignNeeded, w}
}

func (m *T) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// run listens for mapping events, batches them into a mappingChange object and
// triggers reassignments, making sure to run only one at a time. When signaled
// to stop it only quits when all children have closed.
func (m *T) run() {
	var (
		changes             = make(map[Worker]eventType)
		reassignDoneCh      = make(chan none.T, 1)
		lastChangeTimes     = make(map[Worker]clock.Time)
		deferredChanges     = make(map[Worker]clock.Time)
		nilOrDeferredCh     <-chan clock.Time
		nilOrReassignDoneCh <-chan none.T
		stop                = false
		nilOrStopCh         = m.stopCh
	)
	for {
		select {
		case ev := <-m.eventsCh:
			now := clock.Now().UTC()
			switch ev.t {
			case evWorkerSpawned:
				changes[ev.w] = ev.t
				lastChangeTimes[ev.w] = now

			case evReassignNeeded:
				if now.Sub(lastChangeTimes[ev.w]) < m.cfg.Consumer.RetryBackoff {
					if _, ok := deferredChanges[ev.w]; !ok {
						deferredChanges[ev.w] = now.Add(m.cfg.Consumer.RetryBackoff)
						if nilOrDeferredCh == nil {
							nilOrDeferredCh = clock.After(m.cfg.Consumer.RetryBackoff)
						}
					}
					break
				}
				changes[ev.w] = ev.t
				lastChangeTimes[ev.w] = now
				delete(deferredChanges, ev.w)

			case evWorkerStopped:
				changes[ev.w] = ev.t
				delete(lastChangeTimes, ev.w)
				delete(deferredChanges, ev.w)
			}
		case <-nilOrReassignDoneCh:
			nilOrReassignDoneCh = nil

		case <-nilOrDeferredCh:
			nilOrDeferredCh = nil
			now := clock.Now().UTC()
			nextRetryAt := now.Add(m.cfg.Consumer.RetryBackoff)
			for w, deadline := range deferredChanges {
				if deadline.After(now) {
					if deadline.Before(nextRetryAt) {
						nextRetryAt = deadline
					}
					continue
				}
				changes[w] = evReassignNeeded
				lastChangeTimes[w] = now
				delete(deferredChanges, w)
			}
			if len(deferredChanges) >= 0 {
				nilOrDeferredCh = clock.After(nextRetryAt.Sub(now))
			}
		case <-nilOrStopCh:
			nilOrStopCh = nil
			stop = true
		}
		// Allows control over what change should trigger reassignment in tests.
		if testMode {
			skip := atomic.AddInt32(&testSkipEvents, -1)
			if skip >= 0 {
				continue
			}
		}
		// If there are changes to apply, and there is no reassign goroutine
		// running at the moment then spawn one.
		if len(changes) > 0 && nilOrReassignDoneCh == nil {
			m.actDesc.Log().Infof("Reassign initiated: changes=%s", prettyfmt.Val(changes))
			reassignActDesc := m.actDesc.NewChild("reassign")
			frozenChanges := changes
			actor.Spawn(reassignActDesc, nil, func() {
				m.reassign(reassignActDesc, frozenChanges, reassignDoneCh)
			})
			changes = make(map[Worker]eventType)
			nilOrReassignDoneCh = reassignDoneCh
		}
		// Do not leave this loop until all workers are closed.
		if stop && nilOrReassignDoneCh == nil && len(m.assignments) == 0 {
			return
		}
	}
}

// reassign updates partition-to-broker assignments using the external resolver.
func (m *T) reassign(actDesc *actor.Descriptor, changes map[Worker]eventType, doneCh chan none.T) {
	defer func() { doneCh <- none.V }()

	for worker, event := range changes {
		switch event {
		case evWorkerStopped:
			executor := m.assignments[worker]
			if executor == nil {
				continue
			}
			delete(m.assignments, worker)
			m.references[executor] = m.references[executor] - 1
			actDesc.Log().Infof("Unassign: worker=%s, executor=%s(ref=%d)", worker, executor, m.references[executor])

		case evWorkerSpawned:
			executor := m.assignments[worker]
			if executor != nil {
				continue
			}
			m.resolveBroker(actDesc, worker)

		case evReassignNeeded:
			m.resolveBroker(actDesc, worker)
		}
	}
	// All broker assignments have been propagated to workers, so it is safe to
	// close executors that are not used anymore.
	for executor, referenceCount := range m.references {
		if referenceCount != 0 {
			continue
		}
		actDesc.Log().Infof("Stopping executor: %s", executor)
		executor.Stop()
		delete(m.references, executor)
		if m.connections[executor.BrokerConn()] == executor {
			delete(m.connections, executor.BrokerConn())
		}
	}
}

// resolveBroker determines a broker connection for the worker and assigns
// executor associated with the broker connection to the worker. If there is
// no such executor then it is created.
func (m *T) resolveBroker(actDesc *actor.Descriptor, w Worker) {
	var newExec Executor
	brokerConn, err := m.resolver.ResolveBroker(w)
	if err != nil {
		actDesc.Log().WithError(err).Errorf("Failed to resolve broker: worker=%s", w)
	} else if brokerConn == nil {
		actDesc.Log().Errorf("Nil broker resolved: worker=%s", w)
	} else {
		newExec = m.connections[brokerConn]
		if newExec == nil {
			newExec = m.resolver.SpawnExecutor(brokerConn)
			actDesc.Log().Infof("Executor spawned: %s", newExec)
			m.connections[brokerConn] = newExec
		}
	}
	// Assign the new executor, but only if the operation does not block.
	if newExec != nil {
		select {
		case w.Assignment() <- newExec:
		default:
			newExec = nil
		}
	}
	// If we failed to resolve or assign an executor, make sure we retry later.
	if newExec == nil {
		m.TriggerReassign(w)
	}
	// Update old executor references and assignment.
	oldExecRepr := "<nil>"
	oldExec := m.assignments[w]
	if oldExec != nil {
		delete(m.assignments, w)
		m.references[oldExec] -= 1
		oldExecRepr = fmt.Sprintf("%s(ref=%d)", oldExec, m.references[oldExec])
	}
	// Update new executor references and assignment.
	newExecRepr := "<nil>"
	if newExec != nil {
		m.assignments[w] = newExec
		m.references[newExec] += 1
		newExecRepr = fmt.Sprintf("%s(ref=%d)", newExec, m.references[newExec])
	}

	actDesc.Log().Infof("Assigned: worker=%s, executor=%s, old=%s",
		w, newExecRepr, oldExecRepr)
}
