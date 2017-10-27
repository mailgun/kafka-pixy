package mapper

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/none"
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

	eventsChBufSize = 32
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
func Spawn(parentActDesc *actor.Descriptor, resolver Resolver) *T {
	m := &T{
		actDesc:     parentActDesc.NewChild("mapper"),
		resolver:    resolver,
		eventsCh:    make(chan event, eventsChBufSize),
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
	changes := make(map[Worker]eventType)
	reassignDoneCh := make(chan none.T, 1)
	var nilOrRedispatchDoneCh <-chan none.T
	stop := false
	for {
		select {
		case ev := <-m.eventsCh:
			changes[ev.w] = ev.t

		case <-nilOrRedispatchDoneCh:
			nilOrRedispatchDoneCh = nil

		case <-m.stopCh:
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
		if len(changes) > 0 && nilOrRedispatchDoneCh == nil {
			m.actDesc.Log().Infof("reassign: changes=%s", changes)
			reassignActDesc := m.actDesc.NewChild("reassign")
			frozenChanges := changes
			actor.Spawn(reassignActDesc, nil, func() {
				m.reassign(reassignActDesc, frozenChanges, reassignDoneCh)
			})
			changes = make(map[Worker]eventType)
			nilOrRedispatchDoneCh = reassignDoneCh
		}
		// Do not leave this loop until all workers are closed.
		if stop && nilOrRedispatchDoneCh == nil && len(m.assignments) == 0 {
			return
		}
	}
}

// reassign updates partition-to-broker assignments using the external resolver.
func (m *T) reassign(actDesc *actor.Descriptor, change map[Worker]eventType, doneCh chan none.T) {
	defer func() { doneCh <- none.V }()

	for worker, event := range change {
		switch event {
		case evWorkerStopped:
			executor := m.assignments[worker]
			if executor == nil {
				continue
			}
			delete(m.assignments, worker)
			m.references[executor] = m.references[executor] - 1
			actDesc.Log().Infof("unassign %s -> %s (ref=%d)", worker, executor, m.references[executor])

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
		actDesc.Log().Infof("decomission %s", executor)
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
func (m *T) resolveBroker(actDesc *actor.Descriptor, worker Worker) {
	var newExecutor Executor
	brokerConn, err := m.resolver.ResolveBroker(worker)
	if err != nil {
		actDesc.Log().WithError(err).Errorf("Failed to resolve broker: worker=%s", worker)
	} else if brokerConn == nil {
		actDesc.Log().Errorf("Nil broker resolved: worker=%s", worker)
	} else {
		newExecutor = m.connections[brokerConn]
		if newExecutor == nil {
			newExecutor = m.resolver.SpawnExecutor(brokerConn)
			actDesc.Log().Infof("spawned %s", newExecutor)
			m.connections[brokerConn] = newExecutor
		}
	}
	// Assign the new executor, but only if it does not block.
	select {
	case worker.Assignment() <- newExecutor:
	default:
		newExecutor = nil
	}
	// Update both old and new executor reference counts.
	oldExecutorRepr := "nil"
	oldExecutor := m.assignments[worker]
	if oldExecutor != nil {
		m.references[oldExecutor] -= 1
		oldExecutorRepr = fmt.Sprintf("%s (ref=%d)", oldExecutor, m.references[oldExecutor])
	}
	newExecutorRepr := "nil"
	if newExecutor != nil {
		m.assignments[worker] = newExecutor
		m.references[newExecutor] += 1
		newExecutorRepr = fmt.Sprintf("%s (ref=%d)", newExecutor, m.references[newExecutor])
	}
	actDesc.Log().Infof("assigned: worker=%s, newExec=%s, oldExec=%s",
		worker, oldExecutorRepr, newExecutorRepr)
}
