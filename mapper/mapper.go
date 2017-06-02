package mapper

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
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
	actorID     *actor.ID
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
	eventsChBufSize = 32

	evWorkerSpawned eventType = iota
	evWorkerStopped
	evReassignNeeded
)

// Spawn creates a mapper instance and starts its internal goroutines.
func Spawn(namespace *actor.ID, resolver Resolver) *T {
	m := &T{
		actorID:     namespace.NewChild("mapper"),
		resolver:    resolver,
		eventsCh:    make(chan event, eventsChBufSize),
		assignments: make(map[Worker]Executor),
		references:  make(map[Executor]int),
		connections: make(map[*sarama.Broker]Executor),
		stopCh:      make(chan none.T),
	}
	actor.Spawn(m.actorID, &m.wg, m.run)
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

type mappingChanges struct {
	spawned  map[Worker]none.T
	outdated map[Worker]none.T
	stopped  map[Worker]none.T
}

func (m *T) newMappingChanges() *mappingChanges {
	return &mappingChanges{
		spawned:  make(map[Worker]none.T),
		outdated: make(map[Worker]none.T),
		stopped:  make(map[Worker]none.T),
	}
}

func (mc *mappingChanges) isEmtpy() bool {
	return len(mc.spawned) == 0 && len(mc.outdated) == 0 && len(mc.stopped) == 0
}

func (mc *mappingChanges) String() string {
	return fmt.Sprintf("{created=%d, outdated=%d, closed=%d}",
		len(mc.spawned), len(mc.outdated), len(mc.stopped))
}

// run listens for mapping events, batches them into a mappingChange object and
// triggers reassignments, making sure to run only one at a time. When signaled
// to stop it only quits when all children have closed.
func (m *T) run() {
	changes := m.newMappingChanges()
	redispatchDoneCh := make(chan none.T, 1)
	var nilOrRedispatchDoneCh <-chan none.T
	stop := false
	for {
		select {
		case ev := <-m.eventsCh:
			switch ev.t {
			case evWorkerSpawned:
				changes.spawned[ev.w] = none.V
			case evWorkerStopped:
				changes.stopped[ev.w] = none.V
			case evReassignNeeded:
				changes.outdated[ev.w] = none.V
			}
		case <-nilOrRedispatchDoneCh:
			nilOrRedispatchDoneCh = nil

		case <-m.stopCh:
			stop = true
		}
		// If redispatch is required and there is none running at the moment
		// then spawn a redispatch goroutine.
		if !changes.isEmtpy() && nilOrRedispatchDoneCh == nil {
			log.Infof("<%s> reassign: change=%s", m.actorID, changes)
			reassignActorID := m.actorID.NewChild("reassign")
			changesForReassign := changes
			actor.Spawn(reassignActorID, nil, func() {
				m.reassign(reassignActorID, changesForReassign, redispatchDoneCh)
			})
			changes = m.newMappingChanges()
			nilOrRedispatchDoneCh = redispatchDoneCh
		}
		// Do not leave this loop until all workers are closed.
		if stop && nilOrRedispatchDoneCh == nil && len(m.assignments) == 0 {
			return
		}
	}
}

// reassign updates partition-to-broker assignments using the external resolver.
func (m *T) reassign(actorID *actor.ID, change *mappingChanges, doneCh chan none.T) {
	defer func() { doneCh <- none.V }()

	// Travers through stopped workers and dereference brokers assigned to them.
	for worker := range change.stopped {
		executor := m.assignments[worker]
		delete(m.assignments, worker)
		delete(change.spawned, worker)
		if executor != nil {
			m.references[executor] = m.references[executor] - 1
			log.Infof("<%s> unassign %s -> %s (ref=%d)", actorID, worker, executor, m.references[executor])
		}
	}
	// Weed out workers that have already been closed.
	for worker := range change.outdated {
		if _, ok := m.assignments[worker]; !ok {
			delete(change.outdated, worker)
		}
	}
	// Run resolution for the created and outdated workers.
	for worker := range change.spawned {
		m.resolveBroker(actorID, worker)
	}
	for worker := range change.outdated {
		m.resolveBroker(actorID, worker)
	}
	// All broker assignments have been propagated to workers, so it is safe to
	// close executors that are not used anymore.
	for executor, referenceCount := range m.references {
		if referenceCount != 0 {
			continue
		}
		log.Infof("<%s> decomission %s", actorID, executor)
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
func (m *T) resolveBroker(actorID *actor.ID, worker Worker) {
	var newExecutor Executor
	brokerConn, err := m.resolver.ResolveBroker(worker)
	if err != nil {
		log.Infof("<%s> failed to resolve broker: worker=%s, err=(%s)", actorID, worker, err)
	} else {
		if brokerConn != nil {
			newExecutor = m.connections[brokerConn]
			if newExecutor == nil && brokerConn != nil {
				newExecutor = m.resolver.SpawnExecutor(brokerConn)
				log.Infof("<%s> spawned %s", actorID, newExecutor)
				m.connections[brokerConn] = newExecutor
			}
		}
	}
	// Assign the new executor, but only if it does not block.
	select {
	case worker.Assignment() <- newExecutor:
	default:
		return
	}
	oldExecutor := m.assignments[worker]
	m.assignments[worker] = newExecutor
	// Update both old and new executor reference counts.
	if oldExecutor != nil {
		m.references[oldExecutor] = m.references[oldExecutor] - 1
		log.Infof("<%s> unassign %s -> %s (ref=%d)",
			actorID, worker, oldExecutor, m.references[oldExecutor])
	}
	if newExecutor == nil {
		log.Infof("<%s> assign %s -> <nil>", actorID, worker)
		return
	}
	m.references[newExecutor] = m.references[newExecutor] + 1
	log.Infof("<%s> assign %s -> %s (ref=%d)",
		actorID, worker, newExecutor, m.references[newExecutor])
}
