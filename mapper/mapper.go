package mapper

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/context"
	"github.com/mailgun/log"
)

// T maintains mapping of partition workers that generate requests to broker
// executors that process them. It uses an external resolver to determine a
// particular broker executor instance to assign to a partition worker.
//
// Mapper triggers reassignment whenever one of the following events happen:
//   * it is signaled that a new worker has been spawned via `WorkerSpawned()`;
//   * it is signaled that an existing worker has stopped via `WorkerStopped()`;
//   * a worker explicitly requested reassignment via `WorkerReassign()`
//   * an executor reported connection error via `BrokerFailed()`.
//
// Broker executors are spawned on demand when a broker connection is mapped to
// a partition worker for the first time. It is guaranteed that a broker
// executor is stopped only after all partition workers that used to be assigned
// to it have either been stopped or assigned another broker executor.
type T struct {
	baseCID          *context.ID
	resolver         Resolver
	workerSpawnedCh  chan Worker
	workerStoppedCh  chan Worker
	workerReassignCh chan Worker
	assignments      map[Worker]Executor
	references       map[Executor]int
	connections      map[*sarama.Broker]Executor
	stopCh           chan none
	wg               sync.WaitGroup
}

// Resolver defines an interface to resolve a broker connection that should
// serve requests of a particular partition worker and create a broker executor
// from a broker connection.
type Resolver interface {
	// ResolveBroker returns a broker connection that should be used to
	// determine a broker executor assigned to the specified partition worker.
	ResolveBroker(pw Worker) (*sarama.Broker, error)
	// SpawnExecutor spawns a broker executor for the specified connection.
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

// Executor represents an entity that executes requests of partition workers
// via a particular broker connection.
type Executor interface {
	// BrokerConn returns a broker connection used by the executor.
	BrokerConn() *sarama.Broker
	// Stop synchronously stops the executor.
	Stop()
}

// Spawn creates a mapper instance and starts its internal goroutines.
func Spawn(cid *context.ID, resolver Resolver) *T {
	m := &T{
		baseCID:          cid,
		resolver:         resolver,
		workerSpawnedCh:  make(chan Worker),
		workerStoppedCh:  make(chan Worker),
		workerReassignCh: make(chan Worker),
		assignments:      make(map[Worker]Executor),
		references:       make(map[Executor]int),
		connections:      make(map[*sarama.Broker]Executor),
		stopCh:           make(chan none),
	}
	spawn(&m.wg, m.watch4Changes)
	return m
}

func (m *T) WorkerSpawned() chan<- Worker {
	return m.workerSpawnedCh
}

func (m *T) WorkerStopped() chan<- Worker {
	return m.workerStoppedCh
}

func (m *T) WorkerReassign() chan<- Worker {
	return m.workerReassignCh
}

func (m *T) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

type mappingChange struct {
	spawned  map[Worker]none
	outdated map[Worker]none
	stopped  map[Worker]none
}

func (m *T) newMappingChange() *mappingChange {
	return &mappingChange{
		spawned:  make(map[Worker]none),
		outdated: make(map[Worker]none),
		stopped:  make(map[Worker]none),
	}
}

func (mc *mappingChange) isEmtpy() bool {
	return len(mc.spawned) == 0 && len(mc.outdated) == 0 && len(mc.stopped) == 0
}

func (mc *mappingChange) String() string {
	return fmt.Sprintf("{created=%d, outdated=%d, closed=%d}",
		len(mc.spawned), len(mc.outdated), len(mc.stopped))
}

// watch4Changes listens for mapping affecting signals, batches them into
// a mappingChange object and triggers reassignments, making sure to run only
// one at a time. When signaled to stop it only quits when all children have
// closed.
func (m *T) watch4Changes() {
	cid := m.baseCID.NewChild("watch4Changes")
	defer cid.LogScope()()

	change := m.newMappingChange()
	redispatchDoneCh := make(chan none, 1)
	var nilOrRedispatchDoneCh <-chan none
	stop := false
	for {
		select {
		case pw := <-m.workerSpawnedCh:
			change.spawned[pw] = nothing

		case pw := <-m.workerStoppedCh:
			change.stopped[pw] = nothing

		case pw := <-m.workerReassignCh:
			change.outdated[pw] = nothing

		case <-nilOrRedispatchDoneCh:
			nilOrRedispatchDoneCh = nil

		case <-m.stopCh:
			stop = true
		}
		// If redispatch is required and there is none running at the moment
		// then spawn a redispatch goroutine.
		if !change.isEmtpy() && nilOrRedispatchDoneCh == nil {
			log.Infof("<%s> reassign: change=%s", cid, change)
			go m.reassign(cid, change, redispatchDoneCh)
			change = m.newMappingChange()
			nilOrRedispatchDoneCh = redispatchDoneCh
		}
		// Do not leave this loop until all workers are closed.
		if stop && nilOrRedispatchDoneCh == nil && len(m.assignments) == 0 {
			return
		}
	}
}

// reassign updates partition-to-broker assignments using the external resolver.
func (m *T) reassign(parentGid *context.ID, change *mappingChange, doneCh chan none) {
	cid := parentGid.NewChild("reassign")
	defer cid.LogScope(change)()
	defer func() { doneCh <- nothing }()

	// Travers through stopped workers and dereference brokers assigned to them.
	for pw := range change.stopped {
		be := m.assignments[pw]
		delete(m.assignments, pw)
		delete(change.spawned, pw)
		if be != nil {
			m.references[be] = m.references[be] - 1
			log.Infof("<%s> unassign %s -> %s (ref=%d)", cid, pw, be, m.references[be])
		}
	}
	// Weed out partition workers that have already been closed.
	for pw := range change.outdated {
		if _, ok := m.assignments[pw]; !ok {
			delete(change.outdated, pw)
		}
	}
	// Run resolution for the created and outdated partition workers.
	for pw := range change.spawned {
		m.resolveBroker(cid, pw)
	}
	for pw := range change.outdated {
		m.resolveBroker(cid, pw)
	}
	// All broker assignments have been propagated to partition workers, so
	// it is safe to close broker executors that are not used anymore.
	for be, referenceCount := range m.references {
		if referenceCount != 0 {
			continue
		}
		log.Infof("<%s> decomission %s", cid, be)
		be.Stop()
		delete(m.references, be)
		if m.connections[be.BrokerConn()] == be {
			delete(m.connections, be.BrokerConn())
		}
	}
}

// resolveBroker queries the Kafka cluster for a new partition leader and
// assigns it to the specified partition consumer.
func (m *T) resolveBroker(cid *context.ID, pw Worker) {
	var newBrokerExecutor Executor
	brokerConn, err := m.resolver.ResolveBroker(pw)
	if err != nil {
		log.Infof("<%s> failed to resolve broker: pw=%s, err=(%s)", cid, pw, err)
	} else {
		if brokerConn != nil {
			newBrokerExecutor = m.connections[brokerConn]
			if newBrokerExecutor == nil && brokerConn != nil {
				newBrokerExecutor = m.resolver.SpawnExecutor(brokerConn)
				log.Infof("<%s> spawned %s", cid, newBrokerExecutor)
				m.connections[brokerConn] = newBrokerExecutor
			}
		}
	}
	// Assign the new broker executor, but only if it does not block.
	select {
	case pw.Assignment() <- newBrokerExecutor:
	default:
		return
	}
	oldBrokerExecutor := m.assignments[pw]
	m.assignments[pw] = newBrokerExecutor
	// Update both old and new broker executor reference counts.
	if oldBrokerExecutor != nil {
		m.references[oldBrokerExecutor] = m.references[oldBrokerExecutor] - 1
		log.Infof("<%s> unassign %s -> %s (ref=%d)",
			cid, pw, oldBrokerExecutor, m.references[oldBrokerExecutor])
	}
	if newBrokerExecutor == nil {
		log.Infof("<%s> assign %s -> <nil>", cid, pw)
		return
	}
	m.references[newBrokerExecutor] = m.references[newBrokerExecutor] + 1
	log.Infof("<%s> assign %s -> %s (ref=%d)",
		cid, pw, newBrokerExecutor, m.references[newBrokerExecutor])
}
