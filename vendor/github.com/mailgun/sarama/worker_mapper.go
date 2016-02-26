package sarama

import (
	"fmt"
	"sync"
)

// partition2BrokerMapper manages mapping partition workers to brokers
// responsible for executing requests coming from the partition worker. To
// determine what broker should processes requests from a particular partition
// worker an external resolver is used.
//
// Mapper triggers reassignment whenever one of the following events happen:
//   * it is signaled that a new partition worker has been spawned via `workerCreated()`;
//   * it is signaled to close a partition consumer via `workerClosing()`.
//     The mapper synchronously terminates the worker calling its `close()`;
//   * a partition worker explicitly requested reassignment via `workerReassign()`
//   * a broker executor reported connection error via `brokerFailed()`.
//
// Broker executors are spawned on demand when a broker connection is mapped to
// a partition worker for the first time. It is guaranteed that a broker
// executor is closed only after all partition consumers that used to be assigned
// to it have either been closed or assigned another broker executor.
type partition2BrokerMapper struct {
	baseCID          *ContextID
	resolver         brokerResolver
	workerCreatedCh  chan partitionWorker
	workerClosedCh   chan partitionWorker
	workerReassignCh chan partitionWorker
	assignments      map[partitionWorker]brokerExecutor
	references       map[brokerExecutor]int
	connections      map[*Broker]brokerExecutor
	closingCh        chan none
	wg               sync.WaitGroup
}

// brokerResolver defines an interface to resolve a broker connection that
// should server requests of a particular partition worker and a way to create
// a broker executor from a broker connection.
type brokerResolver interface {
	resolveBroker(pw partitionWorker) (*Broker, error)
	spawnBrokerExecutor(brokerConn *Broker) brokerExecutor
}

// partitionWorker represents an entity that makes requests via an assigned
// broker executor.
type partitionWorker interface {
	// assignment returns a channel that the worker expects broker assignments
	// at. Implementations have to ensure that the channel has a non zero buffer
	// and that they read from this channel as soon as the value becomes
	// available, for mapper will drop assignments in case the write to the
	// channel may block.
	assignment() chan<- brokerExecutor
}

// brokerExecutor represents an entity that executes requests of partition
// workers via a particular broker connection.
type brokerExecutor interface {
	brokerConn() *Broker
	close()
}

// spawnPartition2BrokerMapper spawns an instance of Partition2BrokerMapper
// that uses the specified resolver to make partition-to-broker mapping decisions.
func spawnPartition2BrokerMapper(cid *ContextID, resolver brokerResolver) *partition2BrokerMapper {
	m := &partition2BrokerMapper{
		baseCID:          cid,
		resolver:         resolver,
		workerCreatedCh:  make(chan partitionWorker),
		workerClosedCh:   make(chan partitionWorker),
		workerReassignCh: make(chan partitionWorker),
		assignments:      make(map[partitionWorker]brokerExecutor),
		references:       make(map[brokerExecutor]int),
		connections:      make(map[*Broker]brokerExecutor),
		closingCh:        make(chan none),
	}
	spawn(&m.wg, m.watch4Changes)
	return m
}

func (m *partition2BrokerMapper) workerCreated() chan<- partitionWorker {
	return m.workerCreatedCh
}

func (m *partition2BrokerMapper) workerClosed() chan<- partitionWorker {
	return m.workerClosedCh
}

func (m *partition2BrokerMapper) workerReassign() chan<- partitionWorker {
	return m.workerReassignCh
}

func (m *partition2BrokerMapper) close() {
	close(m.closingCh)
	m.wg.Wait()
}

type mappingChange struct {
	created  map[partitionWorker]none
	outdated map[partitionWorker]none
	closed   map[partitionWorker]none
}

func (m *partition2BrokerMapper) newMappingChange() *mappingChange {
	return &mappingChange{
		created:  make(map[partitionWorker]none),
		outdated: make(map[partitionWorker]none),
		closed:   make(map[partitionWorker]none),
	}
}

func (mc *mappingChange) isEmtpy() bool {
	return len(mc.created) == 0 && len(mc.outdated) == 0 && len(mc.closed) == 0
}

func (mc *mappingChange) String() string {
	return fmt.Sprintf("{created=%d, outdated=%d, closed=%d}",
		len(mc.created), len(mc.outdated), len(mc.closed))
}

// watch4Changes listens for mapping affecting signals, batches them into
// a mappingChange object and triggers reassignments, making sure to run only
// one at a time. When signaled to stop it only quits when all children have
// closed.
func (m *partition2BrokerMapper) watch4Changes() {
	cid := m.baseCID.NewChild("watch4Changes")
	defer cid.LogScope()()

	change := m.newMappingChange()
	redispatchDoneCh := make(chan none, 1)
	var nilOrRedispatchDoneCh <-chan none
	closing := false
	for {
		select {
		case pw := <-m.workerCreatedCh:
			change.created[pw] = nothing

		case pw := <-m.workerClosedCh:
			change.closed[pw] = nothing

		case pw := <-m.workerReassignCh:
			change.outdated[pw] = nothing

		case <-nilOrRedispatchDoneCh:
			nilOrRedispatchDoneCh = nil

		case <-m.closingCh:
			closing = true
		}
		// If redispatch is required and there is none running at the moment
		// then spawn a redispatch goroutine.
		if !change.isEmtpy() && nilOrRedispatchDoneCh == nil {
			Logger.Printf("<%s> reassign: change=%s", cid, change)
			go m.reassign(cid, change, redispatchDoneCh)
			change = m.newMappingChange()
			nilOrRedispatchDoneCh = redispatchDoneCh
		}
		// Do not leave this loop until all workers are closed.
		if closing && nilOrRedispatchDoneCh == nil && len(m.assignments) == 0 {
			return
		}
	}
}

// reassign updates partition-to-broker assignments using the external resolver.
func (m *partition2BrokerMapper) reassign(parentGid *ContextID, change *mappingChange, doneCh chan none) {
	cid := parentGid.NewChild("reassign")
	defer cid.LogScope(change)()
	defer func() { doneCh <- nothing }()

	// Travers through closed workers and dereference brokers assigned to them.
	for pw := range change.closed {
		be := m.assignments[pw]
		delete(m.assignments, pw)
		delete(change.created, pw)
		if be != nil {
			m.references[be] = m.references[be] - 1
			Logger.Printf("<%s> unassign %s -> %s (ref=%d)", cid, pw, be, m.references[be])
		}
	}
	// Weed out partition workers that have already been closed.
	for pw := range change.outdated {
		if _, ok := m.assignments[pw]; !ok {
			delete(change.outdated, pw)
		}
	}
	// Run resolution for the created and outdated partition workers.
	for pw := range change.created {
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
		Logger.Printf("<%s> decomission %s", cid, be)
		be.close()
		delete(m.references, be)
		if m.connections[be.brokerConn()] == be {
			delete(m.connections, be.brokerConn())
		}
	}
}

// resolveBroker queries the Kafka cluster for a new partition leader and
// assigns it to the specified partition consumer.
func (m *partition2BrokerMapper) resolveBroker(cid *ContextID, pw partitionWorker) {
	var newBrokerExecutor brokerExecutor
	brokerConn, err := m.resolver.resolveBroker(pw)
	if err != nil {
		Logger.Printf("<%s> failed to resolve broker: pw=%s, err=(%s)", cid, pw, err)
	} else {
		if brokerConn != nil {
			newBrokerExecutor = m.connections[brokerConn]
			if newBrokerExecutor == nil && brokerConn != nil {
				newBrokerExecutor = m.resolver.spawnBrokerExecutor(brokerConn)
				Logger.Printf("<%s> spawned %s", cid, newBrokerExecutor)
				m.connections[brokerConn] = newBrokerExecutor
			}
		}
	}
	// Assign the new broker executor, but only if it does not block.
	select {
	case pw.assignment() <- newBrokerExecutor:
	default:
		return
	}
	oldBrokerExecutor := m.assignments[pw]
	m.assignments[pw] = newBrokerExecutor
	// Update both old and new broker executor reference counts.
	if oldBrokerExecutor != nil {
		m.references[oldBrokerExecutor] = m.references[oldBrokerExecutor] - 1
		Logger.Printf("<%s> unassign %s -> %s (ref=%d)",
			cid, pw, oldBrokerExecutor, m.references[oldBrokerExecutor])
	}
	if newBrokerExecutor == nil {
		Logger.Printf("<%s> assign %s -> <nil>", cid, pw)
		return
	}
	m.references[newBrokerExecutor] = m.references[newBrokerExecutor] + 1
	Logger.Printf("<%s> assign %s -> %s (ref=%d)",
		cid, pw, newBrokerExecutor, m.references[newBrokerExecutor])
}
