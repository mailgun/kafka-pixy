package mapper

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/holster/v4/clock"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
)

var testEpoch = clock.Date(2009, 02, 19, 13, 45, 0, 0, clock.UTC)

func Test(t *testing.T) {
	TestingT(t)
}

type MapperSuite struct {
	ns      *actor.Descriptor
	cfg     *config.Proxy
	workers []*mockWorker
	brokers []*sarama.Broker
}

var (
	_          = Suite(&MapperSuite{})
	lastExecId int32
)

func (s *MapperSuite) SetUpSuite(c *C) {
	testMode = true
	testhelpers.InitLogging()
}

func (s *MapperSuite) TearDownSuite(c *C) {
	testMode = false
}

func (s *MapperSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
	s.cfg = testhelpers.NewTestProxyCfg("mapper")
	s.workers = make([]*mockWorker, 5)
	s.brokers = make([]*sarama.Broker, 5)
	for i := 0; i < 5; i++ {
		s.workers[i] = &mockWorker{i, make(chan Executor, 10)}
		s.brokers[i] = sarama.NewBroker(fmt.Sprintf("b%d", i))
	}
}

func (s *MapperSuite) TestBasicAssignment(c *C) {
	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[1],
			s.workers[1]: s.brokers[0],
			s.workers[2]: s.brokers[0],
			s.workers[3]: s.brokers[1],
			s.workers[4]: s.brokers[0],
		},
	}
	m := Spawn(s.ns, s.cfg, r)

	// When
	for _, w := range s.workers {
		m.OnWorkerSpawned(w)
	}

	// Then
	assigned := make([]Executor, len(s.workers))
	for i, w := range s.workers {
		assigned[i] = <-w.assignmentsCh
	}

	c.Assert(assigned[0].BrokerConn(), Equals, s.brokers[1])
	c.Assert(assigned[1].BrokerConn(), Equals, s.brokers[0])
	c.Assert(assigned[3], Equals, assigned[0])
	c.Assert(assigned[2], Equals, assigned[1])
	c.Assert(assigned[4], Equals, assigned[1])

	for _, w := range s.workers {
		m.OnWorkerStopped(w)
	}
	m.Stop()
	for _, w := range s.workers {
		assertNoAssignments(c, w, 0)
	}
	assertExecutorStopped(c, assigned[0], 200*clock.Millisecond)
	assertExecutorStopped(c, assigned[1], 200*clock.Millisecond)
}

func (s *MapperSuite) TestTriggerReassignImmediate(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[0],
			s.workers[1]: s.brokers[1],
		},
	}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)
	assigned := make([]Executor, len(s.workers))
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[1])
	assigned[0] = <-s.workers[0].assignmentsCh
	assigned[1] = <-s.workers[1].assignmentsCh
	clock.Advance(500 * clock.Millisecond)

	// When
	log.Info("*** When")
	r.w2b = map[Worker]*sarama.Broker{
		s.workers[0]: s.brokers[1],
		s.workers[1]: s.brokers[1],
	}
	skipEvents(1)
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1]) // This update won't be skipped.

	// Then
	log.Info("*** Then")
	c.Assert((<-s.workers[0].assignmentsCh).BrokerConn(), Equals, s.brokers[1])
	c.Assert((<-s.workers[1].assignmentsCh).BrokerConn(), Equals, s.brokers[1])
	assertExecutorStopped(c, assigned[0], 200*clock.Millisecond)

	// Cleanup
	m.OnWorkerStopped(s.workers[0])
	m.OnWorkerStopped(s.workers[1])
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
	assertNoAssignments(c, s.workers[1], 0)
}

func (s *MapperSuite) TestTriggerReassignDeferred(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[1],
			s.workers[1]: s.brokers[0],
			s.workers[2]: s.brokers[0],
			s.workers[3]: s.brokers[1],
			s.workers[4]: s.brokers[0],
		},
	}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)
	assigned := make([]Executor, len(s.workers))
	for i, w := range s.workers {
		m.OnWorkerSpawned(w)
		assigned[i] = <-w.assignmentsCh
	}

	// When
	log.Info("*** When")
	r.w2b = map[Worker]*sarama.Broker{
		s.workers[0]: s.brokers[2],
		s.workers[1]: s.brokers[0],
		s.workers[2]: s.brokers[0],
		s.workers[3]: s.brokers[0],
		s.workers[4]: s.brokers[0],
	}
	for _, w := range s.workers {
		m.TriggerReassign(w)
	}
	// A couple of duplicate triggers.
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1])
	assertNoAssignments(c, s.workers[1], 100*clock.Millisecond)

	// Then
	log.Info("*** Then")
	// The reassign requests were received too soon after the workers were
	// assigned their executors on startup, so they are deferred.
	clock.Advance(499 * clock.Millisecond)
	for _, w := range s.workers {
		assertNoAssignments(c, w, 0)
	}
	clock.Advance(1 * clock.Millisecond)
	c.Assert((<-s.workers[0].assignmentsCh).BrokerConn(), Equals, s.brokers[2])
	c.Assert((<-s.workers[1].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[2].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[3].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[4].assignmentsCh).BrokerConn(), Equals, s.brokers[0])

	c.Assert(assigned[0].BrokerConn(), Equals, s.brokers[1])
	assertExecutorStopped(c, assigned[0], 200*clock.Millisecond)

	// Cleanup
	for _, w := range s.workers {
		m.OnWorkerStopped(w)
	}
	m.Stop()
	for _, w := range s.workers {
		assertNoAssignments(c, w, 0)
	}
}

// Reassigns for a worker is only performed ones per Consumer.RetryBackOff, and
// only once regardless of how many times reassign was triggered within a
// Consumer.RetryBackOff interval.
func (s *MapperSuite) TestOnlyOneReassign(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[2],
			s.workers[1]: s.brokers[2],
		},
	}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[1])
	c.Assert((<-s.workers[0].assignmentsCh).BrokerConn(), Equals, s.brokers[2])
	c.Assert((<-s.workers[1].assignmentsCh).BrokerConn(), Equals, s.brokers[2])

	// When
	log.Info("*** When")
	m.TriggerReassign(s.workers[0])
	c.Assert(clock.Wait4Scheduled(1, 3*clock.Second), Equals, true)

	clock.Advance(200 * clock.Millisecond)
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1])
	assertNoAssignments(c, s.workers[1], 100*clock.Millisecond)

	clock.Advance(100 * clock.Millisecond)
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1])

	clock.Advance(199 * clock.Millisecond)
	m.TriggerReassign(s.workers[1])

	c.Assert(clock.Wait4Scheduled(1, 0), Equals, true)
	c.Assert(clock.Wait4Scheduled(2, 100*clock.Millisecond), Equals, false)
	assertNoAssignments(c, s.workers[0], 0)
	assertNoAssignments(c, s.workers[1], 0)

	// Then
	log.Info("*** Then")
	clock.Advance(1 * clock.Millisecond)
	c.Assert((<-s.workers[0].assignmentsCh).BrokerConn(), Equals, s.brokers[2])
	assertNoAssignments(c, s.workers[1], 0)

	clock.Advance(199 * clock.Millisecond)
	assertNoAssignments(c, s.workers[1], 0)

	clock.Advance(1 * clock.Millisecond)
	c.Assert((<-s.workers[1].assignmentsCh).BrokerConn(), Equals, s.brokers[2])

	clock.Advance(1000 * clock.Millisecond)
	assertNoAssignments(c, s.workers[0], 0)
	assertNoAssignments(c, s.workers[1], 0)

	// Cleanup
	log.Info("*** Cleanup")
	m.OnWorkerStopped(s.workers[0])
	m.OnWorkerStopped(s.workers[1])
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
	assertNoAssignments(c, s.workers[1], 0)
}

// Duplicate spawn notifications for a worker are ignored.
func (s *MapperSuite) TestDuplicateSpawn(c *C) {
	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[0],
		},
	}
	m := Spawn(s.ns, s.cfg, r)

	m.OnWorkerSpawned(s.workers[0])
	assigned := <-s.workers[0].assignmentsCh
	c.Assert(assigned.BrokerConn(), Equals, s.brokers[0])

	// When
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[0])

	// Then
	assertNoAssignments(c, s.workers[0], 0)

	m.OnWorkerStopped(s.workers[0])
	m.Stop()
}

// If a worker never got assigned an executor before it terminated, that won't
// stop the mapper from terminating.
func (s *MapperSuite) TestNeverAssigned(c *C) {
	r := &mockResolver{
		errors: map[Worker]error{
			s.workers[0]: errors.New("Kaboom!"),
		},
	}
	s.cfg.Consumer.RetryBackoff = 50 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)

	m.OnWorkerSpawned(s.workers[0])
	assertNoAssignments(c, s.workers[0], 200*clock.Millisecond)

	// When
	m.OnWorkerStopped(s.workers[0])

	// Then
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
}

// If broker resolution return nil, it is treated as an error and a retry is
// scheduled.
func (s *MapperSuite) TestNilResolved(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: nil,
		},
	}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)

	m.OnWorkerSpawned(s.workers[0])
	c.Assert(clock.Wait4Scheduled(1, 3*clock.Second), Equals, true)
	clock.Advance(499 * clock.Millisecond)
	assertNoAssignments(c, s.workers[0], 0)

	// When
	r.w2b = map[Worker]*sarama.Broker{
		s.workers[0]: s.brokers[3],
	}
	clock.Advance(1 * clock.Millisecond)

	// Then
	assigned := <-s.workers[0].assignmentsCh
	c.Assert(assigned.BrokerConn(), Equals, s.brokers[3])

	// Cleanup
	m.OnWorkerStopped(s.workers[0])
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
}

// If writing to the workers assignments channel would block, then the
// resolution attempt is considered to fail, and retry is scheduled.
func (s *MapperSuite) TestChannelFull(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[4],
		},
	}
	s.workers[0].assignmentsCh = make(chan Executor, 1)
	s.workers[0].assignmentsCh <- &mockExecutor{id: 42}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)

	m.OnWorkerSpawned(s.workers[0])
	c.Assert(clock.Wait4Scheduled(1, 3*clock.Second), Equals, true)
	clock.Advance(1999 * clock.Millisecond)

	// When
	// Remove the dummy from the channel to make sending to it non-blocking.
	<-s.workers[0].assignmentsCh
	clock.Advance(1 * clock.Millisecond)

	// Then
	assigned := <-s.workers[0].assignmentsCh
	c.Assert(assigned.BrokerConn(), Equals, s.brokers[4])

	// Cleanup
	m.OnWorkerStopped(s.workers[0])
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
}

// If mapper cannot resolve an executor for a worker, then it sends nothing to
// its assignment channel but keeps retrying until it succeeds or the worker
// is stopped.
func (s *MapperSuite) TestNilNotPropagated(c *C) {
	clock.Freeze(testEpoch)

	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[2],
			s.workers[1]: s.brokers[2],
		},
	}
	s.cfg.Consumer.RetryBackoff = 500 * clock.Millisecond
	m := Spawn(s.ns, s.cfg, r)
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[1])
	// Initial assigment received.
	assigned0 := <-s.workers[0].assignmentsCh
	c.Assert(assigned0.BrokerConn(), Equals, s.brokers[2])
	assigned1 := <-s.workers[1].assignmentsCh
	c.Assert(assigned1.BrokerConn(), Equals, s.brokers[2])

	// Trigger reassignemnt making sure it fails.
	clock.Advance(500 * clock.Millisecond)
	r.errors = map[Worker]error{
		s.workers[0]: errors.New("Kaboom!"),
		s.workers[1]: errors.New("Kaboom!"),
	}
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1])
	assertExecutorStopped(c, assigned0, 3*clock.Second)
	assertNoAssignments(c, s.workers[0], 200*clock.Millisecond)
	assertNoAssignments(c, s.workers[1], 0)

	// When
	log.Info("*** When")
	r.errors = map[Worker]error{
		s.workers[1]: errors.New("Kaboom!"),
	}
	r.w2b = map[Worker]*sarama.Broker{
		s.workers[0]: s.brokers[1],
	}
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1])

	// Then
	log.Info("*** Then")
	clock.Advance(500 * clock.Millisecond)
	assigned0 = <-s.workers[0].assignmentsCh
	c.Assert(assigned0.BrokerConn(), Equals, s.brokers[1])

	// Cleanup
	m.OnWorkerStopped(s.workers[0])
	m.OnWorkerStopped(s.workers[1])
	m.Stop()
	assertNoAssignments(c, s.workers[0], 0)
	assertNoAssignments(c, s.workers[1], 0)
}

func skipEvents(n int) {
	atomic.StoreInt32(&testSkipEvents, int32(n))
}

type mockResolver struct {
	w2b    map[Worker]*sarama.Broker
	errors map[Worker]error
}

func (r *mockResolver) ResolveBroker(w Worker) (*sarama.Broker, error) {
	err := r.errors[w]
	if err != nil {
		return nil, err
	}
	return r.w2b[w], nil
}

func (r *mockResolver) SpawnExecutor(b *sarama.Broker) Executor {
	return &mockExecutor{id: atomic.AddInt32(&lastExecId, 1), b: b, stopCh: make(chan none.T)}
}

type mockWorker struct {
	id            int
	assignmentsCh chan Executor
}

func (w *mockWorker) Assignment() chan<- Executor {
	return w.assignmentsCh
}

func (w *mockWorker) String() string {
	return fmt.Sprintf("w%d", w.id)
}

type mockExecutor struct {
	id     int32
	b      *sarama.Broker
	stopCh chan none.T
}

func (e *mockExecutor) BrokerConn() *sarama.Broker {
	return e.b
}

func (e *mockExecutor) Stop() {
	close(e.stopCh)
}

func (e *mockExecutor) String() string {
	return fmt.Sprintf("e%d(%s)", e.id, e.b.Addr())
}

func assertNoAssignments(c *C, w *mockWorker, timeout clock.Duration) {
	select {
	case e := <-w.assignmentsCh:
		c.Errorf("Unexpected assignment: %v", e)
	case <-time.After(timeout):
	}
}

func assertExecutorStopped(c *C, e Executor, timeout clock.Duration) {
	mockExec := e.(*mockExecutor)
	select {
	case <-mockExec.stopCh:
	case <-time.After(timeout):
		c.Error("Executor did not stop in time")
	}
}
