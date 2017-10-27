package mapper

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type MapperSuite struct {
	ns      *actor.Descriptor
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
	m := Spawn(s.ns, r)

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
		assertNoAssignments(c, w)
	}
	assertExecutorStopped(c, assigned[0], 200*time.Millisecond)
	assertExecutorStopped(c, assigned[1], 200*time.Millisecond)
}

func (s *MapperSuite) TestTriggerReassign(c *C) {
	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[1],
			s.workers[1]: s.brokers[0],
			s.workers[2]: s.brokers[0],
			s.workers[3]: s.brokers[1],
			s.workers[4]: s.brokers[0],
		},
	}
	m := Spawn(s.ns, r)
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
	skipEvents(len(s.workers) + 1)
	for _, w := range s.workers {
		m.TriggerReassign(w)
	}
	// A couple of duplicate triggers.
	m.TriggerReassign(s.workers[0])
	m.TriggerReassign(s.workers[1]) // This update won't be skipped.

	// Then
	c.Assert((<-s.workers[0].assignmentsCh).BrokerConn(), Equals, s.brokers[2])
	c.Assert((<-s.workers[1].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[2].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[3].assignmentsCh).BrokerConn(), Equals, s.brokers[0])
	c.Assert((<-s.workers[4].assignmentsCh).BrokerConn(), Equals, s.brokers[0])

	c.Assert(assigned[0].BrokerConn(), Equals, s.brokers[1])
	assertExecutorStopped(c, assigned[0], 200*time.Millisecond)

	// Cleanup
	for _, w := range s.workers {
		m.OnWorkerStopped(w)
	}
	m.Stop()
	for _, w := range s.workers {
		assertNoAssignments(c, w)
	}
}

// If a worker never got assigned an executor before it terminated, that won't
// stop the mapper from stopping afterwards.
func (s *MapperSuite) TestNeverSucceeded(c *C) {
	r := &mockResolver{
		w2b: map[Worker]*sarama.Broker{
			s.workers[0]: s.brokers[0],
		},
	}
	m := Spawn(s.ns, r)

	m.OnWorkerSpawned(s.workers[0])
	assigned := <-s.workers[0].assignmentsCh
	c.Assert(assigned.BrokerConn(), Equals, s.brokers[0])

	// When
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[0])
	m.OnWorkerSpawned(s.workers[0])

	// Then
	assertNoAssignments(c, s.workers[0])

	m.OnWorkerStopped(s.workers[0])
	m.Stop()
}

func (s *MapperSuite) TestDuplicateSpawn(c *C) {
	r := &mockResolver{
		errors: map[Worker]error{
			s.workers[0]: errors.New("Kaboom!"),
		},
	}
	m := Spawn(s.ns, r)

	m.OnWorkerSpawned(s.workers[0])
	assigned := <-s.workers[0].assignmentsCh
	c.Assert(assigned, Equals, nil)

	// When
	m.OnWorkerStopped(s.workers[0])

	// Then
	m.Stop()
	assertNoAssignments(c, s.workers[0])
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

func assertNoAssignments(c *C, w *mockWorker) {
	select {
	case e := <-w.assignmentsCh:
		c.Errorf("Unexpected assignment: %v", e)
	default:
	}
}

func assertExecutorStopped(c *C, e Executor, timeout time.Duration) {
	mockExec := e.(*mockExecutor)
	select {
	case <-mockExec.stopCh:
	case <-time.After(timeout):
		c.Error("Executor did not stop in time")
	}
}
