package topiccsm

import (
	"sync"
	"testing"
	"time"

	"github.com/mailgun/holster/clock"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

const (
	group = "g"
)

func Test(t *testing.T) {
	TestingT(t)
}

type TopicCsmSuite struct {
	epoch      time.Time
	cfg        *config.Proxy
	ns         *actor.Descriptor
	requestsCh chan consumer.Request
	childSpec  dispatcher.ChildSpec
	lifespanCh chan *T

	mu         sync.Mutex
	safe2Stop  bool
	checkCount int
}

var _ = Suite(&TopicCsmSuite{})

func (s *TopicCsmSuite) SetUpSuite(c *C) {
	var err error
	s.epoch, err = time.Parse(time.RFC3339, "2009-02-19T00:00:00Z")
	c.Assert(err, IsNil)
	testhelpers.InitLogging()
}

func (s *TopicCsmSuite) SetUpTest(c *C) {
	clock.Freeze(s.epoch)
	s.cfg = testhelpers.NewTestProxyCfg("topiccsm")
	s.ns = actor.Root().NewChild("T")
	s.requestsCh = make(chan consumer.Request, 100)
	s.childSpec = dispatcher.NewChildSpec4Test(s.requestsCh)
	s.lifespanCh = make(chan *T, 2)

	s.safe2Stop = true
	s.checkCount = 0
}

func (s *TopicCsmSuite) TearDownTest(c *C) {
	clock.Unfreeze()
}

func (s *TopicCsmSuite) isSafe2Stop() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checkCount++
	return s.safe2Stop
}

func (s *TopicCsmSuite) setSafe2Stop(safe2Stop bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.safe2Stop = safe2Stop
}

// Requests are processed in first come first served fashion. When a message is
// is send in response to a requests it is also reported as Offered downstream.
func (s *TopicCsmSuite) TestRequestResponse(c *C) {
	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)
	defer func() {
		close(s.requestsCh) // Signal to stop.
		<-s.lifespanCh      // Wait for it to do so.
	}()

	requests := make([]consumer.Request, 10)
	messages := make([]consumer.Message, 10)
	eventsChs := make([]chan consumer.Event, 10)
	for i := 0; i < 10; i++ {
		requests[i] = newRequest()
		messages[i], eventsChs[i] = newMessage(i)
	}

	// When
	for _, rq := range requests {
		s.requestsCh <- rq
	}
	for _, msg := range messages {
		tc.Messages() <- msg
	}

	// Then
	for i := 0; i < 10; i++ {
		c.Assert(<-requests[i].ResponseCh, DeepEquals,
			consumer.Response{Msg: messages[i]})
		c.Assert(<-eventsChs[i], DeepEquals,
			consumer.Event{consumer.EvOffered, messages[i].Offset})
	}
}

// If request has been waiting for a message longer than
// Consumer.LongPollingTimeout then it is rejected.
func (s *TopicCsmSuite) TestLongPollingExpires(c *C) {
	s.cfg.Consumer.LongPollingTimeout = 300

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)
	defer func() {
		close(s.requestsCh) // Signal to stop.
		<-s.lifespanCh      // Wait for it to do so.
	}()

	rq1 := newRequest() // expires at 300
	c.Assert(clock.Advance(1), Equals, time.Duration(1))
	rq2 := newRequest() // expires at 301
	c.Assert(clock.Advance(1), Equals, time.Duration(2))
	rq3 := newRequest() // expires at 302

	msg1, _ := newMessage(42)
	msg2, _ := newMessage(43)

	s.requestsCh <- rq1
	s.requestsCh <- rq2
	s.requestsCh <- rq3
	c.Assert(clock.Advance(297), Equals, time.Duration(299))
	tc.Messages() <- msg1
	assertResponse(c, rq1, consumer.Response{Msg: msg1}, time.Second)

	// When: The rq2 expires, but rq3 still has 1 ns to last.
	c.Assert(clock.Advance(2), Equals, time.Duration(301))

	// Then
	assertResponse(c, rq2, requestTimeoutRs, time.Second)
	tc.Messages() <- msg2
	assertResponse(c, rq3, consumer.Response{Msg: msg2}, time.Second)
}

// Stale requests are rejected immediately.
func (s *TopicCsmSuite) TestStaleRequest(c *C) {
	s.cfg.Consumer.LongPollingTimeout = 300

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)
	defer func() {
		close(s.requestsCh) // Signal to stop.
		<-s.lifespanCh      // Wait for it to do so.
	}()

	rq := newRequest() // expires at 300
	s.requestsCh <- rq
	c.Assert(clock.Advance(300), Equals, time.Duration(300))

	// When: an already expired request is received.

	// Then: it is rejected on the spot.
	assertResponse(c, rq, requestTimeoutRs, time.Second)
}

// If there has been no requests for Consumer.SubscriptionTimeout and it is
// safe to stop, then the topic consumer terminates.
func (s *TopicCsmSuite) TestSubscriptionExpires(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 300

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)

	c.Assert(clock.Advance(499), Equals, time.Duration(499))
	s.requestsCh <- newRequest()
	msg, _ := newMessage(42)
	tc.Messages() <- msg

	// When
	c.Assert(clock.Advance(499), Equals, time.Duration(998))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)
	c.Assert(clock.Advance(1), Equals, time.Duration(999))

	// Then
	assertStopped(c, s.lifespanCh, time.Second)
}

// If there has been no requests for SubscriptionTimeout, but it is not safe to
// stop then the topic consumer waits until AckTimeout expires as well before
// terminating.
func (s *TopicCsmSuite) TestAckTimeoutExpires(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 700
	s.setSafe2Stop(false)
	safe2StopPollingInterval = 5

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)

	// When/Then
	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	c.Assert(clock.Advance(199), Equals, time.Duration(699))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	// The time in test is deterministic but the topic consumer goroutine is
	// still reacts to deterministic events in realtime that is why we have to
	// advance at least safe2StopPollingInterval, unlike in real life where
	// after 1 nanosecond the topic consumer would have started termination.
	c.Assert(clock.Advance(5), Equals, time.Duration(704))
	assertStopped(c, s.lifespanCh, time.Second)
}

// If a request arrives while waiting for a AckTimeout the expire timeout is
// reset and normal request processing is resumed.
func (s *TopicCsmSuite) TestAckTimeoutRequest(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 700
	s.setSafe2Stop(false)
	safe2StopPollingInterval = 5

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)

	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)
	c.Assert(clock.Advance(199), Equals, time.Duration(699))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	// When: request arrives
	s.requestsCh <- newRequest()
	msg, _ := newMessage(42)
	tc.Messages() <- msg

	// Then
	c.Assert(clock.Advance(500), Equals, time.Duration(1199))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)
	c.Assert(clock.Advance(199), Equals, time.Duration(1398))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	c.Assert(clock.Advance(100), Equals, time.Duration(1498))
	assertStopped(c, s.lifespanCh, time.Second)
}

// If the requests channel is closed signaling to stop while waiting for stop
// to be safe, then the topic consumer cease waiting and terminates immediately.
func (s *TopicCsmSuite) TestAckTimeoutStop(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 700
	s.setSafe2Stop(false)
	safe2StopPollingInterval = 5

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)

	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)
	c.Assert(clock.Advance(100), Equals, time.Duration(600))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	// When
	close(s.requestsCh)

	// Then
	assertStopped(c, s.lifespanCh, time.Second)
}

// If an expired topic consumer polls that it is safe to stop now, then it does
// that immediately before AckTimeout expires.
func (s *TopicCsmSuite) TestAckTimeoutSafe(c *C) {
	s.cfg.Consumer.SubscriptionTimeout = 500
	s.cfg.Consumer.AckTimeout = 700
	s.setSafe2Stop(false)
	safe2StopPollingInterval = 5

	tc := Spawn(s.ns, group, s.childSpec, s.cfg, s.lifespanCh, s.isSafe2Stop)
	c.Assert(<-s.lifespanCh, Equals, tc)

	c.Assert(clock.Advance(500), Equals, time.Duration(500))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)
	c.Assert(clock.Advance(100), Equals, time.Duration(600))
	assertRunning(c, s.lifespanCh, 50*time.Millisecond)

	// When
	s.setSafe2Stop(true)
	// We have to advance by safe2StopPollingInterval to make the consumer get
	// to the point where it polls isSafe2Stop.
	c.Assert(clock.Advance(5), Equals, time.Duration(605))

	// Then
	assertStopped(c, s.lifespanCh, time.Second)
}

func newRequest() consumer.Request {
	return consumer.Request{
		Timestamp:  clock.Now().UTC(),
		ResponseCh: make(chan consumer.Response, 1),
	}
}

func newMessage(idx int) (consumer.Message, chan consumer.Event) {
	eventsCh := make(chan consumer.Event, 1)
	var msg consumer.Message
	msg.Value = []byte{byte(idx)}
	msg.Offset = int64(1000 + idx)
	msg.EventsCh = eventsCh
	return msg, eventsCh
}

func assertResponse(c *C, rq consumer.Request, rs consumer.Response, timeout time.Duration) {
	select {
	case gotRs := <-rq.ResponseCh:
		c.Assert(gotRs, DeepEquals, rs)
	case <-time.After(timeout):
		c.Errorf("Response wait timeout, want=%v", rs)
	}
}

func assertStopped(c *C, lifespanCh <-chan *T, timeout time.Duration) {
	select {
	case <-lifespanCh:
	case <-time.After(timeout):
		c.Errorf("Consumer has not stopped")
	}
}

func assertRunning(c *C, lifespanCh <-chan *T, timeout time.Duration) {
	select {
	case <-lifespanCh:
		c.Errorf("Premature stop")
	case <-time.After(timeout):
	}
}
