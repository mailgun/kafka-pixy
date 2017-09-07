package dispatcher

import (
	"testing"

	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type DispatcherSuite struct {
	cfg    *config.Proxy
	ns     *actor.Descriptor
	groupF *fakeFactory
	topicF *fakeFactory
}

var _ = Suite(&DispatcherSuite{})

func (s *DispatcherSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *DispatcherSuite) SetUpTest(c *C) {
	s.cfg = testhelpers.NewTestProxyCfg("test")
	s.ns = actor.Root().NewChild("T")
	s.groupF = newFakeFactory(dfGroup)
	s.topicF = newFakeFactory(dfTopic)
}

// Dispatcher sends requests down to a child corresponding to a key value
// determined by Factory.KeyOf. If there is no a child for the key yet, then it
// is spawned.
func (s *DispatcherSuite) TestChildrenOnDemand(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	requests := []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g2", "t2"),
		2: consumer.NewRequest("g2", "t3"),
		3: consumer.NewRequest("g1", "t4"),
	}

	// When
	sendAll(d, requests)

	// Then
	var children []ChildSpec
	children = append(children, <-s.groupF.spawnedCh)
	children = append(children, <-s.groupF.spawnedCh)

	c.Assert(<-children[0].Requests(), Equals, requests[0])
	c.Assert(<-children[0].Requests(), Equals, requests[3])

	c.Assert(<-children[1].Requests(), Equals, requests[1])
	c.Assert(<-children[1].Requests(), Equals, requests[2])

	// Cleanup
	for _, child := range children {
		child.Dispose()
	}
}

// If a child terminates while there are still requests in its channel a
// successor is spawned to handle them.
func (s *DispatcherSuite) TestSuccessorSpawned(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	requests := sendAll(d, []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g1", "t2"),
		2: consumer.NewRequest("g1", "t3"),
	})

	child := <-s.groupF.spawnedCh
	c.Assert(<-child.Requests(), Equals, requests[0])
	c.Assert(<-child.Requests(), Equals, requests[1])

	// When
	child.Dispose()

	// Then
	successor := <-s.groupF.spawnedCh
	defer successor.Dispose()
	c.Assert(<-successor.Requests(), Equals, requests[2])
}

// If a child terminates while there are still requests in its channel a
// successor is spawned to handle them.
func (s *DispatcherSuite) TestDisposeTwice(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	requests := []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g1", "t2"),
	}

	d.Requests() <- requests[0]
	child := <-s.groupF.spawnedCh
	c.Assert(<-child.Requests(), Equals, requests[0])

	// When
	child.Dispose()
	child.Dispose()

	// Then: a successor is spawned on demand by the next consumer.Request.
	d.Requests() <- requests[1]
	successor := <-s.groupF.spawnedCh
	defer successor.Dispose()
	c.Assert(<-successor.Requests(), Equals, requests[1])
}

// If the requests chanel of a child is filled with requests to the maximum
// capacity, then subsequent requests for the child are rejected.
func (s *DispatcherSuite) TestRequestChOverflow(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)
	defer d.Stop()

	// Generate as many requests as the consumer.Request channel capacity.
	stuckRequests := make([]consumer.Request, s.cfg.Consumer.ChannelBufferSize)
	for i := range stuckRequests {
		stuckRequests[i].Group = "g1"
		stuckRequests[i].ResponseCh = make(chan consumer.Response, 1)
		d.Requests() <- stuckRequests[i]
	}
	overflowRq := consumer.Request{Group: "g1", ResponseCh: make(chan consumer.Response, 1)}

	// When
	d.Requests() <- overflowRq

	// Then: the overflow consumer.Request is rejected with an error
	assertRejected(c, overflowRq, consumer.ErrTooManyRequests, 100*time.Millisecond)

	// Requests feeling the g1 child channel must not have a response.
	for _, rq := range stuckRequests {
		assertNoResponse(c, rq)
	}
	// Requests addressed to another child are not affected
	anotherRq := consumer.Request{Group: "g2"}
	d.Requests() <- anotherRq
	child1 := <-s.groupF.spawnedCh
	defer child1.Dispose()
	child2 := <-s.groupF.spawnedCh
	defer child2.Dispose()
	c.Assert(<-child2.Requests(), Equals, anotherRq)

	// Drain child1 requests, to make sure that its successor is not spawned.
drain:
	for {
		select {
		case <-child1.requestsCh:
		default:
			break drain
		}
	}
}

// When a dispatcher is stopped it closes consumer.Request channels of all its children
// and waits for the children to call Dispose on their specs. If a child stops
// leaving unprocessed requests in its consumer.Request channel, then dispatcher rejects
// all leftover requests.
func (s *DispatcherSuite) TestStopLeftover(c *C) {
	d := Spawn(s.ns, s.groupF, s.cfg)

	requests := sendAll(d, []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g1", "t2"),
		2: consumer.NewRequest("g2", "t3"),
	})

	child1 := <-s.groupF.spawnedCh
	child2 := <-s.groupF.spawnedCh

	c.Assert(<-child1.Requests(), Equals, requests[0])
	c.Assert(<-child2.Requests(), Equals, requests[2])

	// When
	go d.Stop() // Stop would block so call it in a goroutine

	// Then
	// Dispatcher waits for children to stop...
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	child1.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	// Report an unknown key to make sure that it does not screw things up.
	d.disposalCh <- "unknown key"
	child2.Dispose()
	// ...and now it stops.
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, true)

	// Requests not processed by children before stopped are rejected...
	assertNoResponse(c, requests[0])
	assertRejected(c, requests[1], consumer.ErrUnavailable, 100*time.Millisecond)
	assertNoResponse(c, requests[2])
}

// If when root dispatcher is stopped then the downstream dispatcher does the
// following:
//  * it closes requests channels of its children;
//  * waits for them to call Dispose on their specs;
//  * rejects all requests left in the children requests channels;
//  * terminates;
func (s *DispatcherSuite) TestStopHierarchy(c *C) {
	rootD := Spawn(s.ns, s.groupF, s.cfg)

	requests := sendAll(rootD, []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g1", "t2"),
		2: consumer.NewRequest("g1", "t1"),
		3: consumer.NewRequest("g1", "t1"),
		4: consumer.NewRequest("g1", "t2"),
		5: consumer.NewRequest("g1", "t1"),
		6: consumer.NewRequest("g1", "t2"),
	})
	childG1 := <-s.groupF.spawnedCh
	d := Spawn(s.ns, s.topicF, s.cfg, WithChildSpec(childG1))

	childG1T1 := <-s.topicF.spawnedCh
	childG1T2 := <-s.topicF.spawnedCh

	c.Assert(<-childG1T1.Requests(), Equals, requests[0])
	c.Assert(<-childG1T1.Requests(), Equals, requests[2])
	c.Assert(<-childG1T2.Requests(), Equals, requests[1])
	c.Assert(<-childG1T2.Requests(), Equals, requests[4])

	// When: the root dispatcher is stopped.
	go rootD.Stop() // Stop would block so call it in a goroutine

	// Then: termination sequence starts...
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	c.Assert(rootD.Wait4Stop(10*time.Millisecond), Equals, false)
	childG1T1.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	c.Assert(rootD.Wait4Stop(10*time.Millisecond), Equals, false)
	childG1T2.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, true)
	c.Assert(rootD.Wait4Stop(100*time.Millisecond), Equals, true)

	assertNoResponse(c, requests[0])
	assertNoResponse(c, requests[1])
	assertNoResponse(c, requests[2])
	assertRejected(c, requests[3], consumer.ErrUnavailable, 100*time.Millisecond)
	assertNoResponse(c, requests[4])
	assertRejected(c, requests[5], consumer.ErrUnavailable, 100*time.Millisecond)
	assertRejected(c, requests[6], consumer.ErrUnavailable, 100*time.Millisecond)
}

// When the last child of a downstream dispatcher stops and there are no more
// requests pending in its channel then the downstream consumer stops as well.
func (s *DispatcherSuite) TestStopsWithLastChild(c *C) {
	rootD := Spawn(s.ns, s.groupF, s.cfg)
	defer rootD.Stop()
	requests := sendAll(rootD, []consumer.Request{
		0: consumer.NewRequest("g1", "t1"),
		1: consumer.NewRequest("g1", "t2"),
	})
	childG1 := <-s.groupF.spawnedCh
	d := Spawn(s.ns, s.topicF, s.cfg, WithChildSpec(childG1))

	childG1T1 := <-s.topicF.spawnedCh
	childG1T2 := <-s.topicF.spawnedCh

	// When/Then
	c.Assert(<-childG1T1.Requests(), Equals, requests[0])
	childG1T1.Dispose()

	// Still running because childG1T2 has not stopped yet.
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	childG1T2.Dispose()

	// childG1T2 is recreated because there are still requests in its channel.
	childG1T2 = <-s.topicF.spawnedCh
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)

	// Handle last child messages, stop it and only after that the dispatcher
	// should stop.
	c.Assert(<-childG1T2.Requests(), Equals, requests[1])
	childG1T2.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, true)
}

func (s *DispatcherSuite) TestFinalizedCalled(c *C) {
	called := make(chan none.T)
	finalizer := func() {
		close(called)
	}
	d := Spawn(s.ns, s.groupF, s.cfg, WithFinalizer(finalizer))
	d.Requests() <- consumer.NewRequest("g1", "")
	d.Requests() <- consumer.NewRequest("g2", "")
	childG1 := <-s.groupF.spawnedCh
	childG2 := <-s.groupF.spawnedCh

	// When
	go d.Stop() // Stop would block so call it in a goroutine

	// Then
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	select {
	case <-called:
		c.Error("Finalizer should not have been called yet")
	default:
	}

	childG1.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, false)
	select {
	case <-called:
		c.Error("Finalizer should not have been called yet")
	default:
	}

	childG2.Dispose()
	c.Assert(d.Wait4Stop(100*time.Millisecond), Equals, true)
	select {
	case <-called:
	default:
		c.Error("Finalizer should have been called")
	}
}

type fakeFactory struct {
	df        dispatchField
	spawnedCh chan ChildSpec
}

type dispatchField int

const (
	dfGroup dispatchField = iota
	dfTopic
)

func sendAll(d *T, requests []consumer.Request) []consumer.Request {
	for i := range requests {
		d.Requests() <- requests[i]
	}
	return requests
}

func newFakeFactory(df dispatchField) *fakeFactory {
	ff := fakeFactory{
		df:        df,
		spawnedCh: make(chan ChildSpec, 100),
	}
	return &ff
}

func (ff *fakeFactory) KeyOf(rq consumer.Request) Key {
	switch ff.df {
	case dfGroup:
		return Key(rq.Group)
	case dfTopic:
		return Key(rq.Topic)
	}
	return Key("")
}

func (ff *fakeFactory) SpawnChild(cs ChildSpec) {
	ff.spawnedCh <- cs
}

func assertRejected(c *C, rq consumer.Request, err error, timeout time.Duration) {
	select {
	case rs := <-chan consumer.Response(rq.ResponseCh):
		c.Assert(rs.Err, Equals, err)
	case <-time.After(timeout):
		c.Errorf("Error response expected")
	}
}

func assertNoResponse(c *C, rq consumer.Request) {
	select {
	case rs := <-chan consumer.Response(rq.ResponseCh):
		c.Errorf("Unexpected response: %v", rs)
	default:
	}
}
