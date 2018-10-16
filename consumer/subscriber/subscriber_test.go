package subscriber

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/samuel/go-zookeeper/zk"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type SubscriberSuite struct {
	ns     *actor.Descriptor
	zkConn *zk.Conn
}

var _ = Suite(&SubscriberSuite{})

func (s *SubscriberSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	var err error
	sessionTimeout := 300 * time.Second
	s.zkConn, _, err = zk.Connect(testhelpers.ZookeeperPeers, sessionTimeout)
	c.Assert(err, IsNil)
}

func (s *SubscriberSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

// When a list of topics is sent to the `topics()` channel, a membership change
// is received with the same list of topics for the registrator name.
func (s *SubscriberSuite) TestSimpleSubscribe(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.zkConn)
	defer ss.Stop()

	// When
	ss.Topics() <- []string{"foo", "bar"}

	// Then
	c.Assert(<-ss.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}})
}

// When topic subscription changes occur in close succession only one
// membership change notification is received back with the most recent topic
// list for the registrator name.
func (s *SubscriberSuite) TestSubscribeSequence(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.zkConn)
	defer ss.Stop()
	ss.Topics() <- []string{"foo", "bar"}

	// When
	ss.Topics() <- []string{"blah", "bazz"}

	// Then
	assertSubscription(c, ss.Subscriptions(),
		map[string][]string{"m1": {"bazz", "blah"}}, 3*time.Second)
}

// If a group member resubscribes to the same list of topics, then the same
// member subscriptions are returned.
func (s *SubscriberSuite) TestReSubscribe(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	ss1.Topics() <- []string{"foo", "bar"}

	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	ss2.Topics() <- []string{"bazz", "bar"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bar", "bazz"},
	}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	// When
	ss1.Topics() <- []string{"foo", "bar"}

	// Then
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
}

// To deleteMemberSubscription from all topics an empty topic list can be sent.
func (s *SubscriberSuite) TestSubscribeToNothing(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo"}
	membership := map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	// When
	ss1.Topics() <- []string{}

	// Then
	membership = map[string][]string{"m2": {"foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
}

// To deleteMemberSubscription from all topics nil value can be sent.
func (s *SubscriberSuite) TestSubscribeToNil(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo"}
	membership := map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	// When
	ss1.Topics() <- nil

	// Then
	membership = map[string][]string{"m2": {"foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
}

// It is possible to subscribe to a non-empty list of topics after
// unsubscribing from everything.
func (s *SubscriberSuite) TestSomethingAfterNothingBug(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()

	ss1.Topics() <- []string{"foo"}
	c.Assert(<-ss1.Subscriptions(), DeepEquals, map[string][]string{"m1": {"foo"}})
	ss1.Topics() <- []string{}
	c.Assert(<-ss1.Subscriptions(), DeepEquals, map[string][]string{})

	// When
	ss1.Topics() <- []string{"foo"}

	// Then
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"foo"}})
}

// When several different registrator instances subscribe to the same group,
// they all receive identical membership change notifications that include all
// their subscription.
func (s *SubscriberSuite) TestMembershipChanges(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	cfg3 := newConfig("m3")
	ss3 := Spawn(s.ns.NewChild("m3"), "g1", cfg3, s.zkConn)
	defer ss3.Stop()

	// When
	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo"}
	ss3.Topics() <- []string{"foo", "bazz", "blah"}

	// Then
	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"foo"},
		"m3": {"bazz", "blah", "foo"}}

	assertSubscription(c, ss1.Subscriptions(), membership, 5*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 5*time.Second)
	assertSubscription(c, ss3.Subscriptions(), membership, 5*time.Second)
}

// Redundant updates used to be ignored, but that turned out to be wrong. Due
// to the ZooKeeper single-fire watch semantic it is possible to miss
// intermediate changes and only see the final subscription state, which could
// looked the same as the last a subscriber had seen and would be ignored. But
// topic consumers could have been changed and in need of rewiring.
func (s *SubscriberSuite) TestRedundantUpdateBug(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()

	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)

	// When
	ss2.Topics() <- []string{"bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	// Then
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
}

// If a subscriber registration in ZooKeeper disappears, that can happened
// during a long failover, then it is restored.
func (s *SubscriberSuite) TestMissingSubscriptionBug(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()

	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	// When: brute-force remove g1/m1 subscription to simulate session
	// expiration due to ZooKeeper connection loss.
	ss1.kazooModel.EnsureMemberSubscription(nil)

	// Then
	// Both nodes see the group state without m1:
	membership = map[string][]string{
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
	// But then m1 restores its subscriptions:
	membership = map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
}

// If a subscriber registration in ZooKeeper is different from the list of
// subscribed topics, then correct registration is restored.
func (s *SubscriberSuite) TestMissingOutdatedSubscription(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()

	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	// When: Modify the m1 subscriptions to simulate stale data session
	// expiration due to ZooKeeper connection loss.
	ss1.kazooModel.EnsureMemberSubscription([]string{"foo", "bazz"})

	// Then
	// Both nodes see the group state with the incorrect m1 subscription first:
	membership = map[string][]string{
		"m1": {"bazz", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
	// But then m1 restores its subscriptions:
	membership = map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
}

// When a group registrator claims a topic partitions it becomes its owner.
func (s *SubscriberSuite) TestClaimPartition(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.zkConn)
	defer ss.Stop()
	cancelCh := make(chan none.T)

	owner, err := ss.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	// When
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()

	// Then
	owner, err = ss.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a consumer group member instance tries to acquire a partition that has
// already been acquired by another member then it fails.
func (s *SubscriberSuite) TestClaimPartitionClaimed(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	cancelCh := make(chan none.T)
	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()

	// Close cancel channel so that "m2" tries claiming the partition only once,
	// and gives up after the first failure.
	close(cancelCh)

	// When
	claim2 := ss2.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := ss1.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// It is ok to claim the same partition twice by the same group member.
func (s *SubscriberSuite) TestClaimPartitionTwice(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.zkConn)
	defer ss.Stop()
	cancelCh := make(chan none.T)

	// When
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()
	claim2 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := ss.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a partition has been claimed more then once then it is release as soon as
// any of the claims is revoked.
func (s *SubscriberSuite) TestReleasePartition(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.zkConn)
	defer ss.Stop()
	cancelCh := make(chan none.T)
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	claim2 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)

	// When
	claim2() // the second claim is revoked here but it could have been any.

	// Then
	owner, err := ss.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	claim1()
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *SubscriberSuite) TestClaimPartitionParallel(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	cancelCh := make(chan none.T)

	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
		print("*** m1 released\n")
	}()

	// When: block here until m1 releases the claim over foo/1.
	claim2 := ss2.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim2()

	// Then: the partition is claimed by m2.
	owner, err := ss2.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m2")
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *SubscriberSuite) TestClaimPartitionCanceled(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	cancelCh1 := make(chan none.T)
	cancelCh2 := make(chan none.T)
	wg := &sync.WaitGroup{}

	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// This goroutine will cancel the claim of m2 before, m1 releases the partition.
	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(150 * time.Millisecond)
		close(cancelCh2)
	}()

	// When
	claim2 := ss2.ClaimPartition(s.ns, "foo", 1, cancelCh2)
	defer claim2()

	// Then: the partition is still claimed by m1.
	owner, err := ss2.kazooModel.GetPartitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")

	// Wait for all test goroutines to stop.
	wg.Wait()
}

// If claiming a partition fails then the subscription is updated to make all
// group members re-read the entire group subscription state.
func (s *SubscriberSuite) TestClaimClaimed(c *C) {
	cfg1 := newConfig("m1")
	cfg1.Consumer.RetryBackoff = 150 * time.Millisecond
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	cfg2.Consumer.RetryBackoff = 150 * time.Millisecond
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)
	defer ss2.Stop()
	cfg3 := newConfig("m3")
	cfg3.Consumer.RetryBackoff = 150 * time.Millisecond
	ss3 := Spawn(s.ns.NewChild("m3"), "g1", cfg3, s.zkConn)
	defer ss3.Stop()
	cancelCh := make(chan none.T)
	wg := &sync.WaitGroup{}

	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}
	ss3.Topics() <- []string{"bazz"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bazz", "blah", "foo"},
		"m3": {"bazz"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss3.Subscriptions(), membership, 3*time.Second)

	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()

	// When: try to claim a partition that is claimed by another member.
	wg.Add(1)
	go func() {
		wg.Done()
		ss2.ClaimPartition(s.ns, "foo", 1, cancelCh)()
	}()

	// After the retry backoff timeout is elapsed all members get their
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss3.Subscriptions(), membership, 3*time.Second)

	close(cancelCh)
	// Wait for all test goroutines to stop.
	wg.Wait()
}

func (s *SubscriberSuite) TestDeleteGroupIfEmpty(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.zkConn)
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.zkConn)

	ss1.Topics() <- []string{"foo"}
	ss2.Topics() <- []string{"foo"}

	membership := map[string][]string{
		"m1": {"foo"},
		"m2": {"foo"}}
	assertSubscription(c, ss1.Subscriptions(), membership, 3*time.Second)
	assertSubscription(c, ss2.Subscriptions(), membership, 3*time.Second)

	cancelCh := make(chan none.T)

	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh)
	claim2 := ss2.ClaimPartition(s.ns, "foo", 2, cancelCh)

	ss1.DeleteGroupIfEmpty()
	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m1",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/foo",
		"/consumers/g1/owners/foo/1",
		"/consumers/g1/owners/foo/2",
	} {
		_, _, err := s.zkConn.Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	claim1()
	ss1.DeleteGroupIfEmpty()
	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m1",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/foo",
		"/consumers/g1/owners/foo/2",
	} {
		_, _, err := s.zkConn.Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	ss1.Stop()
	ss1.DeleteGroupIfEmpty()
	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/foo",
		"/consumers/g1/owners/foo/2",
	} {
		_, _, err := s.zkConn.Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	claim2()
	ss1.DeleteGroupIfEmpty()
	for _, path := range []string{
		"/consumers",
		"/consumers/g1",
		"/consumers/g1/ids",
		"/consumers/g1/ids/m2",
		"/consumers/g1/owners",
		"/consumers/g1/owners/foo",
	} {
		_, _, err := s.zkConn.Get(path)
		c.Assert(err, IsNil, Commentf(path))
	}

	ss2.Stop()
	ss1.DeleteGroupIfEmpty()
	_, _, err := s.zkConn.Get("/consumers/g1")
	c.Assert(err, Equals, zk.ErrNoNode)
}

func newConfig(clientId string) *config.Proxy {
	cfg := config.DefaultProxy()
	cfg.ClientID = clientId
	return cfg
}

func assertSubscription(c *C, ch <-chan map[string][]string, want map[string][]string, timeout time.Duration) {
	for {
		select {
		case got := <-ch:
			if reflect.DeepEqual(got, want) {
				return
			}
		case <-time.After(timeout):
			c.Errorf("Timeout waiting for %v", want)
			return
		}
	}
}
