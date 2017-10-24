package subscriber

import (
	"sync"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kazoo-go"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type GroupMemberSuite struct {
	ns       *actor.Descriptor
	kazooClt *kazoo.Kazoo
}

var _ = Suite(&GroupMemberSuite{})

func (s *GroupMemberSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	var err error
	s.kazooClt, err = kazoo.NewKazoo(testhelpers.ZookeeperPeers, kazoo.NewConfig())
	c.Assert(err, IsNil)
}

func (s *GroupMemberSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
}

func (s *GroupMemberSuite) TestNormalizeTopics(c *C) {
	c.Assert(normalizeTopics(nil), DeepEquals, []string(nil))
	c.Assert(normalizeTopics([]string{}), DeepEquals, []string(nil))
	c.Assert(normalizeTopics([]string{"c", "a", "b"}), DeepEquals, []string{"a", "b", "c"})

	c.Assert(normalizeTopics([]string{"c", "a", "b"}), Not(DeepEquals), []string{"a", "b"})
}

func (s *GroupMemberSuite) TestTopicsEqual(c *C) {
	c.Assert(topicsEqual([]string{}, nil), Equals, true)
	c.Assert(topicsEqual(nil, []string{}), Equals, true)
	c.Assert(topicsEqual([]string{}, []string{}), Equals, true)
	c.Assert(topicsEqual([]string{"a"}, []string{"a"}), Equals, true)
	c.Assert(topicsEqual([]string{"a", "b", "c"}, []string{"a", "b", "c"}), Equals, true)

	c.Assert(topicsEqual([]string{"a", "b", "c"}, []string{"a", "b"}), Equals, false)
	c.Assert(topicsEqual([]string{"a", "b"}, []string{"b", "a"}), Equals, false)
}

// When a list of topics is sent to the `topics()` channel, a membership change
// is received with the same list of topics for the registrator name.
func (s *GroupMemberSuite) TestSimpleSubscribe(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.kazooClt)
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
func (s *GroupMemberSuite) TestSubscribeSequence(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.kazooClt)
	defer ss.Stop()
	ss.Topics() <- []string{"foo", "bar"}

	// When
	ss.Topics() <- []string{"blah", "bazz"}

	// Then
	c.Assert(<-ss.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bazz", "blah"}})
}

// If a group member resubscribes to the same list of topics, then nothing is
// updated.
func (s *GroupMemberSuite) TestReSubscribe(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	ss1.Topics() <- []string{"foo", "bar"}

	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()
	ss2.Topics() <- []string{"bazz", "bar"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bar", "bazz"},
	}
	c.Assert(<-ss1.Subscriptions(), DeepEquals, membership)
	c.Assert(<-ss2.Subscriptions(), DeepEquals, membership)

	// When
	ss1.Topics() <- []string{"foo", "bar"}

	// Then
	select {
	case update := <-ss1.Subscriptions():
		c.Errorf("Unexpected update: %v", update)
	case update := <-ss2.Subscriptions():
		c.Errorf("Unexpected update: %v", update)
	case <-time.After(300 * time.Millisecond):
	}
}

// To unsubscribe from all topics an empty topic list can be sent.
func (s *GroupMemberSuite) TestSubscribeToNothing(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()
	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo"}
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}})
	c.Assert(<-ss2.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}})

	// When
	ss1.Topics() <- []string{}

	// Then
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m2": {"foo"}})
	c.Assert(<-ss2.Subscriptions(), DeepEquals,
		map[string][]string{"m2": {"foo"}})
}

// To unsubscribe from all topics nil value can be sent.
func (s *GroupMemberSuite) TestSubscribeToNil(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()
	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo"}
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}})
	c.Assert(<-ss2.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}, "m2": {"foo"}})

	// When
	ss1.Topics() <- nil

	// Then
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m2": {"foo"}})
	c.Assert(<-ss2.Subscriptions(), DeepEquals,
		map[string][]string{"m2": {"foo"}})
}

// It is possible to subscribe to a non-empty list of topics after
// unsubscribing from everything.
func (s *GroupMemberSuite) TestSomethingAfterNothingBug(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()

	ss1.Topics() <- []string{"foo"}
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"foo"}})
	ss1.Topics() <- []string{}
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{})

	// When
	ss1.Topics() <- []string{"foo"}

	// Then
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{"m1": {"foo"}})
}

// When several different registrator instances subscribe to the same group,
// they all receive identical membership change notifications that include all
// their subscription.
func (s *GroupMemberSuite) TestMembershipChanges(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()
	cfg3 := newConfig("m3")
	ss3 := Spawn(s.ns.NewChild("m3"), "g1", cfg3, s.kazooClt)
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

	c.Assert(<-ss1.Subscriptions(), DeepEquals, membership)
	c.Assert(<-ss2.Subscriptions(), DeepEquals, membership)
	c.Assert(<-ss3.Subscriptions(), DeepEquals, membership)
}

// Redundant updates used to be ignored, but that turned out to be wrong. Due
// to the ZooKeeper single-fire watch semantic it is possible to miss
// intermediate changes and only see the final subscription state, which make
// look the same as the last a subscriber had seen and be ignored. But topic
// consumers could have been changed and in need of rewiring.
func (s *GroupMemberSuite) TestRedundantUpdateBug(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()

	ss1.Topics() <- []string{"foo", "bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{
			"m1": {"bar", "foo"},
			"m2": {"bazz", "blah", "foo"}})

	// When
	ss2.Topics() <- []string{"bar"}
	ss2.Topics() <- []string{"foo", "bazz", "blah"}

	// Then
	c.Assert(<-ss1.Subscriptions(), DeepEquals,
		map[string][]string{
			"m1": {"bar", "foo"},
			"m2": {"bazz", "blah", "foo"}})
}

// When a group registrator claims a topic partitions it becomes its owner.
func (s *GroupMemberSuite) TestClaimPartition(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.kazooClt)
	defer ss.Stop()
	cancelCh := make(chan none.T)

	owner, err := partitionOwner(ss, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	// When
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()

	// Then
	owner, err = partitionOwner(ss, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a consumer group member instance tries to acquire a partition that has
// already been acquired by another member then it fails.
func (s *GroupMemberSuite) TestClaimPartitionClaimed(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
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
	owner, err := partitionOwner(ss1, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// It is ok to claim the same partition twice by the same group member.
func (s *GroupMemberSuite) TestClaimPartitionTwice(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.kazooClt)
	defer ss.Stop()
	cancelCh := make(chan none.T)

	// When
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim1()
	claim2 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := partitionOwner(ss, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a partition has been claimed more then once then it is release as soon as
// any of the claims is revoked.
func (s *GroupMemberSuite) TestReleasePartition(c *C) {
	cfg := newConfig("m1")
	ss := Spawn(s.ns.NewChild("m1"), "g1", cfg, s.kazooClt)
	defer ss.Stop()
	cancelCh := make(chan none.T)
	claim1 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)
	claim2 := ss.ClaimPartition(s.ns, "foo", 1, cancelCh)

	// When
	claim2() // the second claim is revoked here but it could have been any.

	// Then
	owner, err := partitionOwner(ss, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	claim1()
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *GroupMemberSuite) TestClaimPartitionParallel(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
	defer ss2.Stop()
	cancelCh := make(chan none.T)

	claim1 := ss1.ClaimPartition(s.ns, "foo", 1, cancelCh)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// When: block here until m1 releases the claim over foo/1.
	claim2 := ss2.ClaimPartition(s.ns, "foo", 1, cancelCh)
	defer claim2()

	// Then: the partition is claimed by m2.
	owner, err := partitionOwner(ss2, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m2")
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *GroupMemberSuite) TestClaimPartitionCanceled(c *C) {
	cfg1 := newConfig("m1")
	ss1 := Spawn(s.ns.NewChild("m1"), "g1", cfg1, s.kazooClt)
	defer ss1.Stop()
	cfg2 := newConfig("m2")
	ss2 := Spawn(s.ns.NewChild("m2"), "g1", cfg2, s.kazooClt)
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
	owner, err := partitionOwner(ss2, "foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")

	// Wait for all test goroutines to stop.
	wg.Wait()
}

// partitionOwner returns the id of the consumer group member that has claimed
// the specified topic/partition.
func partitionOwner(gm *T, topic string, partition int32) (string, error) {
	owner, err := gm.groupZNode.PartitionOwner(topic, partition)
	if err != nil {
		return "", err
	}
	if owner == nil {
		return "", nil
	}
	return owner.ID, nil
}

func newConfig(clientId string) *config.Proxy {
	cfg := config.DefaultProxy()
	cfg.ClientID = clientId
	cfg.Consumer.RebalanceDelay = 100 * time.Millisecond
	return cfg
}
