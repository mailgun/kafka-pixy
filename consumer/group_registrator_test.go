package consumer

import (
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/wvanbergen/kazoo-go"
	. "gopkg.in/check.v1"
)

type GroupRegistratorSuite struct {
	cid       *actor.ID
	kazooConn *kazoo.Kazoo
}

var _ = Suite(&GroupRegistratorSuite{})

func (s *GroupRegistratorSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
	s.cid = actor.RootID.NewChild("gr-test")
	var err error
	s.kazooConn, err = kazoo.NewKazoo(testhelpers.ZookeeperPeers, kazoo.NewConfig())
	c.Assert(err, IsNil)
}

// When a list of topics is sent to the `topics()` channel, a membership change
// is received with the same list of topics for the registrator name.
func (s *GroupRegistratorSuite) TestSimpleSubscribe(c *C) {
	// Given
	cfg := config.Default()
	cfg.Consumer.RebalanceDelay = 200 * time.Millisecond
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()

	// When
	gr.topics() <- []string{"foo", "bar"}

	// Then
	c.Assert(<-gr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}})
}

// When topic subscription changes occur in close succession only one
// membership change notification is received back with the most recent topic
// list for the registrator name.
func (s *GroupRegistratorSuite) TestSubscribeSequence(c *C) {
	// Given
	cfg := config.Default()
	cfg.Consumer.RebalanceDelay = 200 * time.Millisecond
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()
	gr.topics() <- []string{"foo", "bar"}

	// When
	gr.topics() <- []string{"blah", "bazz"}

	// Then
	c.Assert(<-gr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bazz", "blah"}})
}

// If a group registrator resubscribes to the same list of topics, every group
// member gets a membership change notification same as the previous one.
func (s *GroupRegistratorSuite) TestReSubscribe(c *C) {
	// Given
	cfg := config.Default()
	cfg.Consumer.RebalanceDelay = 100 * time.Millisecond

	gr1 := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr1.stop()
	gr1.topics() <- []string{"foo", "bar"}

	gr2 := spawnGroupRegistrator("gr_test", "m2", cfg, s.kazooConn)
	defer gr2.stop()
	gr2.topics() <- []string{"bazz", "bar"}

	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"bar", "bazz"},
	}
	c.Assert(<-gr1.membershipChanges(), DeepEquals, membership)
	c.Assert(<-gr2.membershipChanges(), DeepEquals, membership)

	// When
	gr1.topics() <- []string{"foo", "bar"}

	// Then
	c.Assert(<-gr1.membershipChanges(), DeepEquals, membership)
	c.Assert(<-gr2.membershipChanges(), DeepEquals, membership)
}

// To unsubscribe from all topics an empty topic list can be sent.
func (s *GroupRegistratorSuite) TestSubscribeToNothing(c *C) {
	// Given
	cfg := config.Default()
	cfg.Consumer.RebalanceDelay = 200 * time.Millisecond
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()
	gr.topics() <- []string{"foo", "bar"}
	c.Assert(<-gr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}})

	// When
	gr.topics() <- []string{}

	// Then
	c.Assert(<-gr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {}})
}

// When several different registrator instances subscribe to the same group,
// they all receive identical membership change notifications that include all
// their subscription.
func (s *GroupRegistratorSuite) TestMembershipChanges(c *C) {
	// Given
	cfg := config.Default()
	cfg.Consumer.RebalanceDelay = 200 * time.Millisecond
	gr1 := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr1.stop()
	gr2 := spawnGroupRegistrator("gr_test", "m2", cfg, s.kazooConn)
	defer gr2.stop()
	gr3 := spawnGroupRegistrator("gr_test", "m3", cfg, s.kazooConn)
	defer gr3.stop()

	// When
	gr1.topics() <- []string{"foo", "bar"}
	gr2.topics() <- []string{"foo"}
	gr3.topics() <- []string{"foo", "bazz", "blah"}

	// Then
	membership := map[string][]string{
		"m1": {"bar", "foo"},
		"m2": {"foo"},
		"m3": {"bazz", "blah", "foo"}}

	c.Assert(<-gr1.membershipChanges(), DeepEquals, membership)
	c.Assert(<-gr2.membershipChanges(), DeepEquals, membership)
	c.Assert(<-gr3.membershipChanges(), DeepEquals, membership)
}

// When a group registrator claims a topic partitions it becomes its owner.
func (s *GroupRegistratorSuite) TestClaimPartition(c *C) {
	// Given
	cfg := config.Default()
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()
	cancelCh := make(chan none.T)

	owner, err := gr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	// When
	claim1 := gr.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim1()

	// Then
	owner, err = gr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a consumer group member instance tries to acquire a partition that has
// already been acquired by another member then it fails.
func (s *GroupRegistratorSuite) TestClaimPartitionClaimed(c *C) {
	// Given
	cfg := config.Default()
	gr1 := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr1.stop()
	gr2 := spawnGroupRegistrator("gr_test", "m2", cfg, s.kazooConn)
	defer gr2.stop()
	cancelCh := make(chan none.T)
	close(cancelCh) // there will be no retries

	claim1 := gr1.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim1()

	// When
	claim2 := gr2.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := gr1.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// It is ok to claim the same partition twice by the same group member.
func (s *GroupRegistratorSuite) TestClaimPartitionTwice(c *C) {
	// Given
	cfg := config.Default()
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()
	cancelCh := make(chan none.T)

	// When
	claim1 := gr.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim1()
	claim2 := gr.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := gr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a partition has been claimed more then once then it is release as soon as
// any of the claims is revoked.
func (s *GroupRegistratorSuite) TestReleasePartition(c *C) {
	// Given
	cfg := config.Default()
	gr := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr.stop()
	cancelCh := make(chan none.T)
	claim1 := gr.claimPartition(s.cid, "foo", 1, cancelCh)
	claim2 := gr.claimPartition(s.cid, "foo", 1, cancelCh)

	// When
	claim2() // the second claim is revoked here but it could have been any.

	// Then
	owner, err := gr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	claim1()
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *GroupRegistratorSuite) TestClaimPartitionParallel(c *C) {
	// Given
	cfg := config.Default()
	gr1 := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr1.stop()
	gr2 := spawnGroupRegistrator("gr_test", "m2", cfg, s.kazooConn)
	defer gr2.stop()
	cancelCh := make(chan none.T)

	claim1 := gr1.claimPartition(s.cid, "foo", 1, cancelCh)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// When: block here until m1 releases the claim over foo/1.
	claim2 := gr2.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim2()

	// Then: the partition is claimed by m2.
	owner, err := gr2.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m2")
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *GroupRegistratorSuite) TestClaimPartitionCanceled(c *C) {
	// Given
	cfg := config.Default()
	gr1 := spawnGroupRegistrator("gr_test", "m1", cfg, s.kazooConn)
	defer gr1.stop()
	gr2 := spawnGroupRegistrator("gr_test", "m2", cfg, s.kazooConn)
	defer gr2.stop()
	cancelCh1 := make(chan none.T)
	cancelCh2 := make(chan none.T)
	wg := &sync.WaitGroup{}

	claim1 := gr1.claimPartition(s.cid, "foo", 1, cancelCh1)
	spawn(wg, func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	})

	// This goroutine will cancel the claim of m2 before, m1 releases the partition.
	spawn(wg, func() {
		time.Sleep(150 * time.Millisecond)
		close(cancelCh2)
	})

	// When
	claim2 := gr2.claimPartition(s.cid, "foo", 1, cancelCh2)
	defer claim2()

	// Then: the partition is still claimed by m1.
	owner, err := gr2.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")

	// Wait for all test goroutines to stop.
	wg.Wait()
}
