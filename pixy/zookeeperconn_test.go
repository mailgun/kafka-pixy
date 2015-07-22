package pixy

import (
	"sort"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/kazoo-go"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type ConsumerGroupRegistrySuite struct {
	kazooConn *kazoo.Kazoo
}

var _ = Suite(&ConsumerGroupRegistrySuite{})

func (s *ConsumerGroupRegistrySuite) SetUpSuite(c *C) {
	InitTestLog()

	var err error
	s.kazooConn, err = kazoo.NewKazoo(testZookeeperPeers, kazoo.NewConfig())
	c.Assert(err, IsNil)
}

func (s *ConsumerGroupRegistrySuite) TestSimpleSubscribe(c *C) {
	// Given
	config := NewKafkaClientCfg()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	c.Assert(<-cgr.MembershipChanges(), DeepEquals, []GroupMemberSubscription{})

	// When
	cgr.Topics() <- []string{"foo", "bar"}

	// Then
	assertGroupMembers(c, <-cgr.MembershipChanges(),
		[]GroupMemberSubscription{
			GroupMemberSubscription{"m1", []string{"bar", "foo"}}})
}

func (s *ConsumerGroupRegistrySuite) TestResubscribe(c *C) {
	// Given
	config := NewKafkaClientCfg()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	c.Assert(<-cgr.MembershipChanges(), DeepEquals, []GroupMemberSubscription{})
	cgr.Topics() <- []string{"foo", "bar"}

	// When
	cgr.Topics() <- []string{"blah", "bazz"}

	// Then
	assertGroupMembers(c, <-cgr.MembershipChanges(),
		[]GroupMemberSubscription{
			GroupMemberSubscription{"m1", []string{"bazz", "blah"}}})
}

func (s *ConsumerGroupRegistrySuite) TestSubscribeToNothing(c *C) {
	// Given
	config := NewKafkaClientCfg()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	c.Assert(<-cgr.MembershipChanges(), DeepEquals, []GroupMemberSubscription{})
	cgr.Topics() <- []string{"foo", "bar"}
	assertGroupMembers(c, <-cgr.MembershipChanges(),
		[]GroupMemberSubscription{
			GroupMemberSubscription{"m1", []string{"bar", "foo"}}})

	// When
	cgr.Topics() <- []string{}

	// Then
	assertGroupMembers(c, <-cgr.MembershipChanges(),
		[]GroupMemberSubscription{
			GroupMemberSubscription{"m1", []string{}}})
}

func (s *ConsumerGroupRegistrySuite) TestMembershipChanges(c *C) {
	// Given
	config := NewKafkaClientCfg()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	c.Assert(<-cgr.MembershipChanges(), DeepEquals, []GroupMemberSubscription{})
	cgr.Topics() <- []string{"foo", "bar"}

	// When
	cgr2 := SpawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.Stop()
	cgr2.Topics() <- []string{"foo"}

	cgr3 := SpawnConsumerGroupRegister("cgr_test", "m3", config, s.kazooConn)
	defer cgr3.Stop()
	cgr3.Topics() <- []string{"foo", "bazz", "blah"}

	// Then
	assertGroupMembers(c, <-cgr.MembershipChanges(),
		[]GroupMemberSubscription{
			GroupMemberSubscription{"m1", []string{"bar", "foo"}},
			GroupMemberSubscription{"m2", []string{"foo"}},
			GroupMemberSubscription{"m3", []string{"bazz", "blah", "foo"}}})
}

func (s *ConsumerGroupRegistrySuite) TestClaimPartition(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	cancelCh := make(chan none)

	owner, err := cgr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	// When
	claim1 := cgr.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim1()

	// Then
	owner, err = cgr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a consumer group member instance tries to acquire a partition that has
// already been acquired by another member then it fails.
func (s *ConsumerGroupRegistrySuite) TestClaimPartitionClaimed(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr1 := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.Stop()
	cgr2 := SpawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.Stop()
	cancelCh := make(chan none)
	close(cancelCh) // there will be no retries

	claim1 := cgr1.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim1()

	// When
	claim2 := cgr2.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := cgr1.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// It is ok to claim the same partition twice by the same group member.
func (s *ConsumerGroupRegistrySuite) TestClaimPartitionTwice(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	cancelCh := make(chan none)

	// When
	claim1 := cgr.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim1()
	claim2 := cgr.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := cgr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// If a partition has been claimed more then once then it is release as soon as
// any of the claims is revoked.
func (s *ConsumerGroupRegistrySuite) TestReleasePartition(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.Stop()
	cancelCh := make(chan none)
	claim1 := cgr.ClaimPartition("test", "foo", 1, cancelCh)
	claim2 := cgr.ClaimPartition("test", "foo", 1, cancelCh)

	// When
	claim2() // the second claim is revoked here but it could have been any.

	// Then
	owner, err := cgr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	claim1()
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *ConsumerGroupRegistrySuite) TestClaimPartitionParallel(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr1 := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.Stop()
	cgr2 := SpawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.Stop()
	cancelCh := make(chan none)

	claim1 := cgr1.ClaimPartition("test", "foo", 1, cancelCh)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// When: block here until m1 releases the claim over foo/1.
	claim2 := cgr2.ClaimPartition("test", "foo", 1, cancelCh)
	defer claim2()

	// Then: the partition is claimed by m2.
	owner, err := cgr2.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m2")
}

// If a partition is claimed by another group member then `ClaimPartition` call
// blocks until it is released.
func (s *ConsumerGroupRegistrySuite) TestClaimPartitionCanceled(c *C) {
	// Given
	config := NewKafkaClientCfg()
	cgr1 := SpawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.Stop()
	cgr2 := SpawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.Stop()
	cancelCh1 := make(chan none)
	cancelCh2 := make(chan none)

	claim1 := cgr1.ClaimPartition("test", "foo", 1, cancelCh1)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// This goroutine will cancel the claim of m2before, m1 releases the partition.
	go func() {
		time.Sleep(150 * time.Millisecond)
		close(cancelCh2)
	}()

	// When
	claim2 := cgr2.ClaimPartition("test", "foo", 1, cancelCh2)
	defer claim2()

	// Then: the partition is still claimed by m1.
	owner, err := cgr2.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

func assertGroupMembers(c *C, obtained []GroupMemberSubscription, expected []GroupMemberSubscription) {
	sort.Sort(groupMemberSubscriptionSlice(obtained))
	sort.Sort(groupMemberSubscriptionSlice(expected))
	c.Assert(obtained, DeepEquals, expected)
}

type groupMemberSubscriptionSlice []GroupMemberSubscription

func (p groupMemberSubscriptionSlice) Len() int           { return len(p) }
func (p groupMemberSubscriptionSlice) Less(i, j int) bool { return p[i].memberID < p[j].memberID }
func (p groupMemberSubscriptionSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
