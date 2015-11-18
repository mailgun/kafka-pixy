package pixy

import (
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/wvanbergen/kazoo-go"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
	"github.com/mailgun/kafka-pixy/config"
)

type ConsumerGroupRegistrySuite struct {
	cid       *sarama.ContextID
	kazooConn *kazoo.Kazoo
}

var _ = Suite(&ConsumerGroupRegistrySuite{})

func (s *ConsumerGroupRegistrySuite) SetUpSuite(c *C) {
	InitTestLog()

	s.cid = sarama.RootCID.NewChild("cgr-test")
	var err error
	s.kazooConn, err = kazoo.NewKazoo(testZookeeperPeers, kazoo.NewConfig())
	c.Assert(err, IsNil)
}

func (s *ConsumerGroupRegistrySuite) TestSimpleSubscribe(c *C) {
	// Given
	config := config.Default()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()

	// When
	cgr.topics() <- []string{"foo", "bar"}

	// Then
	c.Assert(<-cgr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}})
}

func (s *ConsumerGroupRegistrySuite) TestResubscribe(c *C) {
	// Given
	config := config.Default()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cgr.topics() <- []string{"foo", "bar"}

	// When
	cgr.topics() <- []string{"blah", "bazz"}

	// Then
	c.Assert(<-cgr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bazz", "blah"}})
}

func (s *ConsumerGroupRegistrySuite) TestSubscribeToNothing(c *C) {
	// Given
	config := config.Default()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cgr.topics() <- []string{"foo", "bar"}
	c.Assert(<-cgr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {"bar", "foo"}})

	// When
	cgr.topics() <- []string{}

	// Then
	c.Assert(<-cgr.membershipChanges(), DeepEquals,
		map[string][]string{"m1": {}})
}

func (s *ConsumerGroupRegistrySuite) TestMembershipChanges(c *C) {
	// Given
	config := config.Default()
	config.Consumer.RebalanceDelay = 200 * time.Millisecond
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cgr.topics() <- []string{"foo", "bar"}

	// When
	cgr2 := spawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.stop()
	cgr2.topics() <- []string{"foo"}

	cgr3 := spawnConsumerGroupRegister("cgr_test", "m3", config, s.kazooConn)
	defer cgr3.stop()
	cgr3.topics() <- []string{"foo", "bazz", "blah"}

	// Then
	c.Assert(<-cgr.membershipChanges(), DeepEquals,
		map[string][]string{
			"m1": {"bar", "foo"},
			"m2": {"foo"},
			"m3": {"bazz", "blah", "foo"}})
}

func (s *ConsumerGroupRegistrySuite) TestClaimPartition(c *C) {
	// Given
	config := config.Default()
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cancelCh := make(chan none)

	owner, err := cgr.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "")

	// When
	claim1 := cgr.claimPartition(s.cid, "foo", 1, cancelCh)
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
	config := config.Default()
	cgr1 := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.stop()
	cgr2 := spawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.stop()
	cancelCh := make(chan none)
	close(cancelCh) // there will be no retries

	claim1 := cgr1.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim1()

	// When
	claim2 := cgr2.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim2()

	// Then
	owner, err := cgr1.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")
}

// It is ok to claim the same partition twice by the same group member.
func (s *ConsumerGroupRegistrySuite) TestClaimPartitionTwice(c *C) {
	// Given
	config := config.Default()
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cancelCh := make(chan none)

	// When
	claim1 := cgr.claimPartition(s.cid, "foo", 1, cancelCh)
	defer claim1()
	claim2 := cgr.claimPartition(s.cid, "foo", 1, cancelCh)
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
	config := config.Default()
	cgr := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr.stop()
	cancelCh := make(chan none)
	claim1 := cgr.claimPartition(s.cid, "foo", 1, cancelCh)
	claim2 := cgr.claimPartition(s.cid, "foo", 1, cancelCh)

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
	config := config.Default()
	cgr1 := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.stop()
	cgr2 := spawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.stop()
	cancelCh := make(chan none)

	claim1 := cgr1.claimPartition(s.cid, "foo", 1, cancelCh)
	go func() {
		time.Sleep(300 * time.Millisecond)
		claim1()
	}()

	// When: block here until m1 releases the claim over foo/1.
	claim2 := cgr2.claimPartition(s.cid, "foo", 1, cancelCh)
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
	config := config.Default()
	cgr1 := spawnConsumerGroupRegister("cgr_test", "m1", config, s.kazooConn)
	defer cgr1.stop()
	cgr2 := spawnConsumerGroupRegister("cgr_test", "m2", config, s.kazooConn)
	defer cgr2.stop()
	cancelCh1 := make(chan none)
	cancelCh2 := make(chan none)
	wg := &sync.WaitGroup{}

	claim1 := cgr1.claimPartition(s.cid, "foo", 1, cancelCh1)
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
	claim2 := cgr2.claimPartition(s.cid, "foo", 1, cancelCh2)
	defer claim2()

	// Then: the partition is still claimed by m1.
	owner, err := cgr2.partitionOwner("foo", 1)
	c.Assert(err, IsNil)
	c.Assert(owner, Equals, "m1")

	// Wait for all test goroutines to stop.
	wg.Wait()
}
