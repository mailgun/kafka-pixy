package offsetmgr

import (
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/testhelpers"
	log "github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type OffsetMgrSuite struct {
	ns *actor.ID
}

var _ = Suite(&OffsetMgrSuite{})

func (s *OffsetMgrSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	testReportErrors = true
}

func (s *OffsetMgrSuite) TearDownSuite(c *C) {
	testReportErrors = false
}

func (s *OffsetMgrSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

// When a partition consumer is created, then an initial offset is sent down
// the InitialOffset() channel.
func (s *OffsetMgrSuite) TestInitialOffset(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "bar", sarama.ErrNoError).
			SetOffset("g1", "t2", 9, 3000, "bazz", sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	defer om.Stop()

	// Then
	initialOffset := <-om.CommittedOffsets()
	c.Assert(initialOffset, DeepEquals, Offset{2000, "bar"})
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func (s *OffsetMgrSuite) TestInitialNoCoordinator(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetError("g1", sarama.ErrOffsetsLoadInProgress),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 50 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	defer om.Stop()

	// Then
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(err, DeepEquals, errNoCoordinator)
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func (s *OffsetMgrSuite) TestInitialFetchError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNotLeaderForPartition),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 50 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	// When
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	defer om.Stop()

	// Then
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(err, Equals, sarama.ErrNotLeaderForPartition)
}

// If offset commit fails then the corresponding error is sent down to the
// errors channel, but the offset manager keeps retrying until it succeeds.
func (s *OffsetMgrSuite) TestCommitError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1234, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNotLeaderForPartition),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 1000 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)

	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om.SubmitOffset(Offset{1000, "foo"})
	var wg sync.WaitGroup
	actor.Spawn(actor.RootID.NewChild("stopper"), &wg, om.Stop)

	// Then
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(err, Equals, sarama.ErrNotLeaderForPartition)

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	wg.Wait()
	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, Offset{1000, "foo"})
}

// If offset a response received from Kafka for an offset commit request does
// not contain information for a submitted offset, then offset manager keeps,
// retrying until it succeeds.
func (s *OffsetMgrSuite) TestCommitIncompleteResponse(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	offsetCommitResponse := sarama.OffsetCommitResponse{
		Errors: map[string]map[int32]sarama.KError{"t1": {2: sarama.ErrNoError}}}

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 1, 1000, "foo", sarama.ErrNoError).
			SetOffset("g1", "t1", 2, 2000, "bar", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockWrapper(&offsetCommitResponse),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 1000 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)

	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 1), "g1", "t1", 1)
	c.Assert(err, IsNil)
	<-om1.CommittedOffsets() // Ignore initial offset.
	om2, err := f.Spawn(s.ns.NewChild("g1", "t1", 2), "g1", "t1", 2)
	c.Assert(err, IsNil)
	<-om2.CommittedOffsets() // Ignore initial offset.

	// When
	om1.SubmitOffset(Offset{1001, "foo1"})
	om2.SubmitOffset(Offset{2001, "bar2"})
	var wg sync.WaitGroup
	actor.Spawn(actor.RootID.NewChild("stopper"), &wg, om1.Stop)
	actor.Spawn(actor.RootID.NewChild("stopper"), &wg, om2.Stop)

	// Then
	err = <-om1.(*offsetMgr).testErrorsCh
	c.Assert(err, Equals, sarama.ErrIncompleteResponse)
	c.Assert(<-om2.CommittedOffsets(), Equals, Offset{2001, "bar2"})

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 1, sarama.ErrNoError),
	})

	wg.Wait()
	c.Assert(<-om1.CommittedOffsets(), Equals, Offset{1001, "foo1"})
}

// It is guaranteed that a partition offset manager commits all pending offsets
// before it terminates. Note that it will try indefinitely by design.
func (s *OffsetMgrSuite) TestCommitBeforeClose(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 25 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 100 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 10 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When: a partition offset manager is closed while there is a pending commit.
	om.SubmitOffset(Offset{1001, "foo"})
	go om.Stop()

	// Then: the partition offset manager terminates only after it has
	// successfully committed the offset.

	// STAGE 1: Requests for coordinator time out.
	log.Infof("    STAGE 1")
	err = <-om.(*offsetMgr).testErrorsCh
	c.Assert(err, Equals, errRequestTimeout)

	// STAGE 2: Requests for initial offset return errors
	log.Infof("    STAGE 2")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNotLeaderForPartition),
	})
	for err = range om.(*offsetMgr).testErrorsCh {
		if err != errRequestTimeout {
			break
		}
	}
	c.Assert(err, Equals, sarama.ErrNotLeaderForPartition)

	// STAGE 3: Val commit requests fail
	log.Infof("    STAGE 3")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1234, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrOffsetMetadataTooLarge),
	})
	for err = range om.(*offsetMgr).testErrorsCh {
		if !reflect.DeepEqual(err, sarama.ErrNotLeaderForPartition) {
			break
		}
	}
	c.Assert(err, DeepEquals, sarama.ErrOffsetMetadataTooLarge)

	// STAGE 4: Finally everything is fine
	log.Infof("    STAGE 4")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})
	// The errors channel is closed when the partition offset manager has
	// terminated.
	for oce := range om.(*offsetMgr).testErrorsCh {
		log.Infof("Drain error: %v", oce)
	}

	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, Offset{1001, "foo"})
}

// Different consumer groups can keep different offsets for the same
// topic/partition, even where they have the same broker as a coordinator.
func (s *OffsetMgrSuite) TestCommitDifferentGroups(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1).
			SetCoordinator("g2", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError).
			SetOffset("g2", "t1", 7, 2000, "bar", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError).
			SetError("g2", "t1", 7, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.Spawn(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om1.SubmitOffset(Offset{1009, "foo1"})
	om1.SubmitOffset(Offset{1010, "foo2"})
	om2.SubmitOffset(Offset{2010, "bar1"})
	om2.SubmitOffset(Offset{2011, "bar2"})
	om1.SubmitOffset(Offset{1017, "foo3"})
	om2.SubmitOffset(Offset{2019, "bar3"})
	om1.Stop()
	om2.Stop()

	// Then
	committedOffset1 := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset1, DeepEquals, Offset{1017, "foo3"})
	committedOffset2 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset2, DeepEquals, Offset{2019, "bar3"})
}

func (s *OffsetMgrSuite) TestCommitNetworkError(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1).
			SetCoordinator("g2", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo1", sarama.ErrNoError).
			SetOffset("g1", "t1", 8, 2000, "foo2", sarama.ErrNoError).
			SetOffset("g2", "t1", 7, 3000, "foo3", sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om1, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.Spawn(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	om3, err := f.Spawn(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)
	om1.SubmitOffset(Offset{1001, "bar1"})
	om2.SubmitOffset(Offset{2001, "bar2"})
	om3.SubmitOffset(Offset{3001, "bar3"})

	log.Infof("*** Waiting for errors...")
	<-om1.(*offsetMgr).testErrorsCh
	<-om2.(*offsetMgr).testErrorsCh
	<-om3.(*offsetMgr).testErrorsCh

	// When
	time.Sleep(cfg.Consumer.RetryBackoff * 2)
	log.Infof("*** Network recovering...")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1).
			SetCoordinator("g2", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError).
			SetError("g1", "t1", 8, sarama.ErrNoError).
			SetError("g2", "t1", 7, sarama.ErrNoError),
	})
	om1.Stop()
	om2.Stop()
	om3.Stop()

	// Then: offset managers are able to commit offsets and terminate.
	committedOffset1 := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset1, DeepEquals, Offset{1001, "bar1"})
	committedOffset2 := lastCommittedOffset(broker1, "g1", "t1", 8)
	c.Assert(committedOffset2, DeepEquals, Offset{2001, "bar2"})
	committedOffset3 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset3, DeepEquals, Offset{3001, "bar3"})
}

func (s *OffsetMgrSuite) TestCommittedChannel(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo1", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	<-om.CommittedOffsets() // Ignore initial offset.

	// When
	om.SubmitOffset(Offset{1001, "bar1"})
	om.SubmitOffset(Offset{1002, "bar2"})
	om.SubmitOffset(Offset{1003, "bar3"})
	om.SubmitOffset(Offset{1004, "bar4"})
	om.SubmitOffset(Offset{1005, "bar5"})
	om.Stop()

	// Then
	var committedOffsets []Offset
	for committedOffset := range om.CommittedOffsets() {
		committedOffsets = append(committedOffsets, committedOffset)
	}
	c.Assert(committedOffsets, DeepEquals, []Offset{{1005, "bar5"}})
}

// Test for issue https://github.com/mailgun/kafka-pixy/issues/29. The problem
// was that if a connection to the broker was broken on the Kafka side while a
// partition manager tried to retrieve an initial commit, the later would never
// try to reestablish connection and get stuck in an infinite loop of
// unassign->assign to the same broker over and over again.
func (s *OffsetMgrSuite) TestBugConnectionRestored(c *C) {
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()
	broker2 := sarama.NewMockBroker(c, 102)

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetBroker(broker2.Addr(), broker2.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker2),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	saramaCfg := sarama.NewConfig()
	saramaCfg.Net.ReadTimeout = 100 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, saramaCfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()

	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	log.Infof("    GIVEN 1")
	// Make sure the partition offset manager established connection with broker2.
	select {
	case err = <-om.(*offsetMgr).testErrorsCh:
	case <-time.After(200 * time.Millisecond):
	}
	_, ok := err.(*net.OpError)
	c.Assert(ok, Equals, true, Commentf("Unexpected or no error: err=%v", err))

	log.Infof("    GIVEN 2")
	// Close both broker2 and the partition offset manager. That will break
	// client connection with broker2 from the broker end.
	om.Stop()
	broker2.Close()
	time.Sleep(cfg.Consumer.RetryBackoff * 2)

	log.Infof("    GIVEN 3")
	// Simulate broker restart. Make sure that the new instances listens on the
	// same port as the old one.
	broker2_2 := sarama.NewMockBrokerAddr(c, broker2.BrokerID(), broker2.Addr())
	broker2_2.SetHandlerByMap(map[string]sarama.MockResponse{
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1000, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	log.Infof("    WHEN")
	// Create a partition offset manager for the same topic partition as before.
	// It will be assigned the broken connection to broker2.
	om, err = f.Spawn(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	defer om.Stop()

	log.Infof("    THEN")
	// Then: the new partition offset manager re-establishes connection with
	// broker2 and successfully retrieves the initial offset.
	var do Offset
	select {
	case do = <-om.CommittedOffsets():
	case err = <-om.(*offsetMgr).testErrorsCh:
	case <-time.After(200 * time.Millisecond):
	}
	c.Assert(do.Val, Equals, int64(1000), Commentf("Failed to retrieve initial offset: %s", err))
}

// Test for issue https://github.com/mailgun/kafka-pixy/issues/62. The problem
// was that if a stop signal is received while there are two submitted offsets
// pending, then offset manager would stop as soon as the first one was
// committed, hereby dropping the second offset.
func (s *OffsetMgrSuite) TestBugOffsetDroppedOnStop(c *C) {
	// Given
	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 1, 1000, "", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 1, sarama.ErrNoError),
	})

	cfg := testhelpers.NewTestProxyCfg("c1")
	cfg.Consumer.OffsetsCommitInterval = 300 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, nil)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), cfg, client)
	defer f.Stop()
	om, err := f.Spawn(s.ns.NewChild("g1", "t1", 1), "g1", "t1", 1)
	c.Assert(err, IsNil)
	time.Sleep(100 * time.Millisecond)
	// Set broker latency to ensure proper test timing.
	broker1.SetLatency(200 * time.Millisecond)
	<-om.CommittedOffsets() // Ignore initial offset.

	// When
	// 0ms: the first offset is submitted;
	om.SubmitOffset(Offset{1001, "bar1"})
	time.Sleep(400 * time.Millisecond)
	// 300ms: broker executor sends OffsetCommitRequest to Kafka
	// 400ms: the second offset is submitted.
	om.SubmitOffset(Offset{1002, "bar2"})
	om.Stop()
	// 500ms: a first offset commit response received from Kafka. Due to a bug
	// the offset manager was quiting here, dropping the second commit.
	// 700ms: a second offset commit response received from Kafka.

	// Then
	var committedOffsets []Offset
	for committedOffset := range om.CommittedOffsets() {
		committedOffsets = append(committedOffsets, committedOffset)
	}
	c.Assert(committedOffsets, DeepEquals, []Offset{{1001, "bar1"}, {1002, "bar2"}})
}

// lastCommittedOffset traverses the mock broker history backwards searching
// for the OffsetCommitRequest coming from the specified consumer group that
// commits an offset of the specified topic/partition.
func lastCommittedOffset(mb *sarama.MockBroker, group, topic string, partition int32) Offset {
	for i := len(mb.History()) - 1; i >= 0; i-- {
		req, ok := mb.History()[i].Request.(*sarama.OffsetCommitRequest)
		if !ok || req.ConsumerGroup != group {
			continue
		}
		offset, metadata, err := req.Offset(topic, partition)
		if err != nil {
			continue
		}
		return Offset{offset, metadata}
	}
	return Offset{}
}
