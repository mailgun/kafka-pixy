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
	"github.com/mailgun/log"
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
	testhelpers.InitLogging(c)
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)

	// When
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)

	// Then
	fo := <-om.InitialOffset()
	c.Assert(fo, DeepEquals, DecoratedOffset{2000, "bar"})

	om.Stop()
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Retry.Backoff = 50 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)

	// When
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)

	// Then
	oce := <-om.Errors()
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 8, ErrNoCoordinator})

	om.Stop()
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Retry.Backoff = 50 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)

	// When
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// Then
	oce := <-om.Errors()
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 7, sarama.ErrNotLeaderForPartition})

	om.Stop()
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Retry.Backoff = 1000 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)

	f := SpawnFactory(s.ns.NewChild(), client)
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om.SubmitOffset(1000, "foo")
	var wg sync.WaitGroup
	actor.Spawn(actor.RootID.NewChild("stopper"), &wg, om.Stop)

	// Then
	oce := <-om.Errors()
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 7, sarama.ErrNotLeaderForPartition})

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrNoError),
	})

	wg.Wait()
	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, DecoratedOffset{1000, "foo"})
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Net.ReadTimeout = 10 * time.Millisecond
	cfg.Consumer.Retry.Backoff = 25 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)
	c.Assert(err, IsNil)
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When: a partition offset manager is closed while there is a pending commit.
	om.SubmitOffset(1001, "foo")
	go om.Stop()

	// Then: the partition offset manager terminates only after it has
	// successfully committed the offset.

	// STAGE 1: Requests for coordinator time out.
	log.Infof("    STAGE 1")
	oce := <-om.Errors()
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 7, ErrNoCoordinator})

	// STAGE 2: Requests for initial offset return errors
	log.Infof("    STAGE 2")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 0, "", sarama.ErrNotLeaderForPartition),
	})
	for oce = range om.Errors() {
		if !reflect.DeepEqual(oce, &OffsetCommitError{"g1", "t1", 7, ErrNoCoordinator}) {
			break
		}
	}
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 7, sarama.ErrNotLeaderForPartition})

	// STAGE 3: Offset commit requests fail
	log.Infof("    STAGE 3")
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"ConsumerMetadataRequest": sarama.NewMockConsumerMetadataResponse(c).
			SetCoordinator("g1", broker1),
		"OffsetFetchRequest": sarama.NewMockOffsetFetchResponse(c).
			SetOffset("g1", "t1", 7, 1234, "foo", sarama.ErrNoError),
		"OffsetCommitRequest": sarama.NewMockOffsetCommitResponse(c).
			SetError("g1", "t1", 7, sarama.ErrOffsetMetadataTooLarge),
	})
	for oce = range om.Errors() {
		if !reflect.DeepEqual(oce, &OffsetCommitError{"g1", "t1", 7, sarama.ErrNotLeaderForPartition}) {
			break
		}
	}
	c.Assert(oce, DeepEquals, &OffsetCommitError{"g1", "t1", 7, sarama.ErrOffsetMetadataTooLarge})

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
	for oce := range om.Errors() {
		log.Infof("Drain error: %v", oce)
	}

	committedOffset := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset, DeepEquals, DecoratedOffset{1001, "foo"})
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)
	om1, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.SpawnOffsetManager(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om1.SubmitOffset(1009, "foo1")
	om1.SubmitOffset(1010, "foo2")
	om2.SubmitOffset(2010, "bar1")
	om2.SubmitOffset(2011, "bar2")
	om1.SubmitOffset(1017, "foo3")
	om2.SubmitOffset(2019, "bar3")
	om1.Stop()
	om2.Stop()

	// Then
	committedOffset1 := lastCommittedOffset(broker1, "g1", "t1", 7)
	c.Assert(committedOffset1, DeepEquals, DecoratedOffset{1017, "foo3"})
	committedOffset2 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset2, DeepEquals, DecoratedOffset{2019, "bar3"})
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Net.ReadTimeout = 50 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Retry.Backoff = 100 * time.Millisecond
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)
	om1, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)
	om2, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 8), "g1", "t1", 8)
	c.Assert(err, IsNil)
	om3, err := f.SpawnOffsetManager(s.ns.NewChild("g2", "t1", 7), "g2", "t1", 7)
	c.Assert(err, IsNil)
	om1.SubmitOffset(1001, "bar1")
	om2.SubmitOffset(2001, "bar2")
	om3.SubmitOffset(3001, "bar3")

	log.Infof("*** Waiting for errors...")
	<-om1.Errors()
	<-om2.Errors()
	<-om3.Errors()

	// When
	time.Sleep(cfg.Consumer.Retry.Backoff * 2)
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
	c.Assert(committedOffset1, DeepEquals, DecoratedOffset{1001, "bar1"})
	committedOffset2 := lastCommittedOffset(broker1, "g1", "t1", 8)
	c.Assert(committedOffset2, DeepEquals, DecoratedOffset{2001, "bar2"})
	committedOffset3 := lastCommittedOffset(broker1, "g2", "t1", 7)
	c.Assert(committedOffset3, DeepEquals, DecoratedOffset{3001, "bar3"})
	f.Stop()
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

	cfg := sarama.NewConfig()
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	// When
	om.SubmitOffset(1001, "bar1")
	om.SubmitOffset(1002, "bar2")
	om.SubmitOffset(1003, "bar3")
	om.SubmitOffset(1004, "bar4")
	om.SubmitOffset(1005, "bar5")
	om.Stop()

	// Then
	var committedOffsets []DecoratedOffset
	for committedOffset := range om.CommittedOffsets() {
		committedOffsets = append(committedOffsets, committedOffset)
	}
	c.Assert(committedOffsets, DeepEquals, []DecoratedOffset{{1005, "bar5"}})
	f.Stop()
}

// Test a scenario revealed in production https://github.com/mailgun/kafka-pixy/issues/29
// the problem was that if a connection to the broker was broker on the Kafka
// side while a partition manager tried to retrieve an initial commit, the later
// would never try to reestablish connection and get stuck in an infinite loop
// of unassign->assign of the same broker over and over again.
func (s *OffsetMgrSuite) TestConnectionRestored(c *C) {
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

	cfg := sarama.NewConfig()
	cfg.Net.ReadTimeout = 100 * time.Millisecond
	cfg.Consumer.Return.Errors = true
	cfg.Consumer.Retry.Backoff = 100 * time.Millisecond
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	client, err := sarama.NewClient([]string{broker1.Addr()}, cfg)
	c.Assert(err, IsNil)
	f := SpawnFactory(s.ns.NewChild(), client)
	om, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	log.Infof("    GIVEN 1")
	// Make sure the partition offset manager established connection with broker2.
	oce := &OffsetCommitError{}
	select {
	case oce = <-om.Errors():
	case <-time.After(200 * time.Millisecond):
	}
	_, ok := oce.Err.(*net.OpError)
	c.Assert(ok, Equals, true, Commentf("Unexpected or no error: err=%v", oce.Err))

	log.Infof("    GIVEN 2")
	// Close both broker2 and the partition offset manager. That will break
	// client connection with broker2 from the broker end.
	om.Stop()
	broker2.Close()
	time.Sleep(cfg.Consumer.Retry.Backoff * 2)

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
	om, err = f.SpawnOffsetManager(s.ns.NewChild("g1", "t1", 7), "g1", "t1", 7)
	c.Assert(err, IsNil)

	log.Infof("    THEN")
	// Then: the new partition offset manager re-establishes connection with
	// broker2 and successfully retrieves the initial offset.
	var do DecoratedOffset
	select {
	case do = <-om.InitialOffset():
	case oce = <-om.Errors():
	case <-time.After(200 * time.Millisecond):
	}
	c.Assert(do.Offset, Equals, int64(1000), Commentf("Failed to retrieve initial offset: %s", oce.Err))

	om.Stop()
	f.Stop()
}

// lastCommittedOffset traverses the mock broker history backwards searching
// for the OffsetCommitRequest coming from the specified consumer group that
// commits an offset of the specified topic/partition.
func lastCommittedOffset(mb *sarama.MockBroker, group, topic string, partition int32) DecoratedOffset {
	for i := len(mb.History()) - 1; i >= 0; i-- {
		req, ok := mb.History()[i].Request.(*sarama.OffsetCommitRequest)
		if !ok || req.ConsumerGroup != group {
			continue
		}
		offset, metadata, err := req.Offset(topic, partition)
		if err != nil {
			continue
		}
		return DecoratedOffset{offset, metadata}
	}
	return DecoratedOffset{}
}
