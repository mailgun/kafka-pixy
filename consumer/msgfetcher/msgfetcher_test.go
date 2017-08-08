package msgfetcher

import (
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/testhelpers"
	log "github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type MsgFetcherSuite struct {
	ns      *actor.ID
	cfg     *config.Proxy
	broker0 *sarama.MockBroker
}

var (
	_       = Suite(&MsgFetcherSuite{})
	testMsg = sarama.StringEncoder("Foo")
)

func (s *MsgFetcherSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *MsgFetcherSuite) SetUpTest(c *C) {
	testReportErrors = true
	s.ns = actor.RootID.NewChild("T")
	s.cfg = testhelpers.NewTestProxyCfg("mis")
	s.cfg.Kafka.Version.Set(sarama.V0_8_2_2)
	s.broker0 = sarama.NewMockBroker(c, 0)
}

func (s *MsgFetcherSuite) TearDownTest(c *C) {
	s.broker0.Close()
}

// If a particular offset is provided then messages are consumed starting from
// that offset.
func (s *MsgFetcherSuite) TestOffsetManual(c *C) {
	mockFetchResponse := sarama.NewMockFetchResponse(c, 1)
	for i := 0; i < 10; i++ {
		mockFetchResponse.SetMessage("my_topic", 0, int64(i+1234), testMsg)
	}

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 2345),
		"FetchRequest": mockFetchResponse,
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	// When
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf, concreteOffset, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 1234)
	defer mf.Stop()
	c.Assert(err, IsNil)
	c.Assert(concreteOffset, Equals, int64(1234))

	// Then: messages starting from offset 1234 are consumed.
	for i := 0; i < 10; i++ {
		select {
		case message := <-mf.Messages():
			c.Assert(message.Offset, Equals, int64(i+1234))
		case err := <-mf.(*msgFetcher).errorsCh:
			c.Error(err)
		}
	}
}

// If `sarama.OffsetNewest` is passed as the initial offset then the first consumed
// message is indeed corresponds to the offset that broker claims to be the
// newest in its metadata response.
func (s *MsgFetcherSuite) TestOffsetNewest(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 10).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 9, testMsg).
			SetMessage("my_topic", 0, 10, testMsg).
			SetMessage("my_topic", 0, 11, testMsg).
			SetHighWaterMark("my_topic", 0, 14),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When
	mf, concreteOffset, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetNewest)
	c.Assert(err, IsNil)
	defer mf.Stop()
	c.Assert(concreteOffset, Equals, int64(10))

	// Then
	msg := <-mf.Messages()
	c.Assert(msg.Offset, Equals, int64(10))
	c.Assert(msg.HighWaterMark, Equals, int64(14))
}

// It is possible to close a partition consumer and create the same anew.
func (s *MsgFetcherSuite) TestRecreate(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 10, testMsg),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 10)
	c.Assert(err, IsNil)
	c.Assert((<-mf.Messages()).Offset, Equals, int64(10))

	// When
	mf.Stop()
	mf, _, err = f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 10)
	c.Assert(err, IsNil)
	defer mf.Stop()

	// Then
	c.Assert((<-mf.Messages()).Offset, Equals, int64(10))
}

// An attempt to consume the same partition twice should fail.
func (s *MsgFetcherSuite) TestDuplicate(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	s.cfg.Consumer.ChannelBufferSize = 0
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf1, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)
	c.Assert(err, IsNil)
	defer mf1.Stop()

	// When
	mf2, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)

	// Then
	if mf2 != nil || err != sarama.ConfigurationError("That topic/partition is already being consumed") {
		c.Error("A partition cannot be consumed twice at the same time")
	}
}

// If consumer fails to refresh metadata it keeps retrying with frequency
// specified by `Config.Consumer.Retry.Backoff`.
func (s *MsgFetcherSuite) TestLeaderRefreshError(c *C) {
	// Stage 1: my_topic/0 served by broker0
	log.Infof("    STAGE 1")

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 123).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 123, testMsg),
	})

	saramaCfg := s.cfg.SaramaClientCfg()
	saramaCfg.Net.ReadTimeout = 100 * time.Millisecond
	saramaCfg.Metadata.Retry.Max = 0
	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, saramaCfg)
	defer kafkaClt.Close()

	s.cfg.Consumer.RetryBackoff = 200 * time.Millisecond
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetOldest)
	c.Assert(err, IsNil)
	defer mf.Stop()
	c.Assert((<-mf.Messages()).Offset, Equals, int64(123))

	// Stage 2: broker0 says that it is no longer the leader for my_topic/0,
	// but the requests to retrieve metadata fail with network timeout.
	log.Infof("    STAGE 2")

	fetchResponse2 := &sarama.FetchResponse{}
	fetchResponse2.AddError("my_topic", 0, sarama.ErrNotLeaderForPartition)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockWrapper(fetchResponse2),
	})

	for {
		err := <-mf.(*msgFetcher).errorsCh
		if err == errIncompleteResponse {
			continue
		}
		if err == sarama.ErrNotLeaderForPartition {
			break
		}
		c.Errorf("Unexpected error: %v", err)
	}

	// Stage 3: finally the metadata returned by broker0 tells that broker1 is
	// a new leader for my_topic/0. Consumption resumes.

	log.Infof("    STAGE 3")

	broker1 := sarama.NewMockBroker(c, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 124, testMsg),
	})
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetBroker(broker1.Addr(), broker1.BrokerID()).
			SetLeader("my_topic", 0, broker1.BrokerID()),
	})

	c.Assert((<-mf.Messages()).Offset, Equals, int64(124))
}

// If a partition reader terminates due to a fatal error, another instance
// of a partition reader for the same partition can be started later.
func (s *MsgFetcherSuite) TestFatalErrorStop(c *C) {
	// Stage 1: my_topic/0 served by broker0, but the partition consumer
	// immediately fails due to fatal error and terminates.
	log.Infof("    STAGE 1")

	fatalErrorRes := &sarama.FetchResponse{}
	fatalErrorRes.AddError("my_topic", 0, sarama.ErrOffsetOutOfRange)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 123).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockWrapper(fatalErrorRes),
	})

	saramaCfg := s.cfg.SaramaClientCfg()
	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, saramaCfg)
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetOldest)
	c.Assert(err, IsNil)

	// Wait for the partition reader to terminate due to fatal error
	select {
	case _, ok := <-mf.Messages():
		if ok {
			c.Error("Message channel should be closed signaling partition consumer termination")
			c.FailNow()
		}
	case <-time.After(1 * time.Second):
		c.Error("Partition reader should terminate due to fatal error")
		c.FailNow()
	}

	// Stage 2: a partition reader can be recreated.
	log.Infof("    STAGE 2")

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 123).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 123, testMsg),
	})

	mf, _, err = f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetOldest)
	c.Assert(err, IsNil)
	defer mf.Stop()

	c.Assert((<-mf.Messages()).Offset, Equals, int64(123))
}

func (s *MsgFetcherSuite) TestInvalidTopic(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When
	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetOldest)

	// Then
	if mf != nil || err != sarama.ErrUnknownTopicOrPartition {
		c.Errorf("Should fail with, err=%v", err)
	}
}

// Nothing bad happens if a partition consumer that has no leader assigned at
// the moment is closed.
func (s *MsgFetcherSuite) TestClosePartitionWithoutLeader(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 123).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1000),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 123, testMsg),
	})

	saramaCfg := s.cfg.SaramaClientCfg()
	saramaCfg.Net.ReadTimeout = 100 * time.Millisecond
	saramaCfg.Metadata.Retry.Max = 0
	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, saramaCfg)
	defer kafkaClt.Close()

	s.cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, sarama.OffsetOldest)
	c.Assert(err, IsNil)
	defer mf.Stop()
	c.Assert((<-mf.Messages()).Offset, Equals, int64(123))

	// broker0 says that it is no longer the leader for my_topic/0, but the
	// requests to retrieve metadata fail with network timeout.
	fetchResponse2 := &sarama.FetchResponse{}
	fetchResponse2.AddError("my_topic", 0, sarama.ErrNotLeaderForPartition)

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockWrapper(fetchResponse2),
	})

	// When
	if err := <-mf.(*msgFetcher).errorsCh; err != sarama.ErrNotLeaderForPartition {
		c.Errorf("Unexpected error: %v", err)
	}

	// Then: the partition consumer can be stopped without any problem.
}

// If the initial offset passed on partition consumer creation is out of the
// actual offset range for the partition, then the partition consumer stops
// immediately closing its output channels.
func (s *MsgFetcherSuite) TestShutsDownOutOfRange(c *C) {
	fetchResponse := new(sarama.FetchResponse)
	fetchResponse.AddError("my_topic", 0, sarama.ErrOffsetOutOfRange)
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 7),
		"FetchRequest": sarama.NewMockWrapper(fetchResponse),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When
	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 101)
	c.Assert(err, IsNil)
	defer mf.Stop()

	// Then: consumer should shut down closing its messages and errors channels.
	if _, ok := <-mf.Messages(); ok {
		c.Error("Expected the consumer to shut down")
	}
}

// If a fetch response contains messages with offsets that are smaller then
// requested, then such messages are ignored.
func (s *MsgFetcherSuite) TestExtraOffsets(c *C) {
	fetchResponse1 := &sarama.FetchResponse{}
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 1)
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 2)
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 3)
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 4)
	fetchResponse2 := &sarama.FetchResponse{}
	fetchResponse2.AddError("my_topic", 0, sarama.ErrNoError)
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockSequence(fetchResponse1, fetchResponse2),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When
	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 3)
	c.Assert(err, IsNil)
	defer mf.Stop()

	// Then: messages with offsets 1 and 2 are not returned even though they
	// are present in the response.
	c.Assert((<-mf.Messages()).Offset, Equals, int64(3))
	c.Assert((<-mf.Messages()).Offset, Equals, int64(4))
}

// It is fine if offsets of fetched messages are not sequential (although
// strictly increasing!).
func (s *MsgFetcherSuite) TestNonSequentialOffsets(c *C) {
	fetchResponse1 := &sarama.FetchResponse{}
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 5)
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 7)
	fetchResponse1.AddMessage("my_topic", 0, nil, testMsg, 11)
	fetchResponse2 := &sarama.FetchResponse{}
	fetchResponse2.AddError("my_topic", 0, sarama.ErrNoError)
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1234).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 0),
		"FetchRequest": sarama.NewMockSequence(fetchResponse1, fetchResponse2),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When
	mf, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 3)
	c.Assert(err, IsNil)
	defer mf.Stop()

	// Then: messages with offsets 1 and 2 are not returned even though they
	// are present in the response.
	c.Assert((<-mf.Messages()).Offset, Equals, int64(5))
	c.Assert((<-mf.Messages()).Offset, Equals, int64(7))
	c.Assert((<-mf.Messages()).Offset, Equals, int64(11))
}

// If leadership for a partition is changing then consumer resolves the new
// leader and switches to it.
func (s *MsgFetcherSuite) TestRebalancingMultiplePartitions(c *C) {
	// initial setup
	leader0 := sarama.NewMockBroker(c, 0)
	defer leader0.Close()
	leader1 := sarama.NewMockBroker(c, 1)
	defer leader1.Close()

	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(leader0.Addr(), leader0.BrokerID()).
			SetBroker(leader1.Addr(), leader1.BrokerID()).
			SetLeader("my_topic", 0, leader0.BrokerID()).
			SetLeader("my_topic", 1, leader1.BrokerID()),
	})

	mockOffsetResponse1 := sarama.NewMockOffsetResponse(c).
		SetOffset("my_topic", 0, sarama.OffsetOldest, 0).
		SetOffset("my_topic", 0, sarama.OffsetNewest, 1000).
		SetOffset("my_topic", 1, sarama.OffsetOldest, 0).
		SetOffset("my_topic", 1, sarama.OffsetNewest, 1000)
	leader0.SetHandlerByMap(map[string]sarama.MockResponse{
		"OffsetRequest": mockOffsetResponse1,
		"FetchRequest":  sarama.NewMockFetchResponse(c, 1),
	})
	leader1.SetHandlerByMap(map[string]sarama.MockResponse{
		"OffsetRequest": mockOffsetResponse1,
		"FetchRequest":  sarama.NewMockFetchResponse(c, 1),
	})

	// launch test goroutines
	testReportErrors = false
	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	s.cfg.Consumer.RetryBackoff = 50 * time.Millisecond
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// we expect to end up (eventually) consuming exactly ten messages on each partition
	var wg sync.WaitGroup
	consumed := make([]chan consumer.Message, 2)
	for i := range consumed {
		consumed[i] = make(chan consumer.Message, 10)
		mf, _, err := f.Spawn(s.ns.NewChild("my_topic", i), "my_topic", int32(i), 0)
		c.Assert(err, IsNil)

		wg.Add(1)
		go func(partition int32, mf T) {
			defer wg.Done()
			defer mf.Stop()
			for i := 0; i < cap(consumed[partition]); i++ {
				msg := <-mf.Messages()
				consumed[partition] <- msg
				log.Infof("*** consumed: partition=%d, msg=%v", partition, msg)
				c.Assert(msg.Offset, Equals, int64(i), Commentf("Incorrect message offset!", i, partition, msg.Offset))
				c.Assert(msg.Partition, Equals, partition, Commentf("Incorrect message partition!"))
			}
		}(int32(i), mf)
	}

	time.Sleep(100 * time.Millisecond)
	log.Infof("    STAGE 1")
	// Stage 1:
	//   * my_topic/0 -> leader0 serves 4 messages
	//   * my_topic/1 -> leader1 serves 0 messages

	mockFetchResponse := sarama.NewMockFetchResponse(c, 1)
	for i := 0; i < 4; i++ {
		mockFetchResponse.SetMessage("my_topic", 0, int64(i), testMsg)
	}
	leader0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": mockFetchResponse,
	})

	wait4msg(c, consumed[0], 4, 500*time.Millisecond)

	log.Infof("    STAGE 2")
	// Stage 2:
	//   * leader0 says that it is no longer serving my_topic/0
	//   * s.broker0 tells that leader1 is serving my_topic/0 now

	// seed broker tells that the new partition 0 leader is leader1
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetLeader("my_topic", 0, leader1.BrokerID()).
			SetLeader("my_topic", 1, leader1.BrokerID()),
	})

	// leader0 says no longer leader of partition 0
	fetchResponse := new(sarama.FetchResponse)
	fetchResponse.AddError("my_topic", 0, sarama.ErrNotLeaderForPartition)
	leader0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockWrapper(fetchResponse),
	})

	time.Sleep(100 * time.Millisecond)
	log.Infof("    STAGE 3")
	// Stage 3:
	//   * my_topic/0 -> leader1 serves 6 messages
	//   * my_topic/1 -> leader1 server 8 messages

	// leader1 provides 3 message on partition 0, and 8 messages on partition 1
	mockFetchResponse2 := sarama.NewMockFetchResponse(c, 2)
	for i := 4; i < 10; i++ {
		mockFetchResponse2.SetMessage("my_topic", 0, int64(i), testMsg)
	}
	for i := 0; i < 8; i++ {
		mockFetchResponse2.SetMessage("my_topic", 1, int64(i), testMsg)
	}
	leader1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": mockFetchResponse2,
	})

	wait4msg(c, consumed[0], 6, 500*time.Millisecond)
	wait4msg(c, consumed[1], 8, 500*time.Millisecond)

	log.Infof("    STAGE 4")
	// Stage 4:
	//   * my_topic/1 -> leader1 tells that it is no longer the leader
	//   * s.broker0 tells that leader0 is a new leader for my_topic/1

	// metadata assigns 0 to leader1 and 1 to leader0
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetLeader("my_topic", 0, leader1.BrokerID()).
			SetLeader("my_topic", 1, leader0.BrokerID()),
	})

	// leader1 says no longer leader of partition1
	fetchResponse4 := new(sarama.FetchResponse)
	fetchResponse4.AddError("my_topic", 1, sarama.ErrNotLeaderForPartition)
	leader1.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": sarama.NewMockWrapper(fetchResponse4),
	})

	// leader0 provides two messages on partition 1
	mockFetchResponse4 := sarama.NewMockFetchResponse(c, 2)
	for i := 8; i < 10; i++ {
		mockFetchResponse4.SetMessage("my_topic", 1, int64(i), testMsg)
	}
	leader0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": mockFetchResponse4,
	})

	wait4msg(c, consumed[1], 2, 500*time.Millisecond)
	wg.Wait()
}

// When two partitions have the same broker as the leader, if one partition
// consumer channel buffer is full then that does not affect the ability to
// read messages by the other consumer.
func (s *MsgFetcherSuite) TestInterleavedClose(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()).
			SetLeader("my_topic", 1, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 1000).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 1100).
			SetOffset("my_topic", 1, sarama.OffsetOldest, 2000).
			SetOffset("my_topic", 1, sarama.OffsetNewest, 2100),
		"FetchRequest": sarama.NewMockFetchResponse(c, 1).
			SetMessage("my_topic", 0, 1000, testMsg).
			SetMessage("my_topic", 0, 1001, testMsg).
			SetMessage("my_topic", 0, 1002, testMsg).
			SetMessage("my_topic", 1, 2000, testMsg),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	s.cfg.Consumer.ChannelBufferSize = 0
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	pc0, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 1000)
	c.Assert(err, IsNil)
	defer pc0.Stop()

	mf1, _, err := f.Spawn(s.ns.NewChild("my_topic", 1), "my_topic", 1, 2000)
	c.Assert(err, IsNil)
	defer mf1.Stop()

	// When/Then: we can read from partition 0 even if nobody reads from partition 1
	c.Assert((<-pc0.Messages()).Offset, Equals, int64(1000))
	c.Assert((<-pc0.Messages()).Offset, Equals, int64(1001))
	c.Assert((<-pc0.Messages()).Offset, Equals, int64(1002))
}

func (s *MsgFetcherSuite) TestBounceWithReferenceOpen(c *C) {
	broker0 := sarama.NewMockBroker(c, 0)
	broker0Addr := broker0.Addr()
	broker1 := sarama.NewMockBroker(c, 1)
	defer broker1.Close()

	mockMetadataResponse := sarama.NewMockMetadataResponse(c).
		SetBroker(broker0.Addr(), broker0.BrokerID()).
		SetBroker(broker1.Addr(), broker1.BrokerID()).
		SetLeader("my_topic", 0, broker0.BrokerID()).
		SetLeader("my_topic", 1, broker1.BrokerID())

	mockOffsetResponse := sarama.NewMockOffsetResponse(c).
		SetOffset("my_topic", 0, sarama.OffsetOldest, 1000).
		SetOffset("my_topic", 0, sarama.OffsetNewest, 1100).
		SetOffset("my_topic", 1, sarama.OffsetOldest, 2000).
		SetOffset("my_topic", 1, sarama.OffsetNewest, 2100)

	mockFetchResponse := sarama.NewMockFetchResponse(c, 1)
	for i := 0; i < 10; i++ {
		mockFetchResponse.SetMessage("my_topic", 0, int64(1000+i), testMsg)
		mockFetchResponse.SetMessage("my_topic", 1, int64(2000+i), testMsg)
	}

	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"OffsetRequest": mockOffsetResponse,
		"FetchRequest":  mockFetchResponse,
	})
	broker1.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": mockMetadataResponse,
		"OffsetRequest":   mockOffsetResponse,
		"FetchRequest":    mockFetchResponse,
	})

	kafkaClt, _ := sarama.NewClient([]string{broker1.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	s.cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	s.cfg.Consumer.ChannelBufferSize = 1
	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	pc0, _, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 1000)
	c.Assert(err, IsNil)
	defer pc0.Stop()

	mf1, _, err := f.Spawn(s.ns.NewChild("my_topic", 1), "my_topic", 1, 2000)
	c.Assert(err, IsNil)
	defer mf1.Stop()

	// read messages from both partition to make sure that both brokers operate
	// normally.
	c.Assert((<-pc0.Messages()).Offset, Equals, int64(1000))
	c.Assert((<-mf1.Messages()).Offset, Equals, int64(2000))

	// Simulate broker shutdown. Note that metadata response does not change,
	// that is the leadership does not move to another broker. So partition
	// consumer will keep retrying to restore the connection with the broker.
	broker0.Close()

	// Make sure that while the partition/0 leader is down, consumer/partition/1
	// is capable of pulling messages from broker1.
	for i := 1; i < 7; i++ {
		c.Assert((<-mf1.Messages()).Offset, Equals, int64(2000+i))
	}

	// Bring broker0 back to service.
	broker0 = sarama.NewMockBrokerAddr(c, 0, broker0Addr)
	defer broker0.Close()
	broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"FetchRequest": mockFetchResponse,
	})

	// Read the rest of messages from both partitions.
	for i := 7; i < 10; i++ {
		c.Assert((<-mf1.Messages()).Offset, Equals, int64(2000+i))
	}
	for i := 1; i < 10; i++ {
		c.Assert((<-pc0.Messages()).Offset, Equals, int64(1000+i))
	}

	select {
	case <-pc0.(*msgFetcher).errorsCh:
	default:
		c.Errorf("Partition consumer should have detected broker restart")
	}
}

func (s *MsgFetcherSuite) TestOffsetOutOfRange(c *C) {
	s.broker0.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest": sarama.NewMockMetadataResponse(c).
			SetBroker(s.broker0.Addr(), s.broker0.BrokerID()).
			SetLeader("my_topic", 0, s.broker0.BrokerID()),
		"OffsetRequest": sarama.NewMockOffsetResponse(c).
			SetOffset("my_topic", 0, sarama.OffsetNewest, 2000).
			SetOffset("my_topic", 0, sarama.OffsetOldest, 1000),
	})

	kafkaClt, _ := sarama.NewClient([]string{s.broker0.Addr()}, s.cfg.SaramaClientCfg())
	defer kafkaClt.Close()

	f, err := SpawnFactory(s.ns, s.cfg, kafkaClt)
	c.Assert(err, IsNil)
	defer f.Stop()

	// When/Then
	mf, offset, err := f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 0)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(1000))
	mf.Stop()

	mf, offset, err = f.Spawn(s.ns.NewChild("my_topic", 0), "my_topic", 0, 3456)
	c.Assert(err, IsNil)
	c.Assert(offset, Equals, int64(2000))
	mf.Stop()
}

func wait4msg(c *C, ch <-chan consumer.Message, want int, timeout time.Duration) {
	for i := 0; i < want; i++ {
		select {
		case <-ch:
		case <-time.After(timeout):
			c.Errorf("Timeout waiting for messages: want=%d, got=%d", want, i)
			return
		}
	}
}
