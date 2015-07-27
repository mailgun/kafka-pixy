package pixy

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type SmartConsumerSuite struct {
	producer *GracefulProducer
}

var _ = Suite(&SmartConsumerSuite{})

func (s *SmartConsumerSuite) SetUpSuite(c *C) {
	InitTestLog()
	var err error
	config := NewConfig()
	config.ClientID = "producer"
	config.Kafka.SeedPeers = testKafkaPeers
	config.ChannelBufferSize = 1
	s.producer, err = SpawnGracefulProducer(config)
	c.Assert(err, IsNil)
}

func (s *SmartConsumerSuite) TearDownSuite(c *C) {
	s.producer.Stop()
}

func (s *SmartConsumerSuite) TestResolveAssignments(c *C) {
	c.Assert(resolveAssignments(nil, nil), IsNil)
	c.Assert(resolveAssignments(nil, []string{}), IsNil)
	c.Assert(resolveAssignments(nil, []string{"a"}), IsNil)
	c.Assert(resolveAssignments(nil, []string{"a", "b"}), IsNil)
	c.Assert(resolveAssignments([]int32{}, nil), IsNil)
	c.Assert(resolveAssignments([]int32{}, []string{}), IsNil)
	c.Assert(resolveAssignments([]int32{}, []string{"a"}), IsNil)
	c.Assert(resolveAssignments([]int32{}, []string{"a", "b"}), IsNil)
	c.Assert(resolveAssignments([]int32{1}, nil), IsNil)
	c.Assert(resolveAssignments([]int32{1}, []string{}), IsNil)

	c.Assert(resolveAssignments([]int32{0}, []string{"a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing},
		})
	c.Assert(resolveAssignments([]int32{1, 2, 0}, []string{"a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing, 2: nothing},
		})
	c.Assert(resolveAssignments([]int32{0}, []string{"b", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing},
		})
	c.Assert(resolveAssignments([]int32{0, 3, 1, 2}, []string{"b", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing},
			"b": map[int32]none{2: nothing, 3: nothing},
		})
	c.Assert(resolveAssignments([]int32{0, 3, 1, 2}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing},
			"b": map[int32]none{2: nothing},
			"c": map[int32]none{3: nothing},
		})
	c.Assert(resolveAssignments([]int32{0, 3, 1, 2, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing},
			"b": map[int32]none{2: nothing, 3: nothing},
			"c": map[int32]none{4: nothing},
		})
	c.Assert(resolveAssignments([]int32{0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing},
			"b": map[int32]none{2: nothing, 3: nothing},
			"c": map[int32]none{4: nothing, 5: nothing},
		})
	c.Assert(resolveAssignments([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing, 2: nothing},
			"b": map[int32]none{3: nothing, 4: nothing},
			"c": map[int32]none{5: nothing, 6: nothing},
		})
	c.Assert(resolveAssignments([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"d", "b", "c", "a"}),
		DeepEquals, map[string]map[int32]none{
			"a": map[int32]none{0: nothing, 1: nothing},
			"b": map[int32]none{2: nothing, 3: nothing},
			"c": map[int32]none{4: nothing, 5: nothing},
			"d": map[int32]none{6: nothing},
		})
}

// If a topic has the only partition then the consumer retrieve messages in the
// order they were produced.
func (s *SmartConsumerSuite) TestSinglePartitionTopic(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.1")
	produced := GenMessages(c, "single", "test.1", map[string]int{"": 3})

	sc, err := SpawnSmartConsumer(s.newConfig("consumer-1"))
	c.Assert(err, IsNil)

	// When/Then
	consumed := s.consume(c, sc, "group-1", "test.1", 1)
	s.assertMsg(c, consumed[""][0], produced[""][0])

	sc.Stop()
}

// If we stop one consumer and start another, the new one picks up where the
// previous one left off.
func (s *SmartConsumerSuite) TestSequentialConsume(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.1")
	produced := GenMessages(c, "sequencial", "test.1", map[string]int{"": 3})

	config := s.newConfig("consumer-1")
	sc1, err := SpawnSmartConsumer(config)
	c.Assert(err, IsNil)
	log.Infof("*** GIVEN 1")
	consumed := s.consume(c, sc1, "group-1", "test.1", 2)
	s.assertMsg(c, consumed[""][0], produced[""][0])
	s.assertMsg(c, consumed[""][1], produced[""][1])

	// When: one consumer stopped and another one takes its place.
	log.Infof("*** WHEN")
	sc1.Stop()
	sc2, err := SpawnSmartConsumer(config)
	c.Assert(err, IsNil)

	// Then: the second message is consumed.
	log.Infof("*** THEN")
	consumed = s.consume(c, sc2, "group-1", "test.1", 1, consumed)
	s.assertMsg(c, consumed[""][2], produced[""][2])
	sc2.Stop()
}

// If a topic has several partitions then they all are being consumed in random
// order.
func (s *SmartConsumerSuite) TestMultiPartitionTopic(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.4")
	GenMessages(c, "multi", "test.4", map[string]int{"A": 10, "B": 10})

	log.Infof("*** GIVEN 1")
	sc, err := SpawnSmartConsumer(s.newConfig("consumer-1"))
	c.Assert(err, IsNil)

	// When: exactly one half of all produced events is consumed.
	log.Infof("*** WHEN")
	sc.Consume("group-1", "test.4")
	// Allow some time to spin up consumers for all partitions.
	time.Sleep(300 * time.Millisecond)
	consumed := s.consume(c, sc, "group-1", "test.4", 5)

	// Then: we have events from both partitions consumed.
	log.Infof("*** THEN")
	delta := len(consumed["A"]) - len(consumed["B"])
	if delta > 5 || delta < -5 {
		c.Errorf("Consumption disbalance: consumed[A]=%d, consumed[B]=%d", len(consumed["A"]), len(consumed["B"]))
	}

	sc.Stop()
}

// When a new consumer joins a group the partitions get evenly redistributed
// among all consumers.
func (s *SmartConsumerSuite) TestRebalanceOnJoin(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.4")
	GenMessages(c, "join", "test.4", map[string]int{"A": 10, "B": 10})

	sc1, err := SpawnSmartConsumer(s.newConfig("consumer-1"))
	c.Assert(err, IsNil)

	// Consume the first message to make the consumer join the group and
	// subscribe to the topic.
	log.Infof("*** GIVEN 1")
	consumed1 := s.consume(c, sc1, "group-1", "test.4", 1)
	// Allow some time to spin up consumers for all partitions.
	time.Sleep(300 * time.Millisecond)

	// Consume 4 messages and make sure that there messages from both
	// partitions among them.
	log.Infof("*** GIVEN 2")
	consumed1 = s.consume(c, sc1, "group-1", "test.4", 4, consumed1)
	c.Assert(len(consumed1["A"]), Not(Equals), 0)
	c.Assert(len(consumed1["B"]), Not(Equals), 0)
	consumedBeforeJoin := len(consumed1["B"])

	// When: another consumer joins the group rebalancing occurs.
	log.Infof("*** WHEN")
	sc2, err := SpawnSmartConsumer(s.newConfig("consumer-2"))
	c.Assert(err, IsNil)

	// Then:
	log.Infof("*** THEN")
	consumed2 := s.consume(c, sc2, "group-1", "test.4", consumeAll)
	consumed1 = s.consume(c, sc1, "group-1", "test.4", consumeAll, consumed1)
	// Partition "A" has been consumed by `consumer-1` only
	c.Assert(len(consumed1["A"]), Equals, 10)
	c.Assert(len(consumed2["A"]), Equals, 0)
	// Partition "B" has been consumed by both consumers, but ever since
	// `consumer-2` joined the group the first one have not got any new messages.
	c.Assert(len(consumed1["B"]), Equals, consumedBeforeJoin)
	c.Assert(len(consumed2["B"]), Not(Equals), 0)
	c.Assert(len(consumed1["B"])+len(consumed2["B"]), Equals, 10)
	// `consumer-2` started consumer from where `consumer-1` left off.
	c.Assert(consumed2["B"][0].Offset, Equals, consumed1["B"][len(consumed1["B"])-1].Offset+1)

	sc2.Stop()
	sc1.Stop()
}

// When a consumer leaves a group the partitions get evenly redistributed
// among the remaining consumers.
func (s *SmartConsumerSuite) TestRebalanceOnLeave(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.4")
	produced := GenMessages(c, "leave", "test.4", map[string]int{"A": 10, "B": 10, "C": 10})

	var consumers []*SmartConsumer
	for i := 0; i < 3; i++ {
		sc, err := SpawnSmartConsumer(s.newConfig(fmt.Sprintf("consumer-%d", i)))
		c.Assert(err, IsNil)
		consumers = append(consumers, sc)
	}
	log.Infof("*** GIVEN 1")
	// Consume the first message to make the consumer join the group and
	// subscribe to the topic.
	consumed := make([]map[string][]*sarama.ConsumerMessage, 3)
	for i := 0; i < 3; i++ {
		consumed[i] = s.consume(c, consumers[i], "group-1", "test.4", 1)
	}
	// consumer[0] can consume the first message from all partitions and
	// consumer[1] can consume the first message from either `B` or `C`.
	log.Infof("*** GIVEN 2")
	if len(consumed[0]["A"]) == 1 {
		if len(consumed[1]["B"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][1])
		} else { // if len(consumed[1]["C"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][0])
		}
	} else if len(consumed[0]["B"]) == 1 {
		if len(consumed[1]["B"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][2])
		} else { // if len(consumed[1]["C"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][1])
		}
	} else { // if len(consumed[0]["C"]) == 1 {
		if len(consumed[1]["B"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][1])
		} else { // if len(consumed[1]["C"]) == 1 {
			s.assertMsg(c, consumed[2]["B"][0], produced["B"][0])
		}
	}
	s.consume(c, consumers[2], "group-1", "test.4", 4, consumed[2])
	c.Assert(len(consumed[2]["B"]), Equals, 5)
	lastConsumedFromBby2 := consumed[2]["B"][4]

	// When
	log.Infof("*** WHEN")
	consumers[2].Stop()
	// Allow some time to rebalance partitions
	time.Sleep(300 * time.Millisecond)

	// Then: partition `B` is reassigned to `consumer[1]` and it picks up where
	// `consumer[2]` left off.
	log.Infof("*** THEN")
	consumedSoFar := make(map[string]int)
	for _, consumedByOne := range consumed {
		for key, consumedWithKey := range consumedByOne {
			consumedSoFar[key] = consumedSoFar[key] + len(consumedWithKey)
		}
	}
	leftToBeConsumedBy1 := 20 - (consumedSoFar["B"] + consumedSoFar["C"])
	consumedBy1 := s.consume(c, consumers[1], "group-1", "test.4", leftToBeConsumedBy1)
	c.Assert(len(consumedBy1["B"]), Equals, 10-consumedSoFar["B"])
	c.Assert(consumedBy1["B"][0].Offset, Equals, lastConsumedFromBby2.Offset+1)

	consumers[0].Stop()
	consumers[1].Stop()
}

// When a consumer registration times out the partitions that used to be
// assigned to it are redistributed among active consumers.
func (s *SmartConsumerSuite) TestRebalanceOnTimeout(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.4")
	GenMessages(c, "join", "test.4", map[string]int{"A": 10, "B": 10})

	sc1, err := SpawnSmartConsumer(s.newConfig("consumer-1"))
	c.Assert(err, IsNil)

	config2 := s.newConfig("consumer-2")
	config2.Consumer.RegistrationTimeout = 300 * time.Millisecond
	sc2, err := SpawnSmartConsumer(config2)
	c.Assert(err, IsNil)

	// Consume the first message to make the consumers join the group and
	// subscribe to the topic.
	log.Infof("*** GIVEN 1")
	consumed1 := s.consume(c, sc1, "group-1", "test.4", 1)
	consumed2 := s.consume(c, sc2, "group-1", "test.4", 1)
	if len(consumed1["B"]) == 0 {
		c.Assert(len(consumed1["A"]), Equals, 1)
	} else {
		c.Assert(len(consumed1["A"]), Equals, 0)
	}
	c.Assert(len(consumed2["A"]), Equals, 0)
	c.Assert(len(consumed2["B"]), Equals, 1)

	// Consume 4 more messages to make sure that each consumer pulls from a
	// particular assigned to it.
	log.Infof("*** GIVEN 2")
	consumed1 = s.consume(c, sc1, "group-1", "test.4", 4, consumed1)
	consumed2 = s.consume(c, sc2, "group-1", "test.4", 4, consumed2)
	if len(consumed1["B"]) == 1 {
		c.Assert(len(consumed1["A"]), Equals, 4)
	} else {
		c.Assert(len(consumed1["A"]), Equals, 5)
	}
	c.Assert(len(consumed2["A"]), Equals, 0)
	c.Assert(len(consumed2["B"]), Equals, 5)

	// When: `consumer-2` registration timeout elapses, the partitions get
	// rebalanced so that `consumer-1` becomes assigned to all of them...
	log.Infof("*** WHEN")
	time.Sleep(500 * time.Millisecond)

	// ...and consumes the remaining messages from all partitions.
	log.Infof("*** THEN")
	consumed1 = s.consume(c, sc1, "group-1", "test.4", 10, consumed1)
	c.Assert(len(consumed1["A"]), Equals, 10)
	c.Assert(len(consumed1["B"]), Equals, 5)
	c.Assert(len(consumed2["A"]), Equals, 0)
	c.Assert(len(consumed2["B"]), Equals, 5)

	sc2.Stop()
	sc1.Stop()
}

// A `ErrConsumerBufferOverflow` error can be returned if internal buffers are
// filled with in-flight consume requests.
func (s *SmartConsumerSuite) TestBufferOverflowError(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.1")
	GenMessages(c, "join", "test.1", map[string]int{"A": 30})

	config := s.newConfig("consumer-1")
	config.ChannelBufferSize = 1
	sc, err := SpawnSmartConsumer(config)
	c.Assert(err, IsNil)

	// When
	var overflowErrorCount int32
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		spawn(&wg, func() {
			for i := 0; i < 10; i++ {
				_, err := sc.Consume("group-1", "test.1")
				if _, ok := err.(ErrConsumerBufferOverflow); ok {
					atomic.AddInt32(&overflowErrorCount, 1)
				}
			}
		})
	}
	wg.Wait()

	// Then
	c.Assert(overflowErrorCount, Not(Equals), 0)
	log.Infof("*** overflow was hit %d times", overflowErrorCount)

	sc.Stop()
}

// This test makes an attempt to exercise the code path where a message is
// received when a down stream dispatch tear is being stopped due to
// registration timeout, in that case a successor tear is created that will be
// started as soon as the original one is completely shutdown.
//
// It is impossible to see from the service behavior if the expected code path
// has been exercised by the test. The only way to check that is through the
// code coverage reports.
func (s *SmartConsumerSuite) TestRequestDuringTimeout(c *C) {
	// Given
	ResetOffsets(c, "group-1", "test.4")
	GenMessages(c, "join", "test.4", map[string]int{"A": 30})

	config := s.newConfig("consumer-1")
	config.Consumer.RegistrationTimeout = 200 * time.Millisecond
	config.ChannelBufferSize = 1
	sc, err := SpawnSmartConsumer(config)
	c.Assert(err, IsNil)

	// When/Then
	for i := 0; i < 10; i++ {
		for j := 0; j < 3; j++ {
			begin := time.Now()
			log.Infof("*** consuming...")
			consMsg, err := sc.Consume("group-1", "test.4")
			c.Assert(err, IsNil)
			log.Infof("*** consumed: in=%s, by=%s, topic=%s, partition=%d, offset=%d, message=%s",
				time.Now().Sub(begin), sc.baseCID.String(), consMsg.Topic, consMsg.Partition, consMsg.Offset, consMsg.Value)
		}
		time.Sleep(200 * time.Millisecond)
	}

	sc.Stop()
}

// If an attempt is made to consume from a topic that does not exist then the
// request times out after `Config.Consumer.LongPollingTimeout`.
func (s *SmartConsumerSuite) TestInvalidTopic(c *C) {
	// Given
	config := s.newConfig("consumer-1")
	config.Consumer.LongPollingTimeout = 1 * time.Second
	sc, err := SpawnSmartConsumer(config)
	c.Assert(err, IsNil)

	// When
	consMsg, err := sc.Consume("group-1", "no-such-topic")

	// Then
	if _, ok := err.(ErrConsumerRequestTimeout); !ok {
		c.Errorf("ErrConsumerRequestTimeout is expected")
	}
	c.Assert(consMsg, IsNil)

	sc.Stop()
}

func (s *SmartConsumerSuite) newConfig(clientID string) *Config {
	config := NewConfig()
	config.ClientID = clientID
	config.Kafka.SeedPeers = testKafkaPeers
	config.ZooKeeper.SeedPeers = testZookeeperPeers
	config.Consumer.LongPollingTimeout = 3000 * time.Millisecond
	config.Consumer.BackOffTimeout = 100 * time.Millisecond
	config.Consumer.RebalanceDelay = 100 * time.Millisecond
	return config
}

func (s *SmartConsumerSuite) assertMsg(c *C, consMsg *sarama.ConsumerMessage, prodMsg *sarama.ProducerMessage) {
	c.Assert(sarama.StringEncoder(consMsg.Value), Equals, prodMsg.Value)
	c.Assert(consMsg.Offset, Equals, prodMsg.Offset)
}

func (s *SmartConsumerSuite) compareMsg(consMsg *sarama.ConsumerMessage, prodMsg *sarama.ProducerMessage) bool {
	return sarama.StringEncoder(consMsg.Value) == prodMsg.Value.(sarama.Encoder) && consMsg.Offset == prodMsg.Offset
}

const consumeAll = -1

func (s *SmartConsumerSuite) consume(c *C, sc *SmartConsumer, group, topic string, count int,
	extend ...map[string][]*sarama.ConsumerMessage) map[string][]*sarama.ConsumerMessage {

	var consumed map[string][]*sarama.ConsumerMessage
	if len(extend) == 0 {
		consumed = make(map[string][]*sarama.ConsumerMessage)
	} else {
		consumed = extend[0]
	}
	for i := 0; i != count; i++ {
		consMsg, err := sc.Consume(group, topic)
		if _, ok := err.(ErrConsumerRequestTimeout); ok {
			if count == consumeAll {
				return consumed
			}
			c.Fatalf("Not enough messages consumed: expected=%d, actual=%d", count, i)
		}
		c.Assert(err, IsNil)
		consumed[string(consMsg.Key)] = append(consumed[string(consMsg.Key)], consMsg)
		logConsumed(sc, consMsg)
	}
	return consumed
}

func logConsumed(sc *SmartConsumer, consMsg *sarama.ConsumerMessage) {
	log.Infof("*** consumed: by=%s, topic=%s, partition=%d, offset=%d, message=%s",
		sc.baseCID.String(), consMsg.Topic, consMsg.Partition, consMsg.Offset, consMsg.Value)
}
