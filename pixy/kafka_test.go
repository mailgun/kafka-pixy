package pixy

import (
	"fmt"
	"strconv"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type KafkaSuite struct {
	cfg       *KafkaClientCfg
	tkc       *TestKafkaClient
	handOffCh chan *producerResult
}

var _ = Suite(&KafkaSuite{})

func (s *KafkaSuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *KafkaSuite) SetUpTest(c *C) {
	s.handOffCh = make(chan *producerResult, 100)
	s.cfg = NewKafkaClientCfg()
	s.cfg.BrokerAddrs = []string{testBroker}
	s.cfg.HandOffCh = s.handOffCh
	s.tkc = NewTestKafkaClient(s.cfg.BrokerAddrs)
}

func (s *KafkaSuite) TearDownTest(c *C) {
	close(s.handOffCh)
	s.tkc.Close()
}

// A started client can be stopped.
func (s *KafkaSuite) TestStartAndStop(c *C) {
	// Given
	kafkaClient, err := SpawnKafkaClient(s.cfg)
	c.Assert(err, IsNil)
	c.Assert(kafkaClient, NotNil)
	// When
	kafkaClient.Stop()
	// Then
	kafkaClient.Wait4Stop()
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *KafkaSuite) TestProduce(c *C) {
	// Given
	kafkaClient, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("kafka-clt-test")
	// When
	for i := 0; i < 100; i++ {
		kafkaClient.Produce("kafka-clt-test", []byte("1"), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < 100; i++ {
		kafkaClient.Produce("kafka-clt-test", []byte("2"), []byte(strconv.Itoa(i)))
	}
	for i := 0; i < 100; i++ {
		kafkaClient.Produce("kafka-clt-test", []byte("3"), []byte(strconv.Itoa(i)))
	}
	kafkaClient.Stop()
	kafkaClient.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("kafka-clt-test")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+200)
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+100)
}

// If `key` of a produced message is `nil` then it is submitted to a random
// partition. Therefore a batch of such messages is evenly distributed among
// all available partitions.
func (s *KafkaSuite) TestProduceNilKey(c *C) {
	// Given
	kafkaClient, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("kafka-clt-test")
	// When
	for i := 0; i < 100; i++ {
		kafkaClient.Produce("kafka-clt-test", nil, []byte(strconv.Itoa(i)))
	}
	kafkaClient.Stop()
	kafkaClient.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("kafka-clt-test")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	delta0 := offsetsAfter[0] - offsetsBefore[0]
	delta1 := offsetsAfter[1] - offsetsBefore[1]
	if delta0 == 0 || delta1 == 0 {
		panic(fmt.Errorf("Too high imbalance: %v != %v", delta0, delta1))
	}
}

// Even though wrapped `sarama.Producer` is instructed to stop immediately on
// client stop due to `ShutdownTimeout == 0`, still none of messages is lost.
// because none of them are retries. This test is mostly to increase coverage.
func (s *KafkaSuite) TestTooSmallShutdownTimeout(c *C) {
	// Given
	s.cfg.ShutdownTimeout = 0
	kafkaClient, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("kafka-clt-test")
	// When
	for i := 0; i < 100; i++ {
		v := []byte(strconv.Itoa(i))
		kafkaClient.Produce("kafka-clt-test", v, v)
	}
	kafkaClient.Stop()
	kafkaClient.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("kafka-clt-test")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	delta0 := offsetsAfter[0] - offsetsBefore[0]
	delta1 := offsetsAfter[1] - offsetsBefore[1]
	c.Assert(delta0+delta1, Equals, int64(100))
}

// If `key` of a produced message is empty then it is deterministically
// submitted to a particular partition determined by the empty key hash.
func (s *KafkaSuite) TestProduceEmptyKey(c *C) {
	// Given
	kafkaClient, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("kafka-clt-test")
	// When
	for i := 0; i < 100; i++ {
		kafkaClient.Produce("kafka-clt-test", []byte{}, []byte(strconv.Itoa(i)))
	}
	kafkaClient.Stop()
	kafkaClient.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("kafka-clt-test")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+100)
}

func (s *KafkaSuite) failedMessages() []string {
	b := []string{}
	for {
		select {
		case prodResult := <-s.handOffCh:
			b = append(b, string(prodResult.msg.Value.(sarama.ByteEncoder)))
		default:
			return b
		}
	}
	return b
}
