package pixy

import (
	"fmt"
	"strconv"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type ProducerSuite struct {
	cfg           *KafkaClientCfg
	tkc           *TestKafkaClient
	deadMessageCh chan *ProduceResult
}

var _ = Suite(&ProducerSuite{})

func (s *ProducerSuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *ProducerSuite) SetUpTest(c *C) {
	s.deadMessageCh = make(chan *ProduceResult, 100)
	s.cfg = NewKafkaClientCfg()
	s.cfg.BrokerAddrs = testBrokers
	s.cfg.DeadMessageCh = s.deadMessageCh
	s.tkc = NewTestKafkaClient(s.cfg.BrokerAddrs)
}

func (s *ProducerSuite) TearDownTest(c *C) {
	close(s.deadMessageCh)
	s.tkc.Close()
}

// A started client can be stopped.
func (s *ProducerSuite) TestStartAndStop(c *C) {
	// Given
	kci, err := SpawnKafkaClient(s.cfg)
	c.Assert(err, IsNil)
	c.Assert(kci, NotNil)
	// When
	kci.Stop()
	// Then
	kci.Wait4Stop()
}

func (s *ProducerSuite) TestProduce(c *C) {
	// Given
	kci, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	err := kci.Produce("test.4", sarama.StringEncoder("1"), sarama.StringEncoder("Foo"))
	// Then
	c.Assert(err, IsNil)
	offsetsAfter := s.tkc.getOffsets("test.4")
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+1)
	// Cleanup
	kci.Stop()
	kci.Wait4Stop()
}

func (s *ProducerSuite) TestProduceInvalidTopic(c *C) {
	// Given
	kci, _ := SpawnKafkaClient(s.cfg)
	// When
	err := kci.Produce("no-such-topic", sarama.StringEncoder("1"), sarama.StringEncoder("Foo"))
	// Then
	c.Assert(err, Equals, sarama.ErrUnknownTopicOrPartition)
	// Cleanup
	kci.Stop()
	kci.Wait4Stop()
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *ProducerSuite) TestAsyncProduce(c *C) {
	// Given
	kci, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 10; i++ {
		kci.AsyncProduce("test.4", sarama.StringEncoder("1"), sarama.StringEncoder(strconv.Itoa(i)))
		kci.AsyncProduce("test.4", sarama.StringEncoder("2"), sarama.StringEncoder(strconv.Itoa(i)))
		kci.AsyncProduce("test.4", sarama.StringEncoder("3"), sarama.StringEncoder(strconv.Itoa(i)))
		kci.AsyncProduce("test.4", sarama.StringEncoder("4"), sarama.StringEncoder(strconv.Itoa(i)))
		kci.AsyncProduce("test.4", sarama.StringEncoder("5"), sarama.StringEncoder(strconv.Itoa(i)))
	}
	kci.Stop()
	kci.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+20)
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+10)
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2]+10)
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// If `key` of a produced message is `nil` then it is submitted to a random
// partition. Therefore a batch of such messages is evenly distributed among
// all available partitions.
func (s *ProducerSuite) TestAsyncProduceNilKey(c *C) {
	// Given
	kci, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 100; i++ {
		kci.AsyncProduce("test.4", nil, sarama.StringEncoder(strconv.Itoa(i)))
	}
	kci.Stop()
	kci.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
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
func (s *ProducerSuite) TestTooSmallShutdownTimeout(c *C) {
	// Given
	s.cfg.ShutdownTimeout = 0
	kci, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 100; i++ {
		v := sarama.StringEncoder(strconv.Itoa(i))
		kci.AsyncProduce("test.4", v, v)
	}
	kci.Stop()
	kci.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	delta := int64(0)
	for i := 0; i < 4; i++ {
		delta += offsetsAfter[i] - offsetsBefore[i]
	}
	c.Assert(delta, Equals, int64(100))
}

// If `key` of a produced message is empty then it is deterministically
// submitted to a particular partition determined by the empty key hash.
func (s *ProducerSuite) TestAsyncProduceEmptyKey(c *C) {
	// Given
	kci, _ := SpawnKafkaClient(s.cfg)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 10; i++ {
		kci.AsyncProduce("test.4", sarama.StringEncoder(""), sarama.StringEncoder(strconv.Itoa(i)))
	}
	kci.Stop()
	kci.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1])
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2])
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

func (s *ProducerSuite) failedMessages() []string {
	b := []string{}
	for {
		select {
		case prodResult := <-s.deadMessageCh:
			b = append(b, string(prodResult.Msg.Value.(sarama.ByteEncoder)))
		default:
			goto done
		}
	}
done:
	return b
}
