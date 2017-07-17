package producer

import (
	"strconv"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"github.com/pkg/errors"
	. "gopkg.in/check.v1"
)

type ProducerSuite struct {
	ns           *actor.ID
	cfg          *config.Proxy
	kh           *kafkahelper.T
	droppedMsgCh chan *sarama.ProducerMessage
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&ProducerSuite{})

func (s *ProducerSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *ProducerSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
	s.droppedMsgCh = make(chan *sarama.ProducerMessage, 100)
	s.cfg = config.DefaultProxy()
	s.cfg.Kafka.SeedPeers = testhelpers.KafkaPeers
	s.kh = kafkahelper.New(c)
}

func (s *ProducerSuite) TearDownTest(c *C) {
	s.kh.Close()
}

// A started client can be stopped.
func (s *ProducerSuite) TestStartAndStop(c *C) {
	// Given
	p, err := Spawn(s.ns, s.cfg)
	c.Assert(err, IsNil)
	c.Assert(p, NotNil)
	// When
	p.Stop()
}

func (s *ProducerSuite) TestProduce(c *C) {
	p, _ := Spawn(s.ns, s.cfg)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	_, err := p.Produce("test.4", sarama.StringEncoder("1"), sarama.StringEncoder("Foo"))

	// Then
	c.Assert(err, IsNil)
	offsetsAfter := s.kh.GetNewestOffsets("test.4")
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+1)

	// Cleanup
	p.Stop()
}

func (s *ProducerSuite) TestProduceInvalidTopic(c *C) {
	p, _ := Spawn(s.ns, s.cfg)

	// When
	_, err := p.Produce("no-such-topic", sarama.StringEncoder("1"), sarama.StringEncoder("Foo"))

	// Then
	c.Assert(err, Equals, sarama.ErrUnknownTopicOrPartition)

	// Cleanup
	p.Stop()
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *ProducerSuite) TestAsyncProduce(c *C) {
	p, _ := Spawn(s.ns, s.cfg)
	p.testDroppedMsgCh = s.droppedMsgCh
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 10; i++ {
		p.AsyncProduce("test.4", sarama.StringEncoder("1"), sarama.StringEncoder(strconv.Itoa(i)))
		p.AsyncProduce("test.4", sarama.StringEncoder("2"), sarama.StringEncoder(strconv.Itoa(i)))
		p.AsyncProduce("test.4", sarama.StringEncoder("3"), sarama.StringEncoder(strconv.Itoa(i)))
		p.AsyncProduce("test.4", sarama.StringEncoder("4"), sarama.StringEncoder(strconv.Itoa(i)))
		p.AsyncProduce("test.4", sarama.StringEncoder("5"), sarama.StringEncoder(strconv.Itoa(i)))
	}
	p.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

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
	p, _ := Spawn(s.ns, s.cfg)
	p.testDroppedMsgCh = s.droppedMsgCh
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 100; i++ {
		p.AsyncProduce("test.4", nil, sarama.StringEncoder(strconv.Itoa(i)))
	}
	p.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Assert(s.failedMessages(), DeepEquals, []string{})
	delta0 := offsetsAfter[0] - offsetsBefore[0]
	delta1 := offsetsAfter[1] - offsetsBefore[1]
	if delta0 == 0 || delta1 == 0 {
		panic(errors.Errorf("Too high imbalance: %v != %v", delta0, delta1))
	}
}

// Even though wrapped `sarama.Producer` is instructed to stop immediately on
// client stop due to `ShutdownTimeout == 0`, still none of messages is lost.
// because none of them are retries. This test is mostly to increase coverage.
func (s *ProducerSuite) TestTooSmallShutdownTimeout(c *C) {
	s.cfg.Producer.ShutdownTimeout = 0
	p, _ := Spawn(s.ns, s.cfg)
	p.testDroppedMsgCh = s.droppedMsgCh
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 100; i++ {
		v := sarama.StringEncoder(strconv.Itoa(i))
		p.AsyncProduce("test.4", v, v)
	}
	p.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

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
	p, _ := Spawn(s.ns, s.cfg)
	p.testDroppedMsgCh = s.droppedMsgCh
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 10; i++ {
		p.AsyncProduce("test.4", sarama.StringEncoder(""), sarama.StringEncoder(strconv.Itoa(i)))
	}
	p.Stop()
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

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
		case prodMsg := <-s.droppedMsgCh:
			b = append(b, string(prodMsg.Value.(sarama.StringEncoder)))
		default:
			goto done
		}
	}
done:
	return b
}
