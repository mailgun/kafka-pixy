package consumer

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	. "gopkg.in/check.v1"
)

type PartitionConsumerFuncSuite struct {
	kh *kafkahelper.T
}

var _ = Suite(&PartitionConsumerFuncSuite{})

func (s *PartitionConsumerFuncSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
	s.kh = kafkahelper.New(c)
}

// BrokerConsumer used to be implemented so that if the message channel of one
// of the associated PartitionConsumers got full, that prevented ALL the rest
// PartitionConsumers from receiving messages from the BrokerConsumer.
//
// This test makes sure that if the queue of one PartitionConsumer gets full,
// the other keep getting new messages.
//
// IMPORTANT: The topic/key of the two sets of the generated messages had been
// selected so that both sets end up in partitions that has the same leader.
func (s *PartitionConsumerFuncSuite) TestSlacker(c *C) {
	// {topic: "test.1", key: "foo"} and {topic: "test.4": key: "bar"} have
	// the same broker #9093 as a leader.
	producedTest1 := s.kh.PutMessages("slacker", "test.1", map[string]int{"foo": 11})
	producedTest4 := s.kh.PutMessages("slacker", "test.4", map[string]int{"bar": 1000})

	config := sarama.NewConfig()
	// The channel buffer size is selected to be one short of the number of
	// messages in the `test.1` topic. So that an attempt to write the
	// (buffer size + 1)th message to the respective PartitionConsumer message
	// channel will block, given that nobody is reading from the channel.
	config.ChannelBufferSize = 10
	f, err := NewConsumer(testhelpers.KafkaPeers, config)
	c.Assert(err, IsNil)
	defer f.Close()

	pcA, _, err := f.ConsumePartition("test.1", 0, producedTest1["foo"][0].Offset)
	c.Assert(err, IsNil)
	defer pcA.Close()

	pcB, _, err := f.ConsumePartition("test.4", 2, producedTest4["bar"][0].Offset)
	c.Assert(err, IsNil)
	defer pcB.Close()

	timeoutCh := time.After(1 * time.Second)
	for i := 0; i < 1000; i++ {
		select {
		case <-pcB.Messages():
			break
		case <-timeoutCh:
			// Both queues should be drained in parallel, otherwise the old
			// BrokerConsumer implementation would get into a deadlock here.
			go pcA.Close()
			messagesA := pcA.Messages()
			go pcB.Close()
			messagesB := pcB.Messages()
		drainLoop:
			for messagesA != nil || messagesB != nil {
				select {
				case _, ok := <-messagesA:
					if !ok {
						messagesA = nil
					}
				case _, ok := <-messagesB:
					if !ok {
						messagesB = nil
					}
				default:
					break drainLoop
				}
			}
			c.Errorf("Failed to read all messages from pcB within 1 seconds: read=%v", i)
		}
	}
}
