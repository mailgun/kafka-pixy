package partitioncsm

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	. "gopkg.in/check.v1"
)

type PartitionConsumerFuncSuite struct {
	ns *actor.ID
	kh *kafkahelper.T
}

var _ = Suite(&PartitionConsumerFuncSuite{})

func (s *PartitionConsumerFuncSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
	s.kh = kafkahelper.New(c)
}

func (s *PartitionConsumerFuncSuite) TearDownSuite(c *C) {
	s.kh.Close()
}

func (s *PartitionConsumerFuncSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
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
	client, _ := sarama.NewClient(testhelpers.KafkaPeers, config)
	defer client.Close()
	f, err := SpawnFactory(s.ns, client)
	c.Assert(err, IsNil)
	defer f.Stop()

	pcA, _, err := f.SpawnPartitionConsumer(s.ns.NewChild("test.1", 0), "test.1", 0, producedTest1["foo"][0].Offset)
	c.Assert(err, IsNil)
	defer pcA.Stop()

	pcB, _, err := f.SpawnPartitionConsumer(s.ns.NewChild("test.4", 2), "test.4", 2, producedTest4["bar"][0].Offset)
	c.Assert(err, IsNil)
	defer pcB.Stop()

	timeoutCh := time.After(1 * time.Second)
	for i := 0; i < 1000; i++ {
		select {
		case <-pcB.Messages():
			break
		case <-timeoutCh:
			// Both queues should be drained in parallel, otherwise the old
			// BrokerConsumer implementation would get into a deadlock here.
			go pcA.Stop()
			messagesA := pcA.Messages()
			go pcB.Stop()
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
