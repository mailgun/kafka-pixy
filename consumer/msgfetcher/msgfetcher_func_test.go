package msgfetcher

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	. "gopkg.in/check.v1"
)

type MsgFetcherFuncSuite struct {
	ns  *actor.ID
	kh  *kafkahelper.T
	cfg *config.Proxy
}

var _ = Suite(&MsgFetcherFuncSuite{})

func (s *MsgFetcherFuncSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	s.kh = kafkahelper.New(c)
}

func (s *MsgFetcherFuncSuite) TearDownSuite(c *C) {
	s.kh.Close()
}

func (s *MsgFetcherFuncSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
	s.cfg = testhelpers.NewTestProxyCfg("mis")
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
func (s *MsgFetcherFuncSuite) TestSlacker(c *C) {
	// {topic: "test.1", key: "foo"} and {topic: "test.4": key: "bar"} have
	// the same broker #9093 as a leader.
	producedTest1 := s.kh.PutMessages("slacker", "test.1", map[string]int{"foo": 11})
	producedTest4 := s.kh.PutMessages("slacker", "test.4", map[string]int{"bar": 1000})

	client, _ := sarama.NewClient(testhelpers.KafkaPeers, s.cfg.SaramaClientCfg())
	defer client.Close()

	s.cfg.Consumer.ChannelBufferSize = 10
	f, err := SpawnFactory(s.ns, s.cfg, client)
	c.Assert(err, IsNil)
	defer f.Stop()

	mfA, _, err := f.Spawn(s.ns.NewChild("test.1", 0), "test.1", 0, producedTest1["foo"][0].Offset)
	c.Assert(err, IsNil)
	defer mfA.Stop()

	mfB, _, err := f.Spawn(s.ns.NewChild("test.4", 2), "test.4", 2, producedTest4["bar"][0].Offset)
	c.Assert(err, IsNil)
	defer mfB.Stop()

	timeoutCh := time.After(1 * time.Second)
	for i := 0; i < 1000; i++ {
		select {
		case <-mfB.Messages():
			break
		case <-timeoutCh:
			// Both queues should be drained in parallel, otherwise the old
			// BrokerConsumer implementation would get into a deadlock here.
			go mfA.Stop()
			messagesA := mfA.Messages()
			go mfB.Stop()
			messagesB := mfB.Messages()
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
