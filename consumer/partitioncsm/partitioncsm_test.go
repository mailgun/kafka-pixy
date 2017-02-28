package partitioncsm

import (
	"testing"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgistream"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"github.com/mailgun/log"
	. "gopkg.in/check.v1"
)

const (
	group     = "test_group"
	topic     = "test.1"
	memberID  = "test_member"
	partition = 0
)

type PartitionCsmSuite struct {
	cfg          *config.Proxy
	ns           *actor.ID
	groupMember  *groupmember.T
	msgIStreamF  msgistream.Factory
	offsetMgrF   offsetmgr.Factory
	kh           *kafkahelper.T
	initOffsetCh chan offsetmgr.Offset
}

var _ = Suite(&PartitionCsmSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

func (s *PartitionCsmSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
	s.kh = kafkahelper.New(c)
	// Make sure that topic has at least 100 messages. There may be more,
	// because other tests are also using it.
	s.kh.PutMessages("pc", topic, map[string]int{"": 100})
}

func (s *PartitionCsmSuite) SetUpTest(c *C) {
	s.cfg = testhelpers.NewTestProxyCfg("test")
	s.ns = actor.RootID.NewChild("T")
	s.groupMember = groupmember.Spawn(s.ns, group, memberID, s.cfg, s.kh.KazooClt())
	var err error
	if s.msgIStreamF, err = msgistream.SpawnFactory(s.ns, s.kh.KafkaClt()); err != nil {
		panic(err)
	}
	s.offsetMgrF = offsetmgr.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt())

	s.initOffsetCh = make(chan offsetmgr.Offset, 1)
	initialOffsetCh = s.initOffsetCh
}

func (s *PartitionCsmSuite) TearDownTest(c *C) {
}

// If sarama.OffsetOldest constant is returned as the last committed offset,
// then it is initialized to the actual offset value of the oldest message.
func (s *PartitionCsmSuite) TestOldestOffset(c *C) {
	oldestOffsets := s.kh.GetOldestOffsets(topic)
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{sarama.OffsetOldest, ""}})
	offsets := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsets[partition], Equals, offsetmgr.Offset{sarama.OffsetOldest, ""})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// When
	<-pc.Messages()
	pc.Stop()

	// Then
	offsets = s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsets[partition].Val, Equals, oldestOffsets[partition])
}

// If initial offset stored in Kafka is greater then the newest offset for a
// partition, then the first message consumed from the partition is the next one
// posted to it.
func (s *PartitionCsmSuite) TestInitialOffsetTooLarge(c *C) {
	oldestOffsets := s.kh.GetOldestOffsets(topic)
	newestOffsets := s.kh.GetNewestOffsets(topic)
	log.Infof("*** test.1 offsets: oldest=%v, newest=%v", oldestOffsets, newestOffsets)
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{newestOffsets[partition] + 100, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()
	// Wait for the partition consumer to initialize.
	initialOffset := <-s.initOffsetCh

	// When
	messages := s.kh.PutMessages("pc", topic, map[string]int{"": 1})
	msg := <-pc.Messages()

	// Then
	c.Assert(msg.Offset, Equals, messages[""][0].Offset)
	c.Assert(msg.Offset, Equals, initialOffset.Val)
}

func (s *PartitionCsmSuite) TestOldLogic(c *C) {
	s.kh.ResetOffsets(group, topic)
	before := s.kh.GetCommittedOffsets(group, topic)
	s.kh.PutMessages("pc", topic, map[string]int{"": 10})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// When
	pc.Offers() <- <-pc.Messages()
	pc.Offers() <- <-pc.Messages()
	pc.Offers() <- <-pc.Messages()
	<-pc.Messages()
	<-pc.Messages()
	pc.Stop()

	// Then
	after := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(after[partition].Val, Equals, before[partition].Val+3)
}
