package partitioncsm

import (
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgistream"
	"github.com/mailgun/kafka-pixy/consumer/offsettrac"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"github.com/mailgun/log"
	. "gopkg.in/check.v1"
)

const (
	group     = "test_group"
	topic     = "test.1"
	partition = 0
	memberID  = "test_member"
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
	if s.msgIStreamF, err = msgistream.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt()); err != nil {
		panic(err)
	}
	s.offsetMgrF = offsetmgr.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt())

	s.initOffsetCh = make(chan offsetmgr.Offset, 1)
	initialOffsetCh = s.initOffsetCh

	// Reset constants that may be modified by tests.
	resetConstants()
	check4RetryInterval = 50 * time.Millisecond
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

// A message read from Messages() must be offered via Offered() before a next
// one can be read from Messages().
func (s *PartitionCsmSuite) TestMustBeOfferedToProceed(c *C) {
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{sarama.OffsetOldest, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	// When
	msg := <-pc.Messages()
	select {
	case <-pc.Messages():
		c.Error("Message must not be available until previous is offered")
	case <-time.After(200 * time.Millisecond):
	}
	sendEOffered(msg)

	// Then
	<-pc.Messages()
}

// If the initial offset has sparsely acked messages then they are not returned
// from Messages() channel.
func (s *PartitionCsmSuite) TestSparseAckedNotRead(c *C) {
	ackedDlts := []bool{
		/* 0 */ false,
		/* 1 */ true,
		/* 2 */ true,
		/* 3 */ true,
		/* 4 */ false,
		/* 5 */ false,
		/* 6 */ true,
		/* 7 */ false,
		/* 8 */ false,
	}
	// Make initial offset that has sparsely acked ranges.
	oldestOffsets := s.kh.GetOldestOffsets(topic)
	base := oldestOffsets[partition]
	ot := offsettrac.New(s.ns, offsetmgr.Offset{Val: base}, -1)
	var initOffset offsetmgr.Offset
	for i, acked := range ackedDlts {
		if acked {
			initOffset, _ = ot.OnAcked(base + int64(i))
		}
	}
	c.Assert(offsettrac.SparseAcks2Str(initOffset), Equals, "1-4,6-7")
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{initOffset})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	// When/Then: only messages that has not been acked previously are returned.
	for i, acked := range ackedDlts {
		if acked {
			continue
		}
		msg := <-pc.Messages()
		c.Assert(msg.Offset, Equals, base+int64(i))

		// Confirm offered to get fetching going
		sendEOffered(msg)
		// Acknowledge to speed up the partition consumer stop.
		sendEAcked(msg)
	}
}

// An attempt to confirm offer of any message but the one recently read from
// Messages() channel results in termination of the partition consumer.
func (s *PartitionCsmSuite) TestOfferIvalid(c *C) {
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{sarama.OffsetOldest, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	// When
	msg, ok := <-pc.Messages()
	c.Assert(ok, Equals, true)
	msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset + 1}

	// Then
	_, ok = <-pc.Messages()
	c.Assert(ok, Equals, false)
}

// If there are too many offered but not acknowledged messages then the
// partition consumer stops feed messages via Messages() channel until the
// number of offered messages drops below offeredHighWaterMark threshold.
func (s *PartitionCsmSuite) TestOfferedTooMany(c *C) {
	offeredHighWaterMark = 3
	s.cfg.Consumer.AckTimeout = 500 * time.Millisecond
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{sarama.OffsetOldest, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()
	var msg consumer.Message

	// Read and confirm offered messages up to the HWM+1 limit.
	var messages []consumer.Message
	for i := 0; i < 4; i++ {
		msg = <-pc.Messages()
		messages = append(messages, msg)
		// Confirm offered to get fetching going.
		sendEOffered(msg)
	}

	// No more message should be returned.
	select {
	case <-pc.Messages():
		c.Error("No messages should be available above HWM limit")
	case <-time.After(200 * time.Millisecond):
	}

	// Acknowledge some message.
	sendEAcked(messages[1])

	// Total number of pending offered messages is 1 short of HWM limit. So we
	// should be able to read just one message.
	msg = <-pc.Messages()
	messages = append(messages, msg)
	// Confirm offered to get fetching going.
	sendEOffered(msg)

	select {
	case msg := <-pc.Messages():
		c.Errorf("No messages should be available above HWM limit: %v", msg)
	case <-time.After(200 * time.Millisecond):
	}
}

// If some offered messages are not committed on stop. Then they are encoded in
// the committed offset metadata.
func (s *PartitionCsmSuite) TestSparseAckedCommitted(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	acks := []bool{
		/* 0 */ true,
		/* 1 */ false,
		/* 2 */ true,
		/* 3 */ true,
		/* 4 */ true,
		/* 5 */ false,
		/* 6 */ false,
		/* 7 */ true,
		/* 8 */ true,
		/* 9 */ false,
	}
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// When
	for _, shouldAck := range acks {
		msg := <-pc.Messages()
		sendEOffered(msg)
		if shouldAck {
			sendEAcked(msg)
		}
	}
	pc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+1)
	c.Assert(offsettrac.SparseAcks2Str(offsetsAfter[partition]), Equals, "1-4,6-8")
}

// When a partition consumer is signalled to stop it waits at most
// Consumer.AckTimeout for acks to arrive, and then commits whatever it has
// gotten and terminates.
func (s *PartitionCsmSuite) TestSparseAckedAfterStop(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	s.cfg.Consumer.AckTimeout = 300 * time.Millisecond
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	var messages []consumer.Message
	for i := 0; i < 10; i++ {
		msg := <-pc.Messages()
		sendEOffered(msg)
		messages = append(messages, msg)
	}

	sendEAcked(messages[7])
	sendEAcked(messages[1])
	sendEAcked(messages[8])

	// When
	go pc.Stop() // Stop asynchronously.
	time.Sleep(100 * time.Millisecond)

	sendEAcked(messages[3])
	sendEAcked(messages[4])
	sendEAcked(messages[6])
	sendEAcked(messages[0])

	// Wait for partition consumer to stop.
	for {
		if _, ok := <-pc.Messages(); !ok {
			break
		}
	}
	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+2)
	c.Assert(offsettrac.SparseAcks2Str(offsetsAfter[partition]), Equals, "1-3,4-7")
}

// If the max retries limit is reached for a message that results in
// termination of the partition consumer. Note that offset is properly
// committed to reflect sparsely acknowledged regions.
func (s *PartitionCsmSuite) TestMaxRetriesReached(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	s.cfg.Consumer.AckTimeout = 100 * time.Millisecond
	retriesEmergencyBreak = 4
	retriesHighWaterMark = 1
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// Read and confirm offer of 2 messages
	msg0 := <-pc.Messages()
	sendEOffered(msg0)
	msg1 := <-pc.Messages()
	sendEOffered(msg1)

	// Acknowledge only the first one
	sendEAcked(msg0)

	// The logic is so that the even when an offer is expired, at first a
	// freshly fetched message is offered, and then retries follow.
	base := offsetsBefore[partition] + int64(2)
	for i := 0; i < 4; i++ {
		time.Sleep(100 * time.Millisecond)
		// Newly fetched message is acknowledged...
		msgI := <-pc.Messages()
		c.Assert(msgI.Offset, Equals, base+int64(i))
		sendEOffered(msgI)
		sendEAcked(msgI)
		// ...but retried message is not.
		msg1_i := <-pc.Messages()
		c.Assert(msg1_i, DeepEquals, msg1, Commentf("got: %d, want: %d", msg1_i.Offset, msg1.Offset))
		sendEOffered(msg1)
	}
	// Expire offer of the retried message one last time.
	time.Sleep(100 * time.Millisecond)
	msgI := <-pc.Messages()
	c.Assert(msgI.Offset, Equals, base+int64(4))
	sendEOffered(msgI)
	sendEAcked(msgI)

	// Wait for partition consumer to stop.
	for {
		if _, ok := <-pc.Messages(); !ok {
			break
		}
	}
	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+1)
	c.Assert(offsettrac.SparseAcks2Str(offsetsAfter[partition]), Equals, "1-6")
}

// When several offers are expired they are retried in the same order they
// were offered.
func (s *PartitionCsmSuite) TestSeveralMessageReties(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	s.cfg.Consumer.AckTimeout = 100 * time.Millisecond
	retriesEmergencyBreak = 4
	retriesHighWaterMark = 1
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	// Read and confirm offered several messages, but do not ack them.
	for i := 0; i < 7; i++ {
		msg := <-pc.Messages()
		sendEOffered(msg)
	}
	// Wait for all offers to expire...
	time.Sleep(100 * time.Millisecond)
	// ...first message we read is not a retry and this is ok...
	msg := <-pc.Messages()
	sendEOffered(msg)
	c.Assert(msg.Offset, Equals, offsetsBefore[partition]+int64(7))
	// ...but following 7 are.
	for i := 0; i < 7; i++ {
		msg := <-pc.Messages()
		sendEOffered(msg)
		c.Assert(msg.Offset, Equals, offsetsBefore[partition]+int64(i))
	}
}

func (s *PartitionCsmSuite) TestRetryNoMoreMessages(c *C) {
	newestOffsets := s.kh.GetNewestOffsets(topic)
	offsetBefore := newestOffsets[partition] - int64(2)
	s.cfg.Consumer.AckTimeout = 200 * time.Millisecond
	retriesEmergencyBreak = 4
	retriesHighWaterMark = 1
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: offsetBefore}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// Read and confirm offer of 2 messages
	msg0 := <-pc.Messages()
	sendEOffered(msg0)
	msg1 := <-pc.Messages()
	sendEOffered(msg1)

	// Acknowledge only the first one.
	time.Sleep(100 * time.Millisecond)
	sendEAcked(msg0)

	// Since there are no more messages in the partition, then only retries are
	// offered.
	for i := 0; i < 4; i++ {
		msg1_i := <-pc.Messages()
		c.Assert(msg1_i, DeepEquals, msg1, Commentf("got: %d, want: %d", msg1_i.Offset, msg1.Offset))
		sendEOffered(msg1)
	}

	// Wait for partition consumer to stop.
	for {
		if _, ok := <-pc.Messages(); !ok {
			break
		}
	}
	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetBefore+int64(1))
	c.Assert(offsettrac.SparseAcks2Str(offsetsAfter[partition]), Equals, "")
}

func (s *PartitionCsmSuite) TestAckedOnStop(c *C) {
	offsetBefore := s.kh.GetNewestOffsets(topic)[partition] - 10
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: offsetBefore}})
	s.cfg.Consumer.AckTimeout = 200 * time.Millisecond
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// Read and confirm offer of 2 messages
	msg0 := <-pc.Messages()
	sendEOffered(msg0)
	time.Sleep(100 * time.Millisecond)
	msg1 := <-pc.Messages()
	sendEOffered(msg1)

	// When
	var wg sync.WaitGroup
	actor.Spawn(s.ns.NewChild("brake"), &wg, pc.Stop)
	defer wg.Wait()
	time.Sleep(100 * time.Millisecond)

	// Acknowledge only the first one.
	sendEAcked(msg0)
	sendEAcked(msg1)

	// Wait for partition consumer to stop.
	for {
		if _, ok := <-pc.Messages(); !ok {
			break
		}
	}
	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetBefore+2)
	c.Assert(offsettrac.SparseAcks2Str(offsetsAfter[partition]), Equals, "")
}

func sendEOffered(msg consumer.Message) {
	log.Infof("*** sending `offered`: offset=%d", msg.Offset)
	select {
	case msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset}:
	case <-time.After(500 * time.Millisecond):
		log.Infof("*** timeout sending `offered`: offset=%d", msg.Offset)
	}
}

func sendEAcked(msg consumer.Message) {
	log.Infof("*** sending `acked`: offset=%d", msg.Offset)
	select {
	case msg.EventsCh <- consumer.Event{consumer.EvAcked, msg.Offset}:
	case <-time.After(500 * time.Millisecond):
		log.Infof("*** timeout sending `acked`: offset=%d", msg.Offset)
	}
}
