package partitioncsm

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	log "github.com/sirupsen/logrus"
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
	msgIStreamF  msgfetcher.Factory
	offsetMgrF   offsetmgr.Factory
	kh           *kafkahelper.T
	initOffsetCh chan offsetmgr.Offset
}

var _ = Suite(&PartitionCsmSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

func (s *PartitionCsmSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	s.kh = kafkahelper.New(c)
	// Make sure that topic has at least 100 messages. There may be more,
	// because other tests are also using it.
	s.kh.PutMessages("pc", topic, map[string]int{"": 100})
}

func (s *PartitionCsmSuite) SetUpTest(c *C) {
	s.cfg = testhelpers.NewTestProxyCfg("test")
	s.cfg.Consumer.MaxPendingMessages = 100
	s.cfg.Consumer.MaxRetries = 1
	check4RetryInterval = 50 * time.Millisecond

	s.ns = actor.RootID.NewChild("T")
	s.groupMember = groupmember.Spawn(s.ns, group, memberID, s.cfg, s.kh.KazooClt())
	var err error
	if s.msgIStreamF, err = msgfetcher.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt()); err != nil {
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
// partition, then partition consumer will wait for the given offset to be
// reached by produced messages and the first message returned will the one
// with the initial offset.
func (s *PartitionCsmSuite) TestInitialOffsetTooLarge(c *C) {
	oldestOffsets := s.kh.GetOldestOffsets(topic)
	newestOffsets := s.kh.GetNewestOffsets(topic)
	log.Infof("*** test.1 offsets: oldest=%v, newest=%v", oldestOffsets, newestOffsets)
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{newestOffsets[partition] + 3, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()
	// Wait for the partition consumer to initialize.
	initialOffset := <-s.initOffsetCh

	// When
	messages := s.kh.PutMessages("pc", topic, map[string]int{"": 4})
	var msg consumer.Message
	select {
	case msg = <-pc.Messages():
	case <-time.After(time.Second):
		c.Errorf("Message is not consumed")
	}
	// Then
	c.Assert(msg.Offset, Equals, messages[""][3].Offset)
	c.Assert(msg.Offset, Equals, initialOffset.Val)
}

// A new message becomes available in the Messages() channel only after the
// previous one is reported as offered.
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
	sendEvOffered(msg)

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
	ot := offsettrk.New(s.ns, offsetmgr.Offset{Val: base}, -1)
	var initOffset offsetmgr.Offset
	for i, acked := range ackedDlts {
		if acked {
			initOffset, _ = ot.OnAcked(base + int64(i))
		}
	}
	c.Assert(offsettrk.SparseAcks2Str(initOffset), Equals, "1-4,6-7")
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
		sendEvOffered(msg)
		// Acknowledge to speed up the partition consumer stop.
		sendEvAcked(msg)
	}
}

// An attempt to confirm an offer of any message but the one recently read from
// Messages() channel is ignored.
func (s *PartitionCsmSuite) TestOfferInvalid(c *C) {
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{sarama.OffsetOldest, ""}})
	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	msg, ok := <-pc.Messages()
	c.Assert(ok, Equals, true)

	// When
	msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset + 1}
	msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset - 1}

	// Then
	msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset}
	msg2, ok := <-pc.Messages()
	c.Assert(msg2.Offset, Equals, msg.Offset+1)
	c.Assert(ok, Equals, true)
}

// If there are too many offered but not acknowledged messages then the
// partition consumer stops feeding messages via Messages() channel until the
// number of offered messages drops below maxOffered threshold.
func (s *PartitionCsmSuite) TestOfferedTooMany(c *C) {
	s.cfg.Consumer.AckTimeout = 500 * time.Millisecond
	s.cfg.Consumer.MaxPendingMessages = 3
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
		sendEvOffered(msg)
	}

	// No more message should be returned.
	select {
	case <-pc.Messages():
		c.Error("No messages should be available above HWM limit")
	case <-time.After(200 * time.Millisecond):
	}

	// Acknowledge a message.
	sendEvAcked(messages[1])

	// Total number of pending offered messages is 1 short of HWM limit. So we
	// should be able to read just one message.
	msg = <-pc.Messages()
	messages = append(messages, msg)
	// Confirm offered to get fetching going.
	sendEvOffered(msg)

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
		sendEvOffered(msg)
		if shouldAck {
			sendEvAcked(msg)
		}
	}
	pc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+1)
	c.Assert(offsettrk.SparseAcks2Str(offsetsAfter[partition]), Equals, "1-4,6-8")
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
		sendEvOffered(msg)
		messages = append(messages, msg)
	}

	sendEvAcked(messages[7])
	sendEvAcked(messages[1])
	sendEvAcked(messages[8])

	// When
	go pc.Stop() // Stop asynchronously.
	time.Sleep(100 * time.Millisecond)

	sendEvAcked(messages[3])
	sendEvAcked(messages[4])
	sendEvAcked(messages[6])
	sendEvAcked(messages[0])

	// Wait for partition consumer to stop.
	for {
		if _, ok := <-pc.Messages(); !ok {
			break
		}
	}

	// Acks sent after a partition consumer is stopped are ignored.
	sendEvAcked(messages[1])
	sendEvAcked(messages[4])

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+2)
	c.Assert(offsettrk.SparseAcks2Str(offsetsAfter[partition]), Equals, "1-3,4-7")
}

// If the max retries limit is reached for a message that results in
// termination of the partition consumer. Note that offset is properly
// committed to reflect sparsely acknowledged regions.
func (s *PartitionCsmSuite) TestMaxRetriesReached(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	s.cfg.Consumer.AckTimeout = 100 * time.Millisecond
	s.cfg.Consumer.MaxRetries = 3
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	var messages []consumer.Message
	for i := 0; i < 3; i++ {
		msg := <-pc.Messages()
		sendEvOffered(msg)
		messages = append(messages, msg)
	}

	// Acknowledge only one.
	sendEvAcked(messages[1])

	// The logic is so that the even when an offer is expired, at first a
	// freshly fetched message is offered, and then retries follow.
	base := offsetsBefore[partition] + int64(3)
	for i := 0; i < 3; i++ {
		time.Sleep(100 * time.Millisecond)
		// Newly fetched message is acknowledged...
		msgI := <-pc.Messages()
		c.Assert(msgI.Offset, Equals, base+int64(i))
		sendEvOffered(msgI)
		sendEvAcked(msgI)
		// ...but retried messages are not.
		msg0_i := <-pc.Messages()
		c.Assert(msg0_i, DeepEquals, messages[0], Commentf(
			"got: %d, want: %d", msg0_i.Offset, messages[0].Offset))
		sendEvOffered(messages[0])
		msg2_i := <-pc.Messages()
		c.Assert(msg2_i, DeepEquals, messages[2], Commentf(
			"got: %d, want: %d", msg2_i.Offset, messages[2].Offset))
		sendEvOffered(messages[2])
	}
	// Wait for the retry timeout to expire.
	time.Sleep(100 * time.Millisecond)

	// When/Then: maxRetries has been reached, only new messages are returned.
	for i := 0; i < 5; i++ {
		msg := <-pc.Messages()
		log.Infof("*** after max: %d, %d", i, msg.Offset)
		c.Assert(msg.Offset, Equals, base+int64(3+i), Commentf("i=%d", i))
		sendEvOffered(msg)
		sendEvAcked(msg)
	}

	pc.Stop()
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetsBefore[partition]+11)
	c.Assert(offsettrk.SparseAcks2Str(offsetsAfter[partition]), Equals, "")
}

// When several offers are expired they are retried in the same order they
// had been offered.
func (s *PartitionCsmSuite) TestSeveralMessageReties(c *C) {
	offsetsBefore := s.kh.GetOldestOffsets(topic)
	s.cfg.Consumer.AckTimeout = 100 * time.Millisecond
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: sarama.OffsetOldest}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)
	defer pc.Stop()

	// Read and confirm offered several messages, but do not ack them.
	for i := 0; i < 7; i++ {
		msg := <-pc.Messages()
		sendEvOffered(msg)
	}
	// Wait for all offers to expire...
	time.Sleep(100 * time.Millisecond)
	// ...the first message we read is not a retry and this is ok...
	msg := <-pc.Messages()
	sendEvOffered(msg)
	c.Assert(msg.Offset, Equals, offsetsBefore[partition]+int64(7))
	// ...but following 7 are.
	for i := 0; i < 7; i++ {
		msg := <-pc.Messages()
		sendEvOffered(msg)
		c.Assert(msg.Offset, Equals, offsetsBefore[partition]+int64(i))
	}
}

// If there are no new messages in the partition then only retries are returned
// via Messages() channel.
func (s *PartitionCsmSuite) TestRetryNoMoreMessages(c *C) {
	newestOffsets := s.kh.GetNewestOffsets(topic)
	offsetBefore := newestOffsets[partition] - int64(2)
	s.cfg.Consumer.AckTimeout = 100 * time.Millisecond
	s.cfg.Consumer.MaxRetries = 3
	s.kh.SetOffsets(group, topic, []offsetmgr.Offset{{Val: offsetBefore}})

	pc := Spawn(s.ns, group, topic, partition, s.cfg, s.groupMember, s.msgIStreamF, s.offsetMgrF)

	// Read and confirm offer of 4 messages
	var messages []consumer.Message
	for i := 0; i < 2; i++ {
		msg := <-pc.Messages()
		sendEvOffered(msg)
		messages = append(messages, msg)
	}

	// Wait for all offers to expire...
	time.Sleep(100 * time.Millisecond)

	// Then: Since there are no more messages in the partition, then the next
	// message returned is a retry.
	msg0_i := <-pc.Messages()
	c.Assert(msg0_i, DeepEquals, messages[0], Commentf(
		"got: %d, want: %d", msg0_i.Offset, messages[0].Offset))

	pc.Stop()
	offsetsAfter := s.kh.GetCommittedOffsets(group, topic)
	c.Assert(offsetsAfter[partition].Val, Equals, offsetBefore)
	c.Assert(offsettrk.SparseAcks2Str(offsetsAfter[partition]), Equals, "")
}

func sendEvOffered(msg consumer.Message) {
	log.Infof("*** sending EvOffered: offset=%d", msg.Offset)
	select {
	case msg.EventsCh <- consumer.Event{consumer.EvOffered, msg.Offset}:
	case <-time.After(500 * time.Millisecond):
		log.Infof("*** timeout sending `offered`: offset=%d", msg.Offset)
	}
}

func sendEvAcked(msg consumer.Message) {
	log.Infof("*** sending EvAcked: offset=%d", msg.Offset)
	select {
	case msg.EventsCh <- consumer.Event{consumer.EvAcked, msg.Offset}:
	case <-time.After(500 * time.Millisecond):
		log.Infof("*** timeout sending `acked`: offset=%d", msg.Offset)
	}
}
