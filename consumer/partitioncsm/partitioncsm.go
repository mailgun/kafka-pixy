package partitioncsm

import (
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

var (
	// TESTING ONLY!: If this channel is not `nil` then partition consumers
	// will use it to notify when they fetch the very first message.
	FirstMessageFetchedCh chan *T

	initialOffsetCh chan offsetmgr.Offset

	// Sets an interval for periodical checks for messages to retry.
	check4RetryInterval = time.Second
)

// T ensures exclusive consumption of messages from a topic
// partition within a particular group. It ensures that a partition is consumed
// exclusively by first claiming the partition in ZooKeeper. When a fetched
// message is pulled from the `messages()` channel, it is considered to be
// consumed and its offset is committed.
type T struct {
	actorID     *actor.ID
	cfg         *config.Proxy
	group       string
	topic       string
	partition   int32
	groupMember *groupmember.T
	msgFetcherF msgfetcher.Factory
	offsetMgrF  offsetmgr.Factory
	messagesCh  chan consumer.Message
	eventsCh    chan consumer.Event
	stopCh      chan none.T
	wg          sync.WaitGroup

	offsetMgr       offsetmgr.T
	committedOffset offsetmgr.Offset
	submittedOffset offsetmgr.Offset
	offsetsOk       bool
	offsetTrk       *offsettrk.T

	// For tests only!
	firstMsgFetched bool
}

// Spawn creates a partition consumer instance and starts its goroutines.
func Spawn(namespace *actor.ID, group, topic string, partition int32, cfg *config.Proxy,
	groupMember *groupmember.T, msgFetcherF msgfetcher.Factory, offsetMgrF offsetmgr.Factory,
) *T {
	pc := &T{
		actorID:     namespace.NewChild(fmt.Sprintf("P:%s_%d", topic, partition)),
		cfg:         cfg,
		group:       group,
		topic:       topic,
		partition:   partition,
		groupMember: groupMember,
		msgFetcherF: msgFetcherF,
		offsetMgrF:  offsetMgrF,
		messagesCh:  make(chan consumer.Message, 1),
		eventsCh:    make(chan consumer.Event, 1),
		stopCh:      make(chan none.T),
	}
	actor.Spawn(pc.actorID, &pc.wg, pc.run)
	return pc
}

// Topic returns the partition ID this partition consumer is responsible for.
func (pc *T) Partition() int32 {
	return pc.partition
}

// implements `multiplexer.In`
func (pc *T) Messages() <-chan consumer.Message {
	return pc.messagesCh
}

func (pc *T) run() {
	defer close(pc.messagesCh)
	defer pc.groupMember.ClaimPartition(pc.actorID, pc.topic, pc.partition, pc.stopCh)()

	var err error
	if pc.offsetMgr, err = pc.offsetMgrF.Spawn(pc.actorID, pc.group, pc.topic, pc.partition); err != nil {
		panic(errors.Wrapf(err, "<%s> must never happen", pc.actorID))
	}
	defer pc.stopOffsetMgr()

	// Wait for the initial offset to be retrieved or a stop signal.
	select {
	case pc.committedOffset = <-pc.offsetMgr.CommittedOffsets():
	case <-pc.stopCh:
		return
	}
	log.Infof("<%s> initial offset: %d, sparseAcks=%s",
		pc.actorID, pc.committedOffset.Val, offsettrk.SparseAcks2Str(pc.committedOffset))
	pc.offsetTrk = offsettrk.New(pc.actorID, pc.committedOffset, pc.cfg.Consumer.AckTimeout)
	pc.submittedOffset = pc.committedOffset
	pc.offsetsOk = true
	pc.notifyTestInitialized(pc.committedOffset)

	for pc.runFetchLoop() {
	}

	for ok, timeout := pc.offsetTrk.ShouldWait4Ack(); ok; ok, timeout = pc.offsetTrk.ShouldWait4Ack() {
		select {
		case event := <-pc.eventsCh:
			if event.T == consumer.EvAcked {
				pc.submittedOffset, _ = pc.offsetTrk.OnAcked(event.Offset)
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
			}
		case <-time.After(timeout):
			continue
		}
	}
}

func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}

func (pc *T) runFetchLoop() bool {
	// Initialize a message fetcher to read from the initial offset.
	mf, realOffsetVal, err := pc.msgFetcherF.Spawn(pc.actorID, pc.topic, pc.partition, pc.committedOffset.Val)
	if err != nil {
		panic(errors.Wrapf(err, "<%s> must never happen", pc.actorID))
	}
	defer mf.Stop()

	pc.submittedOffset = pc.offsetTrk.Adjust(realOffsetVal)
	// If the real offset is different from the committed one then submit it
	// and report in the logs.
	if pc.submittedOffset != pc.committedOffset {
		pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		log.Errorf("<%s> offset adjusted: %d, sparseAcks=%s",
			pc.actorID, pc.submittedOffset.Val, offsettrk.SparseAcks2Str(pc.submittedOffset))
	}
	var (
		nilOrMsgFetcherCh = mf.Messages()
		nilOrMessagesCh   chan consumer.Message
		retryTicker       = time.NewTicker(check4RetryInterval)
		msg               consumer.Message
		msgOk             bool
	)
	defer retryTicker.Stop()
	for {
		select {
		case msg = <-nilOrMsgFetcherCh:
			if ok, _ := pc.offsetTrk.IsAcked(msg.Offset); ok {
				continue
			}
			msg.EventsCh = pc.eventsCh
			msgOk = true
			pc.notifyTestFetched()
			nilOrMsgFetcherCh = nil
			nilOrMessagesCh = pc.messagesCh
		case <-retryTicker.C:
			if msgOk {
				continue
			}
			if msg, msgOk = pc.nextRetry(); msgOk {
				nilOrMsgFetcherCh = nil
				nilOrMessagesCh = pc.messagesCh
			}
		case nilOrMessagesCh <- msg:
			nilOrMessagesCh = nil
		case event := <-pc.eventsCh:
			switch event.T {
			case consumer.EvOffered:
				if event.Offset != msg.Offset {
					log.Errorf("<%s> invalid offer offset %d, want=%d", pc.actorID, event.Offset, msg.Offset)
					continue
				}
				offeredCount := pc.offsetTrk.OnOffered(msg)
				if msg, msgOk = pc.nextRetry(); msgOk {
					nilOrMessagesCh = pc.messagesCh
					continue
				}
				if offeredCount > pc.cfg.Consumer.MaxPendingMessages {
					log.Warningf("<%s> offered count above HWM: %d", pc.actorID, offeredCount)
					nilOrMsgFetcherCh = nil
					continue
				}
				nilOrMsgFetcherCh = mf.Messages()
			case consumer.EvAcked:
				var offeredCount int
				pc.submittedOffset, offeredCount = pc.offsetTrk.OnAcked(event.Offset)
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
				if !msgOk && offeredCount <= pc.cfg.Consumer.MaxPendingMessages {
					nilOrMsgFetcherCh = mf.Messages()
				}
			}
		case pc.committedOffset = <-pc.offsetMgr.CommittedOffsets():
		case <-pc.stopCh:
			return false
		}
	}
}

// nextRetry checks with the offset tracker if there is a message ready to be
// retried. If it gets a message that has already been retried maxRetries times,
// then it acks the message and asks the offset tracker for another one. It
// continues doing that until either a message with less then maxRetries is
// returned or there are no more messages to be retried.
func (pc *T) nextRetry() (consumer.Message, bool) {
	msg, retryNo, ok := pc.offsetTrk.NextRetry()
	for ok && retryNo > pc.cfg.Consumer.MaxRetries {
		log.Errorf("<%s> too many retries: retryNo=%d, offset=%d, key=%s, msg=%s",
			pc.actorID, retryNo, msg.Offset, string(msg.Key), base64.StdEncoding.EncodeToString(msg.Value))
		pc.submittedOffset, _ = pc.offsetTrk.OnAcked(msg.Offset)
		pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		// TODO: Dump expired messages to a long term storage?
		msg, retryNo, ok = pc.offsetTrk.NextRetry()
	}
	if ok {
		log.Warningf("<%s> retrying: retryNo=%d, offset=%d, key=%s",
			pc.actorID, retryNo, msg.Offset, string(msg.Key))
	}
	return msg, ok
}

func (pc *T) stopOffsetMgr() {
	pc.offsetMgr.Stop()
	if !pc.offsetsOk {
		return
	}
	// Drain committed offsets.
	for pc.committedOffset = range pc.offsetMgr.CommittedOffsets() {
	}
	if pc.committedOffset != pc.submittedOffset {
		log.Errorf("<%s> failed to commit offset: %d, sparseAcks=%s",
			pc.actorID, pc.submittedOffset.Val, offsettrk.SparseAcks2Str(pc.submittedOffset))
	}
	log.Infof("<%s> last committed offset: %d, sparseAcks=%s",
		pc.actorID, pc.committedOffset.Val, offsettrk.SparseAcks2Str(pc.committedOffset))
}

// notifyTestInitialized sends initial offset to initialOffsetCh channel.
func (pc *T) notifyTestInitialized(initialOffset offsetmgr.Offset) {
	if initialOffsetCh != nil {
		initialOffsetCh <- initialOffset
	}
}

// notifyTestFetched sends a signal to FirstMessageFetchedCh channel the
// first time it is called.
func (pc *T) notifyTestFetched() {
	if !pc.firstMsgFetched && FirstMessageFetchedCh != nil {
		pc.firstMsgFetched = true
		FirstMessageFetchedCh <- pc
	}
}
