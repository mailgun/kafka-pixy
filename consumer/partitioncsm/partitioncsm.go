package partitioncsm

import (
	"encoding/base64"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/consumer/subscriber"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
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
	actDesc     *actor.Descriptor
	cfg         *config.Proxy
	group       string
	topic       string
	partition   int32
	groupMember *subscriber.T
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
	offerCount      int32

	// For tests only!
	firstMsgFetched bool
}

// Spawn creates a partition consumer instance and starts its goroutines.
func Spawn(parentActDesc *actor.Descriptor, group, topic string, partition int32, cfg *config.Proxy,
	groupMember *subscriber.T, msgFetcherF msgfetcher.Factory, offsetMgrF offsetmgr.Factory,
) *T {
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s.p%d", topic, partition))
	actDesc.AddLogField("kafka.group", group)
	actDesc.AddLogField("kafka.topic", topic)
	actDesc.AddLogField("kafka.partition", partition)
	pc := &T{
		actDesc:     actDesc,
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
	actor.Spawn(pc.actDesc, &pc.wg, pc.run)
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

// implements `multiplexer.In`
func (pc *T) IsSafe2Stop() bool {
	return atomic.LoadInt32(&pc.offerCount) == 0
}

// implements `multiplexer.In`
func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}

func (pc *T) run() {
	defer close(pc.messagesCh)
	defer pc.groupMember.ClaimPartition(pc.actDesc, pc.topic, pc.partition, pc.stopCh)()

	var err error
	if pc.offsetMgr, err = pc.offsetMgrF.Spawn(pc.actDesc, pc.group, pc.topic, pc.partition); err != nil {
		panic(errors.Wrapf(err, "<%s> must never happen", pc.actDesc))
	}
	defer pc.stopOffsetMgr()

	// Wait for the initial offset to be retrieved or a stop signal.
	select {
	case pc.committedOffset = <-pc.offsetMgr.CommittedOffsets():
	case <-pc.stopCh:
		return
	}
	pc.actDesc.Log().Infof("Initial offset: %s", offsetRepr(pc.committedOffset))
	pc.offsetTrk = offsettrk.New(pc.actDesc, pc.committedOffset, pc.cfg.Consumer.AckTimeout)
	pc.submittedOffset = pc.committedOffset
	pc.offsetsOk = true
	pc.notifyTestInitialized(pc.committedOffset)

	// Run a fetch loop until the partition consumer is signalled to stop.
	for pc.runFetchLoop() {
	}

	// Wait for clients to acknowledge pending offers.
	for timeout := pc.offsetTrk.ShouldWait4Ack(); timeout > 0; timeout = pc.offsetTrk.ShouldWait4Ack() {
		select {
		case event := <-pc.eventsCh:
			if event.T == consumer.EvAcked {
				var offerCount int
				pc.submittedOffset, offerCount = pc.offsetTrk.OnAcked(event.Offset)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
			}
		case <-time.After(timeout):
			continue
		}
	}
}

func (pc *T) runFetchLoop() bool {
	// Initialize a message fetcher to read from the initial offset.
	mf, realOffsetVal, err := pc.msgFetcherF.Spawn(pc.actDesc, pc.topic, pc.partition, pc.submittedOffset.Val)
	if err != nil {
		pc.actDesc.Log().WithError(err).Error("Failed to spawn fetcher")
		return true
	}
	defer mf.Stop()

	var offerCount int
	pc.submittedOffset, offerCount = pc.offsetTrk.Adjust(realOffsetVal)
	atomic.StoreInt32(&pc.offerCount, int32(offerCount))

	// If the real offset is different from the committed one then submit it
	// and report in the logs.
	if pc.submittedOffset != pc.committedOffset {
		pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		pc.actDesc.Log().Errorf("Adjusted offset: new=%s, old=%s",
			offsetRepr(pc.submittedOffset), offsetRepr(pc.committedOffset))
	}
	var (
		nilOrMsgInCh  = mf.Messages()
		nilOrMsgOutCh chan consumer.Message
		retryTicker   = time.NewTicker(check4RetryInterval)
		msg           consumer.Message
		msgOk         bool
	)
	defer retryTicker.Stop()
	for {
		select {
		case msg, msgOk = <-nilOrMsgInCh:
			// If the fetcher terminated due to failure, then quit the fetch
			// loop signaling that it needs to be reinitialized.
			if !msgOk {
				return true
			}
			// If a fetched message has already been acked, then skip it.
			if ok, _ := pc.offsetTrk.IsAcked(msg.Offset); ok {
				msgOk = false
				continue
			}
			msg.EventsCh = pc.eventsCh
			pc.notifyTestFetched()
			nilOrMsgOutCh = pc.messagesCh
			// Stop fetching messages until this one is offered to a client.
			nilOrMsgInCh = nil

		case <-retryTicker.C:
			if msgOk {
				continue
			}
			if msg, msgOk = pc.nextRetry(); msgOk {
				nilOrMsgInCh = nil
				nilOrMsgOutCh = pc.messagesCh
			}
		case nilOrMsgOutCh <- msg:
			nilOrMsgOutCh = nil

		case event := <-pc.eventsCh:
			switch event.T {
			case consumer.EvOffered:
				if !msgOk || event.Offset != msg.Offset {
					pc.actDesc.Log().Errorf("Invalid offer offset %d, want=%d", event.Offset, msg.Offset)
					continue
				}
				offerCount = pc.offsetTrk.OnOffered(msg)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				if msg, msgOk = pc.nextRetry(); msgOk {
					nilOrMsgOutCh = pc.messagesCh
					continue
				}
				if offerCount > pc.cfg.Consumer.MaxPendingMessages {
					pc.actDesc.Log().Warnf("Offer count above HWM: %d", offerCount)
					nilOrMsgInCh = nil
					continue
				}
				nilOrMsgInCh = mf.Messages()

			case consumer.EvAcked:
				pc.submittedOffset, offerCount = pc.offsetTrk.OnAcked(event.Offset)
				atomic.StoreInt32(&pc.offerCount, int32(offerCount))
				pc.offsetMgr.SubmitOffset(pc.submittedOffset)
				if !msgOk && offerCount <= pc.cfg.Consumer.MaxPendingMessages {
					nilOrMsgInCh = mf.Messages()
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
		pc.actDesc.Log().Errorf("Too many retries: retryNo=%d, offset=%d, key=%s, msg=%s",
			retryNo, msg.Offset, string(msg.Key), base64.StdEncoding.EncodeToString(msg.Value))
		pc.submittedOffset, _ = pc.offsetTrk.OnAcked(msg.Offset)
		pc.offsetMgr.SubmitOffset(pc.submittedOffset)
		// TODO: Dump expired messages to a long term storage?
		msg, retryNo, ok = pc.offsetTrk.NextRetry()
	}
	if ok {
		pc.actDesc.Log().Warnf("Retrying: retryNo=%d, offset=%d, key=%s",
			retryNo, msg.Offset, string(msg.Key))
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
		pc.actDesc.Log().Errorf("Failed to commit offset: %s", offsetRepr(pc.submittedOffset))
	}
	pc.actDesc.Log().Infof("Last committed offset: %s", offsetRepr(pc.committedOffset))
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

func offsetRepr(offset offsetmgr.Offset) string {
	return fmt.Sprintf("%d(%s)", offset.Val, offsettrk.SparseAcks2Str(offset))
}
