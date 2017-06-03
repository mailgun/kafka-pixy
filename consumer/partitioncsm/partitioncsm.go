package partitioncsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/offsettrac"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

var (
	// TESTING ONLY!: If this channel is not `nil` then partition consumers
	// will use it to notify when they fetch the very first message.
	FirstMessageFetchedCh chan *T
	initialOffsetCh       chan offsetmgr.Offset

	// Following variables are supposed to be constants but they were defined
	// as variables to allow overriding in tests:
	check4RetryInterval   = time.Second
	retriesHighWaterMark  = 1
	retriesEmergencyBreak = 3 * retriesHighWaterMark
	offeredHighWaterMark  = 100
)

// exclusiveConsumer ensures exclusive consumption of messages from a topic
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

	om, err := pc.offsetMgrF.SpawnOffsetManager(pc.actorID, pc.group, pc.topic, pc.partition)
	if err != nil {
		// Must never happen!
		panic(errors.Wrapf(err, "<%s> failed to spawn offset manager", pc.actorID))
	}
	defer func() {
		if om != nil {
			om.Stop()
		}
	}()

	// Wait for the initial offset to be retrieved.
	var committedOffset offsetmgr.Offset
	select {
	case committedOffset = <-om.CommittedOffsets():
	case <-pc.stopCh:
		return
	}
	submittedOffset := committedOffset

	// Initialize a message fetcher to read from the initial offset.
	mf, realOffsetVal, err := pc.msgFetcherF.SpawnMsgFetcher(pc.actorID, pc.topic, pc.partition, committedOffset.Val)
	if err != nil {
		// Must never happen!
		panic(errors.Wrapf(err, "<%s> failed to start message stream, offset=%d", pc.actorID, committedOffset.Val))
	}
	defer mf.Stop()

	// If the real initial offset is not what had been committed then adjust.
	if committedOffset.Val != realOffsetVal {
		log.Errorf("<%s> invalid initial offset: %d, sparseAcks=%s",
			pc.actorID, committedOffset.Val, offsettrac.SparseAcks2Str(committedOffset))
		submittedOffset = offsetmgr.Offset{Val: realOffsetVal, Meta: ""}
		om.SubmitOffset(submittedOffset)
	}
	log.Infof("<%s> initialized: offset=%d, sparseAcks=%s",
		pc.actorID, submittedOffset.Val, offsettrac.SparseAcks2Str(submittedOffset))
	pc.notifyTestInitialized(submittedOffset)
	ot := offsettrac.New(pc.actorID, submittedOffset, pc.cfg.Consumer.AckTimeout)

	var (
		nilOrIStreamMessagesCh = mf.Messages()
		nilOrMessagesCh        chan consumer.Message
		retryTicker            = time.NewTicker(check4RetryInterval)
		msg                    consumer.Message
		msgOk                  = false
		retryNo                int
	)
	defer retryTicker.Stop()
	for {
		select {
		case msg = <-nilOrIStreamMessagesCh:
			if ok, _ := ot.IsAcked(msg.Offset); ok {
				continue
			}
			msg.EventsCh = pc.eventsCh
			msgOk = true
			pc.notifyTestFetched()
			nilOrIStreamMessagesCh = nil
			nilOrMessagesCh = pc.messagesCh
		case <-retryTicker.C:
			if msgOk {
				continue
			}
			msg, retryNo, msgOk = ot.NextRetry()
			if !msgOk {
				continue
			}
			if retryNo > retriesEmergencyBreak {
				log.Errorf("<%s> too many retries: offset=%d", pc.actorID, msg.Offset)
				goto wait4Ack
			}
			if retryNo > retriesHighWaterMark {
				log.Warningf("<%s> retries above HWM: retryNo=%d, offset=%d", pc.actorID, retryNo, msg.Offset)
			}
			nilOrIStreamMessagesCh = nil
			nilOrMessagesCh = pc.messagesCh
		case nilOrMessagesCh <- msg:
			nilOrMessagesCh = nil
		case event := <-pc.eventsCh:
			switch event.T {
			case consumer.EvOffered:
				if event.Offset != msg.Offset {
					// Must never happen!
					panic(errors.Wrapf(err, "<%s> invalid offer offset %d, want=%d", pc.actorID, event.Offset, msg.Offset))
				}
				offeredCount := ot.OnOffered(msg)
				msg, retryNo, msgOk = ot.NextRetry()
				if msgOk {
					log.Warningf("<%s> retrying: offset=%d, no=%d", pc.actorID, msg.Offset, retryNo)
					if retryNo > retriesEmergencyBreak {
						log.Errorf("<%s> too many retries: offset=%d", pc.actorID, msg.Offset)
						goto wait4Ack
					}
					if retryNo > retriesHighWaterMark {
						log.Warningf("<%s> retries above HWM: %d, offset=%d", pc.actorID, retryNo, msg.Offset)
					}
					nilOrMessagesCh = pc.messagesCh
					continue
				}
				if offeredCount > offeredHighWaterMark {
					log.Warningf("<%s> offered count above HWM: %d", pc.actorID, offeredCount)
					nilOrIStreamMessagesCh = nil
				} else {
					nilOrIStreamMessagesCh = mf.Messages()
				}
			case consumer.EvAcked:
				var offeredCount int
				submittedOffset, offeredCount = ot.OnAcked(event.Offset)
				om.SubmitOffset(submittedOffset)
				if !msgOk && offeredCount <= offeredHighWaterMark {
					nilOrIStreamMessagesCh = mf.Messages()
				}
			}
		case committedOffset = <-om.CommittedOffsets():
		case <-pc.stopCh:
			goto wait4Ack
		}
	}
wait4Ack:
	for ok, timeout := ot.ShouldWait4Ack(); ok; ok, timeout = ot.ShouldWait4Ack() {
		select {
		case event := <-pc.eventsCh:
			if event.T == consumer.EvAcked {
				submittedOffset, _ = ot.OnAcked(event.Offset)
				om.SubmitOffset(submittedOffset)
			}
		case <-time.After(timeout):
			continue
		}
	}
	om.Stop()
	// Drain committed offsets.
	for committedOffset = range om.CommittedOffsets() {
	}
	// Reset `om` to prevent the deferred panic offset manager cleanup function
	// from running and calling `Stop()` on the already stopped offset manager.
	om = nil
	if committedOffset != submittedOffset {
		log.Errorf("<%s> failed to commit offset: %d, sparseAcks=%s",
			pc.actorID, submittedOffset.Val, offsettrac.SparseAcks2Str(submittedOffset))
	}
	log.Infof("<%s> last committed offset: %d, sparceAcks=%s",
		pc.actorID, committedOffset.Val, offsettrac.SparseAcks2Str(committedOffset))
}

func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
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

func resetConstants() {
	check4RetryInterval = time.Second
	retriesHighWaterMark = 1
	retriesEmergencyBreak = 3 * retriesHighWaterMark
	offeredHighWaterMark = 100
}
