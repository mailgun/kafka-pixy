package partitioncsm

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgistream"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
)

var (
	// TESTING ONLY!: If this channel is not `nil` then partition consumers
	// will use it to notify when they fetch the very first message.
	FirstMessageFetchedCh chan *T
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
	msgIStreamF msgistream.Factory
	offsetMgrF  offsetmgr.Factory
	messagesCh  chan *consumer.Message
	offersCh    chan *consumer.Message
	stopCh      chan none.T
	wg          sync.WaitGroup
}

// Spawn creates a partition consumer instance and starts its goroutines.
func Spawn(namespace *actor.ID, group, topic string, partition int32, cfg *config.Proxy,
	groupMember *groupmember.T, msgIStreamF msgistream.Factory, offsetMgrF offsetmgr.Factory,
) *T {
	pc := &T{
		actorID:     namespace.NewChild(fmt.Sprintf("P:%s_%d", topic, partition)),
		cfg:         cfg,
		group:       group,
		topic:       topic,
		partition:   partition,
		groupMember: groupMember,
		msgIStreamF: msgIStreamF,
		offsetMgrF:  offsetMgrF,
		messagesCh:  make(chan *consumer.Message),
		offersCh:    make(chan *consumer.Message),
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
func (pc *T) Messages() <-chan *consumer.Message {
	return pc.messagesCh
}

// implements `multiplexer.In`
func (pc *T) Offers() chan<- *consumer.Message {
	return pc.offersCh
}

func (pc *T) run() {
	defer pc.groupMember.ClaimPartition(pc.actorID, pc.topic, pc.partition, pc.stopCh)()

	om, err := pc.offsetMgrF.SpawnOffsetManager(pc.actorID, pc.group, pc.topic, pc.partition)
	if err != nil {
		// Must never happen.
		log.Errorf("<%s> failed to spawn offset manager: err=(%s)", pc.actorID, err)
		return
	}
	defer func() {
		if om != nil {
			om.Stop()
		}
	}()

	// Wait for the initial offset to be retrieved.
	var committedOffset offsetmgr.Offset
	select {
	case committedOffset = <-om.InitialOffset():
	case <-pc.stopCh:
		return
	}
	submittedOffset := committedOffset

	// Initialize the message input stream from the initial offset.
	mis, realOffsetVal, err := pc.msgIStreamF.SpawnMessageIStream(pc.actorID, pc.topic, pc.partition, committedOffset.Val)
	if err != nil {
		// Must never happen.
		log.Errorf("<%s> failed to start message stream: offset=%d, err=(%s)", pc.actorID, committedOffset.Val, err)
		return
	}
	defer mis.Stop()

	// If the real initial offset is not what had been committed then adjust.
	if committedOffset.Val != realOffsetVal {
		log.Errorf("<%s> invalid initial offset: stored=%+v, adjusted=%+v",
			pc.actorID, committedOffset.Val, realOffsetVal)
		submittedOffset = offsetmgr.Offset{Val: realOffsetVal, Meta: ""}
		om.SubmitOffset(submittedOffset)
	} else {
		log.Infof("<%s> initialized: offset=%+v", pc.actorID, submittedOffset)
	}

	nilOrIStreamMessagesCh := mis.Messages()
	var nilOrMessagesCh chan *consumer.Message
	var msg *consumer.Message
	firstMessageFetched := false
	for {
		select {
		case msg = <-nilOrIStreamMessagesCh:
			nilOrIStreamMessagesCh = nil
			nilOrMessagesCh = pc.messagesCh
			// Notify tests when the very first message is fetched.
			if !firstMessageFetched && FirstMessageFetchedCh != nil {
				firstMessageFetched = true
				FirstMessageFetchedCh <- pc
			}
		case nilOrMessagesCh <- msg:
			nilOrIStreamMessagesCh = mis.Messages()
			nilOrMessagesCh = nil
		case ack := <-pc.offersCh:
			submittedOffset = offsetmgr.Offset{Val: ack.Offset + 1, Meta: ""}
			om.SubmitOffset(submittedOffset)
		case committedOffset = <-om.CommittedOffsets():
		case <-pc.stopCh:
			goto done
		}
	}
done:
	om.Stop()
	// Drain committed offsets.
	for committedOffset = range om.CommittedOffsets() {
	}
	// Reset `om` to prevent the deferred panic offset manager cleanup function
	// from running and calling `Stop()` on the already stopped offset manager.
	om = nil
	if committedOffset != submittedOffset {
		log.Errorf("<%s> failed to commit offset: submitted=%+v, committed=%+v",
			pc.actorID, submittedOffset, committedOffset)
		return
	}
	log.Infof("<%s> last committed offset: %+v", pc.actorID, committedOffset)
}

func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}
