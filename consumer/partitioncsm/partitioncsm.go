package partitioncsm

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgstream"
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
	actorID          *actor.ID
	cfg              *config.Proxy
	group            string
	topic            string
	partition        int32
	groupMember      *groupmember.T
	msgStreamFactory msgstream.Factory
	offsetMgrFactory offsetmgr.Factory
	messagesCh       chan *consumer.Message
	acksCh           chan *consumer.Message
	stopCh           chan none.T
	wg               sync.WaitGroup
}

// Spawn creates a partition consumer instance and starts its goroutines.
func Spawn(namespace *actor.ID, group, topic string, partition int32, cfg *config.Proxy,
	groupMember *groupmember.T, msgStreamFactory msgstream.Factory, offsetMgrFactory offsetmgr.Factory,
) *T {
	pc := &T{
		actorID:          namespace.NewChild(fmt.Sprintf("P:%s_%d", topic, partition)),
		cfg:              cfg,
		group:            group,
		topic:            topic,
		partition:        partition,
		groupMember:      groupMember,
		msgStreamFactory: msgStreamFactory,
		offsetMgrFactory: offsetMgrFactory,
		messagesCh:       make(chan *consumer.Message),
		acksCh:           make(chan *consumer.Message),
		stopCh:           make(chan none.T),
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
func (pc *T) Acks() chan<- *consumer.Message {
	return pc.acksCh
}

func (pc *T) run() {
	defer pc.groupMember.ClaimPartition(pc.actorID, pc.topic, pc.partition, pc.stopCh)()

	om, err := pc.offsetMgrFactory.SpawnOffsetManager(pc.actorID, pc.group, pc.topic, pc.partition)
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
	var initialOffset offsetmgr.DecoratedOffset
	select {
	case initialOffset = <-om.InitialOffset():
	case <-pc.stopCh:
		return
	}

	ms, concreteOffset, err := pc.msgStreamFactory.SpawnMessageStream(pc.actorID, pc.topic, pc.partition, initialOffset.Offset)
	if err != nil {
		// Must never happen.
		log.Errorf("<%s> failed to start message stream: offset=%d, err=(%s)", pc.actorID, initialOffset.Offset, err)
		return
	}
	defer ms.Stop()
	if initialOffset.Offset != concreteOffset {
		log.Errorf("<%s> invalid initial offset: stored=%d, adjusted=%d",
			pc.actorID, initialOffset.Offset, concreteOffset)
	}
	log.Infof("<%s> initialized: offset=%d", pc.actorID, concreteOffset)

	// Initialize the Kafka offset storage for a group on first consumption.
	if initialOffset.Offset == sarama.OffsetNewest {
		om.SubmitOffset(concreteOffset, "")
	}
	lastSubmittedOffset := concreteOffset
	lastCommittedOffset := concreteOffset

	firstMessageFetched := false
	for {
		var msg *consumer.Message
		// Wait for a fetched message to be provided by the message stream.
		for {
			select {
			case msg = <-ms.Messages():
				// Notify tests when the very first message is fetched.
				if !firstMessageFetched && FirstMessageFetchedCh != nil {
					firstMessageFetched = true
					FirstMessageFetchedCh <- pc
				}
				goto offerAndAck
			case committedOffset := <-om.CommittedOffsets():
				lastCommittedOffset = committedOffset.Offset
				continue
			case <-pc.stopCh:
				goto done
			}
		}
	offerAndAck:
		// Offer the fetched message to the upstream consumer and wait for it
		// to be acknowledged.
		for {
			select {
			case pc.messagesCh <- msg:
			// Keep offering the same message until it is acknowledged.
			case <-pc.acksCh:
				lastSubmittedOffset = msg.Offset + 1
				om.SubmitOffset(lastSubmittedOffset, "")
				break offerAndAck
			case committedOffset := <-om.CommittedOffsets():
				lastCommittedOffset = committedOffset.Offset
				continue
			case <-pc.stopCh:
				goto done
			}
		}
	}
done:
	om.Stop()
	// Drain committed offsets.
	for committedOffset := range om.CommittedOffsets() {
		lastCommittedOffset = committedOffset.Offset
	}
	// Reset `om` to prevent the deferred panic offset manager cleanup function
	// from running and calling `Stop()` on the already stopped offset manager.
	om = nil
	if lastCommittedOffset != lastSubmittedOffset {
		log.Errorf("<%s> failed to commit offset: submitted=%d, committed=%d",
			pc.actorID, lastSubmittedOffset, lastCommittedOffset)
		return
	}
	log.Infof("<%s> last committed offset: %d", pc.actorID, lastCommittedOffset)
}

func (pc *T) Stop() {
	close(pc.stopCh)
	pc.wg.Wait()
}
