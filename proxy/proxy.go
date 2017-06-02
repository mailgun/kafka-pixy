package proxy

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/consumerimpl"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/producer"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

const (
	initEventsChMapCapacity = 256
)

var (
	noAck   = Ack{partition: -1}
	autoAck = Ack{partition: -2}
)

// T implements a proxy to a particular Kafka/ZooKeeper cluster.
type T struct {
	actorID    *actor.ID
	cfg        *config.Proxy
	producer   *producer.T
	kafkaClt   sarama.Client
	offsetMgrF offsetmgr.Factory
	consumer   consumer.T
	admin      *admin.T

	// FIXME: We never remove stale elements from eventsChMap. It is sort of ok
	// FIXME: since the number of group/topic/partition combinations is fairly
	// FIXME: limited and should not cause any significant system memory usage.
	eventsChMapMu sync.RWMutex
	eventsChMap   map[eventsChID]chan<- consumer.Event
}

type Ack struct {
	partition int32
	offset    int64
}

// NewAck creates an acknowledgement instance from a partition and an offset.
// Note that group and topic are not included. Respective values that are
// passed to proxy.Consume function along with the ack are gonna be used.
func NewAck(partition int32, offset int64) (Ack, error) {
	if partition < 0 {
		return Ack{}, errors.Errorf("bad partition: %d", partition)
	}
	if offset < 0 {
		return Ack{}, errors.Errorf("bad offset: %d", offset)
	}
	return Ack{partition, offset}, nil
}

// NoAck returns an ack value that should be passed to proxy.Consume function
// when a caller does not want to acknowledge anything.
func NoAck() Ack {
	return noAck
}

// AutoAck returns an ack value that should be passed to proxy.Consume function
// when a caller wants the consumed message to be acknowledged immediately.
func AutoAck() Ack {
	return autoAck
}

type eventsChID struct {
	group     string
	topic     string
	partition int32
}

// Spawn creates a proxy instance and starts its internal goroutines.
func Spawn(namespace *actor.ID, name string, cfg *config.Proxy) (*T, error) {
	p := T{
		actorID:     namespace.NewChild(name),
		cfg:         cfg,
		eventsChMap: make(map[eventsChID]chan<- consumer.Event, initEventsChMapCapacity),
	}
	var err error

	if p.kafkaClt, err = sarama.NewClient(cfg.Kafka.SeedPeers, cfg.SaramaClientCfg()); err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client")
	}
	p.offsetMgrF = offsetmgr.SpawnFactory(p.actorID, cfg, p.kafkaClt)
	if p.producer, err = producer.Spawn(p.actorID, cfg); err != nil {
		return nil, errors.Wrap(err, "failed to spawn producer")
	}
	if p.consumer, err = consumerimpl.Spawn(p.actorID, cfg, p.offsetMgrF); err != nil {
		return nil, errors.Wrap(err, "failed to spawn consumer")
	}
	if p.admin, err = admin.Spawn(p.actorID, cfg); err != nil {
		return nil, errors.Wrap(err, "failed to spawn admin")
	}
	return &p, nil
}

// Stop terminates the proxy instances synchronously.
func (p *T) Stop() {
	var wg sync.WaitGroup
	if p.producer != nil {
		actor.Spawn(p.actorID.NewChild("producer_stop"), &wg, p.producer.Stop)
	}
	if p.consumer != nil {
		actor.Spawn(p.actorID.NewChild("consumer_stop"), &wg, p.consumer.Stop)
	}
	if p.admin != nil {
		actor.Spawn(p.actorID.NewChild("admin_stop"), &wg, p.admin.Stop)
	}
	wg.Wait()
	if p.offsetMgrF != nil {
		p.offsetMgrF.Stop()
	}
	if p.kafkaClt != nil {
		p.kafkaClt.Close()
	}
}

// Produce submits a message to the specified `topic` of the Kafka cluster
// using `key` to identify a destination partition. The exact algorithm used to
// map keys to partitions is implementation specific but it is guaranteed that
// it returns consistent results. If `key` is `nil`, then the message is placed
// into a random partition.
//
// Errors usually indicate a catastrophic failure of the Kafka cluster, or
// missing topic if there cluster is not configured to auto create topics.
func (p *T) Produce(topic string, key, message sarama.Encoder) (*sarama.ProducerMessage, error) {
	return p.producer.Produce(topic, key, message)
}

// AsyncProduce is an asynchronously counterpart of the `Produce` function.
// Errors are silently ignored.
func (p *T) AsyncProduce(topic string, key, message sarama.Encoder) {
	p.producer.AsyncProduce(topic, key, message)
}

// Consume consumes a message from the specified topic on behalf of the
// specified consumer group. If there are no more new messages in the topic
// at the time of the request then it will block for
// `Config.Consumer.LongPollingTimeout`. If no new message is produced during
// that time, then `ErrRequestTimeout` is returned.
//
// Note that during state transitions topic subscribe<->unsubscribe and
// consumer group register<->deregister the method may return either
// `ErrBufferOverflow` or `ErrRequestTimeout` even when there are messages
// available for consumption. In that case the user should back off a bit
// and then repeat the request.
func (p *T) Consume(group, topic string, ack Ack) (consumer.Message, error) {
	if ack != noAck && ack != autoAck {
		p.eventsChMapMu.RLock()
		eventsChID := eventsChID{group, topic, ack.partition}
		eventsCh, ok := p.eventsChMap[eventsChID]
		p.eventsChMapMu.RUnlock()
		if ok {
			go func() {
				select {
				case eventsCh <- consumer.Ack(ack.offset):
				case <-time.After(p.cfg.Consumer.LongPollingTimeout):
					log.Errorf("<%s> ack timeout: partition=%d, offset=%d",
						p.actorID, ack.partition, ack.offset)
				}
			}()
		}
	}
	msg, err := p.consumer.Consume(group, topic)
	if err != nil {
		return consumer.Message{}, err
	}

	eventsChID := eventsChID{group, topic, msg.Partition}
	p.eventsChMapMu.Lock()
	p.eventsChMap[eventsChID] = msg.EventsCh
	p.eventsChMapMu.Unlock()

	if ack == autoAck {
		msg.EventsCh <- consumer.Ack(msg.Offset)
	}
	return msg, nil
}

func (p *T) Ack(group, topic string, ack Ack) error {
	eventsChID := eventsChID{group, topic, ack.partition}
	p.eventsChMapMu.RLock()
	eventsCh, ok := p.eventsChMap[eventsChID]
	p.eventsChMapMu.RUnlock()
	if !ok {
		return errors.New("acks channel missing")
	}
	select {
	case eventsCh <- consumer.Ack(ack.offset):
	case <-time.After(p.cfg.Consumer.LongPollingTimeout):
		return errors.New("ack timeout")
	}
	return nil
}

// GetGroupOffsets for every partition of the specified topic it returns the
// current offset range along with the latest offset and metadata committed by
// the specified consumer group.
func (p *T) GetGroupOffsets(group, topic string) ([]admin.PartitionOffset, error) {
	return p.admin.GetGroupOffsets(group, topic)
}

// SetGroupOffsets commits specific offset values along with metadata for a list
// of partitions of a particular topic on behalf of the specified group.
func (p *T) SetGroupOffsets(group, topic string, offsets []admin.PartitionOffset) error {
	return p.admin.SetGroupOffsets(group, topic, offsets)
}

// GetTopicConsumers returns client-id -> consumed-partitions-list mapping
// for a clients from a particular consumer group and a particular topic.
func (p *T) GetTopicConsumers(group, topic string) (map[string][]int32, error) {
	return p.admin.GetTopicConsumers(group, topic)
}

// GetAllTopicConsumers returns group -> client-id -> consumed-partitions-list
// mapping for a particular topic. Warning, the function performs scan of all
// consumer groups registered in ZooKeeper and therefore can take a lot of time.
func (p *T) GetAllTopicConsumers(topic string) (map[string]map[string][]int32, error) {
	return p.admin.GetAllTopicConsumers(topic)
}
