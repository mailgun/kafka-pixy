package proxy

import (
	"fmt"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/consumerimpl"
	"github.com/mailgun/kafka-pixy/producer"
)

// T implements a proxy to a particular Kafka/ZooKeeper cluster.
type T struct {
	actorID *actor.ID
	cfg     *config.Proxy
	prod    *producer.T
	cons    consumer.T
	adm     *admin.T
}

// Spawn creates a proxy instance and starts its internal goroutines.
func Spawn(namespace *actor.ID, name string, cfg *config.Proxy) (*T, error) {
	p := T{
		actorID: namespace.NewChild(name),
		cfg:     cfg,
	}
	var err error

	if p.prod, err = producer.Spawn(p.actorID, cfg); err != nil {
		return nil, fmt.Errorf("failed to spawn producer, err=(%s)", err)
	}
	if p.cons, err = consumerimpl.Spawn(p.actorID, cfg); err != nil {
		return nil, fmt.Errorf("failed to spawn consumer, err=(%s)", err)
	}
	if p.adm, err = admin.Spawn(p.actorID, cfg); err != nil {
		return nil, fmt.Errorf("failed to spawn admin, err=(%s)", err)
	}
	return &p, nil
}

// Stop terminates the proxy instances synchronously.
func (p *T) Stop() {
	var wg sync.WaitGroup
	if p.prod != nil {
		actor.Spawn(p.actorID.NewChild("producer_stop"), &wg, p.prod.Stop)
	}
	if p.cons != nil {
		actor.Spawn(p.actorID.NewChild("consumer_stop"), &wg, p.cons.Stop)
	}
	if p.adm != nil {
		actor.Spawn(p.actorID.NewChild("admin_stop"), &wg, p.adm.Stop)
	}
	wg.Wait()
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
	return p.prod.Produce(topic, key, message)
}

// AsyncProduce is an asynchronously counterpart of the `Produce` function.
// Errors are silently ignored.
func (p *T) AsyncProduce(topic string, key, message sarama.Encoder) {
	p.prod.AsyncProduce(topic, key, message)
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
func (p *T) Consume(group, topic string) (*consumer.Message, error) {
	return p.cons.Consume(group, topic)
}

// GetGroupOffsets for every partition of the specified topic it returns the
// current offset range along with the latest offset and metadata committed by
// the specified consumer group.
func (p *T) GetGroupOffsets(group, topic string) ([]admin.PartitionOffset, error) {
	return p.adm.GetGroupOffsets(group, topic)
}

// SetGroupOffsets commits specific offset values along with metadata for a list
// of partitions of a particular topic on behalf of the specified group.
func (p *T) SetGroupOffsets(group, topic string, offsets []admin.PartitionOffset) error {
	return p.adm.SetGroupOffsets(group, topic, offsets)
}

// GetTopicConsumers returns client-id -> consumed-partitions-list mapping
// for a clients from a particular consumer group and a particular topic.
func (p *T) GetTopicConsumers(group, topic string) (map[string][]int32, error) {
	return p.adm.GetTopicConsumers(group, topic)
}

// GetAllTopicConsumers returns group -> client-id -> consumed-partitions-list
// mapping for a particular topic. Warning, the function performs scan of all
// consumer groups registered in ZooKeeper and therefore can take a lot of time.
func (p *T) GetAllTopicConsumers(topic string) (map[string]map[string][]int32, error) {
	return p.adm.GetAllTopicConsumers(topic)
}
