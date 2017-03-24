package consumerimpl

import (
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/consumer/groupcsm"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
	"github.com/wvanbergen/kazoo-go"
)

// T is a Kafka consumer implementation that automatically maintains consumer
// groups registrations and topic subscriptions. Whenever a message from a
// particular topic is consumed by a particular consumer group T checks if it
// has registered with the consumer group, and registers otherwise. Then it
// checks if it has subscribed for the topic, and subscribes otherwise. Later
// if a particular topic has not been consumed for
// `Config.Consumer.RegistrationTimeout` period of time, the consumer
// unsubscribes from the topic, likewise if a consumer group has not seen any
// requests for that period then the consumer deregisters from the group.
//
// implements `consumer.T`.
// implements `dispatcher.Factory`.
type t struct {
	namespace  *actor.ID
	cfg        *config.Proxy
	dispatcher *dispatcher.T
	kafkaClt   sarama.Client
	kazooClt   *kazoo.Kazoo
	offsetMgrF offsetmgr.Factory
}

// Spawn creates a consumer instance with the specified configuration and
// starts all its goroutines.
func Spawn(namespace *actor.ID, cfg *config.Proxy, offsetMgrF offsetmgr.Factory) (*t, error) {
	namespace = namespace.NewChild("cons")

	kafkaClt, err := sarama.NewClient(cfg.Kafka.SeedPeers, cfg.SaramaClientCfg())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client for message streams")
	}

	kazooClt, err := kazoo.NewKazoo(cfg.ZooKeeper.SeedPeers, cfg.KazooCfg())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kazoo.Kazoo")
	}

	c := &t{
		namespace:  namespace,
		cfg:        cfg,
		kafkaClt:   kafkaClt,
		offsetMgrF: offsetMgrF,
		kazooClt:   kazooClt,
	}
	c.dispatcher = dispatcher.New(c.namespace, c, c.cfg)
	c.dispatcher.Start()
	return c, nil
}

// implements `consumer.T`
func (c *t) Consume(group, topic string) (consumer.Message, error) {
	replyCh := make(chan dispatcher.Response, 1)
	c.dispatcher.Requests() <- dispatcher.Request{time.Now().UTC(), group, topic, replyCh}
	result := <-replyCh
	return result.Msg, result.Err
}

// implements `consumer.T`
func (c *t) Stop() {
	c.dispatcher.Stop()
	c.kazooClt.Close()
	c.kafkaClt.Close()
}

// implements `dispatcher.Factory`.
func (c *t) KeyOf(req dispatcher.Request) string {
	return req.Group
}

// implements `dispatcher.Factory`.
func (c *t) NewTier(key string) dispatcher.Tier {
	return groupcsm.New(c.namespace, key, c.cfg, c.kafkaClt, c.kazooClt, c.offsetMgrF)
}

// String returns a string ID of this instance to be used in logs.
func (sc *t) String() string {
	return sc.namespace.String()
}
