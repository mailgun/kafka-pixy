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
	"github.com/samuel/go-zookeeper/zk"
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
	actDesc    *actor.Descriptor
	cfg        *config.Proxy
	dispatcher *dispatcher.T
	kafkaClt   sarama.Client
	zkConn     *zk.Conn
	offsetMgrF offsetmgr.Factory
}

// Spawn creates a consumer instance with the specified configuration and
// starts all its goroutines.
func Spawn(parentActDesc *actor.Descriptor, cfg *config.Proxy, offsetMgrF offsetmgr.Factory) (*t, error) {
	kafkaClt, err := sarama.NewClient(cfg.Kafka.SeedPeers, cfg.SaramaClientCfg())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Kafka client for message streams")
	}

	// ZooKeeper documentation says following about the session timeout: "The
	// current (ZooKeeper) implementation requires that the timeout be a
	// minimum of 2 times the tickTime (as set in the server configuration) and
	// a maximum of 20 times the tickTime". The default tickTime is 2 seconds.
	// See http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkSessions
	sessionTimeout := 15 * time.Second
	zkConn, _, err := zk.Connect(cfg.ZooKeeper.SeedPeers, sessionTimeout)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create kazoo.Kazoo")
	}

	c := &t{
		actDesc:    parentActDesc.NewChild("cons"),
		cfg:        cfg,
		kafkaClt:   kafkaClt,
		offsetMgrF: offsetMgrF,
		zkConn:     zkConn,
	}
	c.dispatcher = dispatcher.Spawn(c.actDesc, c, c.cfg)
	return c, nil
}

// implements `consumer.T`
func (c *t) Consume(group, topic string) (consumer.Message, error) {
	rs := <-c.AsyncConsume(group, topic)
	return rs.Msg, rs.Err
}

// implements `consumer.T`
func (c *t) AsyncConsume(group, topic string) <-chan consumer.Response {
	rq := consumer.NewRequest(group, topic)
	c.dispatcher.Requests() <- rq
	return rq.ResponseCh
}

// implements `consumer.T`
func (c *t) Stop() {
	c.dispatcher.Stop()
	c.zkConn.Close()
	c.kafkaClt.Close()
}

// implements `dispatcher.Factory`.
func (c *t) KeyOf(rq consumer.Request) dispatcher.Key {
	return dispatcher.Key(rq.Group)
}

// implements `dispatcher.Factory`.
func (c *t) SpawnChild(childSpec dispatcher.ChildSpec) {
	groupcsm.Spawn(c.actDesc, childSpec, c.cfg, c.kafkaClt, c.zkConn, c.offsetMgrF)
}

// String returns a string ID of this instance to be used in logs.
func (c *t) String() string {
	return c.actDesc.String()
}
