package consumerimpl

import (
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/consumer/groupcsm"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
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
	namespace           *actor.ID
	cfg                 *config.Proxy
	dispatcher          *dispatcher.T
	clientForMsgStreams sarama.Client
	clientForOffsetMgrs sarama.Client
	kazooConn           *kazoo.Kazoo
	offsetMgrFactory    offsetmgr.Factory
}

// Spawn creates a consumer instance with the specified configuration and
// starts all its goroutines.
func Spawn(namespace *actor.ID, cfg *config.Proxy) (*t, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.ClientID = cfg.ClientID
	saramaCfg.ChannelBufferSize = cfg.Consumer.ChannelBufferSize
	saramaCfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	saramaCfg.Consumer.Retry.Backoff = cfg.Consumer.BackOffTimeout
	saramaCfg.Consumer.Fetch.Default = 1024 * 1024

	namespace = namespace.NewChild("cons")

	clientForMsgStreams, err := sarama.NewClient(cfg.Kafka.SeedPeers, saramaCfg)
	if err != nil {
		return nil, consumer.ErrSetup(fmt.Errorf("failed to create Kafka client for message streams: err=(%v)", err))
	}
	clientForOffsetMgrs, err := sarama.NewClient(cfg.Kafka.SeedPeers, saramaCfg)
	if err != nil {
		return nil, consumer.ErrSetup(fmt.Errorf("failed to create Kafka client for offset managers: err=(%v)", err))
	}

	kazooCfg := kazoo.NewConfig()
	kazooCfg.Chroot = cfg.ZooKeeper.Chroot
	// ZooKeeper documentation says following about the session timeout: "The
	// current (ZooKeeper) implementation requires that the timeout be a
	// minimum of 2 times the tickTime (as set in the server configuration) and
	// a maximum of 20 times the tickTime". The default tickTime is 2 seconds.
	// See http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkSessions
	kazooCfg.Timeout = 15 * time.Second
	kazooConn, err := kazoo.NewKazoo(cfg.ZooKeeper.SeedPeers, kazooCfg)
	if err != nil {
		return nil, consumer.ErrSetup(fmt.Errorf("failed to create kazoo.Kazoo: err=(%v)", err))
	}

	offsetMgrFactory := offsetmgr.SpawnFactory(namespace, cfg, clientForOffsetMgrs)

	c := &t{
		namespace:           namespace,
		cfg:                 cfg,
		clientForMsgStreams: clientForMsgStreams,
		clientForOffsetMgrs: clientForOffsetMgrs,
		offsetMgrFactory:    offsetMgrFactory,
		kazooConn:           kazooConn,
	}
	c.dispatcher = dispatcher.New(c.namespace, c, c.cfg)
	c.dispatcher.Start()
	return c, nil
}

// implements `consumer.T`
func (c *t) Consume(group, topic string) (*consumer.Message, error) {
	replyCh := make(chan dispatcher.Response, 1)
	c.dispatcher.Requests() <- dispatcher.Request{time.Now().UTC(), group, topic, replyCh}
	result := <-replyCh
	return result.Msg, result.Err
}

// implements `consumer.T`
func (c *t) Stop() {
	c.dispatcher.Stop()
	c.offsetMgrFactory.Stop()
	c.kazooConn.Close()
	c.clientForOffsetMgrs.Close()
	c.clientForMsgStreams.Close()
}

// implements `dispatcher.Factory`.
func (c *t) KeyOf(req dispatcher.Request) string {
	return req.Group
}

// implements `dispatcher.Factory`.
func (c *t) NewTier(key string) dispatcher.Tier {
	return groupcsm.New(c.namespace, key, c.cfg, c.clientForMsgStreams, c.kazooConn, c.offsetMgrFactory)
}

// String returns a string ID of this instance to be used in logs.
func (sc *t) String() string {
	return sc.namespace.String()
}
