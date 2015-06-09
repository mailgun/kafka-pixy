package pixy

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
)

// KafkaProxy is a Kafka client that provides high-level functions to produce
// and consume messages as well as a number of topic introspection and
// management functions. Most of these functions are exposed by the Kafka-Pixy
// service via an HTTP API.
type KafkaProxy interface {
	// Produce accepts a message to be asynchronously sent to the configured
	// Kafka cluster. It is possible that the message may be dropped if the
	// destination Kafka broker (the leader of the topic partition that the
	// message belongs to), keeps rejecting it or stays network unreachable for
	// an extended period of time (see the retry policy in `sarama.ClientCfg`).
	//
	// If `topic` does not exist then it is created with the default queue
	// parameters on the broker determined by `key`.
	// If `key` is `nil` then the message is placed into a random partition.
	AsyncProduce(topic string, key, message []byte)
}

// KafkaProxyImpl is the sole implementation of the `KafkaProxy` interface.
type KafkaProxyImpl struct {
	cfg         *KafkaProxyCfg
	kafkaClient sarama.Client
	producer    *GracefulProducer
	wg          sync.WaitGroup
}

type KafkaProxyCfg struct {
	// BrokerAddrs is a slice of Kafka broker connection strings.
	BrokerAddrs []string
	// A period of time that a proxy should allow to `sarama.Producer` to
	// submit buffered messages to Kafka. Should be large enough to avoid
	// event loss when shutdown is performed during Kafka leader election.
	ShutdownTimeout time.Duration
	// DeadMessageCh is a channel to dump undelivered messages into. It is used
	// in testing only.
	DeadMessageCh chan<- *ProductionResult
}

func NewKafkaProxyCfg() *KafkaProxyCfg {
	return &KafkaProxyCfg{
		ShutdownTimeout: 30 * time.Second,
	}
}

// SpawnKafkaProxy creates a `KafkaProxy` instance and starts its internal
// goroutines.
func SpawnKafkaProxy(cfg *KafkaProxyCfg) (*KafkaProxyImpl, error) {
	clientID, err := newClientID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate client id, cause=(%v)", err)
	}

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.ClientID = clientID
	kafkaCfg.Producer.RequiredAcks = sarama.WaitForAll
	kafkaCfg.Producer.Return.Successes = true
	kafkaCfg.Producer.Return.Errors = true
	kafkaCfg.Producer.Compression = sarama.CompressionSnappy
	kafkaCfg.Producer.Retry.Backoff = time.Second
	kafkaCfg.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaCfg.Producer.Flush.Bytes = 1024 * 1024

	kafkaClient, err := sarama.NewClient(cfg.BrokerAddrs, kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client, cause=(%v)", err)
	}

	gracefulProducer, err := SpawnGracefulProducer(kafkaClient, cfg.ShutdownTimeout, cfg.DeadMessageCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create graceful producer, cause=(%v)", err)
	}

	kpi := &KafkaProxyImpl{
		cfg:         cfg,
		kafkaClient: kafkaClient,
		producer:    gracefulProducer,
	}

	if err != nil {
		return nil, err
	}
	return kpi, nil
}

// Stop triggers asynchronous proxy shutdown. Use `Wait4Stop` to wait for
// the proxy to shutdown.
func (kpi *KafkaProxyImpl) Stop() {
	kpi.producer.Stop()
}

// Wait4Stop blocks until all internal goroutines are stopped.
func (kpi *KafkaProxyImpl) Wait4Stop() {
	kpi.producer.Wait4Stop()
	kpi.kafkaClient.Close()
}

// AsyncProduce submits a message to the specified topic of the Kafka cluster
// using `key` to navigate the message to a particular shard.
func (kpi *KafkaProxyImpl) AsyncProduce(topic string, key, message []byte) {
	kpi.producer.AsyncProduce(topic, key, message)
}

// newClientID creates a client id that should be used when connecting to
// a Kafka cluster.
func newClientID() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", fmt.Errorf("failed to get hostname, cause=(%v)", err)
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s_%d", hostname, pid), nil
}
