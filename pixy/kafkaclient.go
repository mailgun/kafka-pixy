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
type KafkaClient interface {
	// Produce accepts a message to be asynchronously sent to the configured
	// Kafka cluster. It is possible that the message may be dropped if the
	// destination Kafka broker (the leader of the topic partition that the
	// message belongs to), keeps rejecting it or stays network unreachable for
	// an extended period of time (see the retry policy in `sarama.ClientCfg`).
	//
	// If `topic` does not exist then it is created with the default queue
	// parameters on the broker determined by `key`.
	// If `key` is `nil` then the message is placed into a random partition.
	AsyncProduce(topic string, key, message sarama.Encoder)
}

// KafkaProxyImpl is the sole implementation of the `KafkaProxy` interface.
type KafkaClientImpl struct {
	config       *KafkaClientCfg
	saramaClient sarama.Client
	producer     *KafkaProducer
	wg           sync.WaitGroup
}

type KafkaClientCfg struct {
	// BrokerAddrs is a slice of Kafka broker connection strings.
	BrokerAddrs []string
	// A period of time that a proxy should allow to `sarama.Producer` to
	// submit buffered messages to Kafka. Should be large enough to avoid
	// event loss when shutdown is performed during Kafka leader election.
	ShutdownTimeout time.Duration
	// DeadMessageCh is a channel to dump undelivered messages into. It is used
	// in testing only.
	DeadMessageCh chan<- *ProduceResult
}

func NewKafkaClientCfg() *KafkaClientCfg {
	return &KafkaClientCfg{
		ShutdownTimeout: 30 * time.Second,
	}
}

// SpawnKafkaProxy creates a `KafkaProxy` instance and starts its internal
// goroutines.
func SpawnKafkaClient(config *KafkaClientCfg) (*KafkaClientImpl, error) {
	clientID, err := newClientID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate client id, cause=(%v)", err)
	}

	saramaCfg := sarama.NewConfig()
	saramaCfg.ClientID = clientID
	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true
	saramaCfg.Producer.Compression = sarama.CompressionSnappy
	saramaCfg.Producer.Retry.Backoff = time.Second
	saramaCfg.Producer.Flush.Frequency = 500 * time.Millisecond
	saramaCfg.Producer.Flush.Bytes = 1024 * 1024

	kafkaClient, err := sarama.NewClient(config.BrokerAddrs, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama client, cause=(%v)", err)
	}

	gracefulProducer, err := SpawnKafkaProducer(kafkaClient, config.ShutdownTimeout, config.DeadMessageCh)
	if err != nil {
		return nil, fmt.Errorf("failed to create graceful producer, cause=(%v)", err)
	}

	kci := &KafkaClientImpl{
		config:       config,
		saramaClient: kafkaClient,
		producer:     gracefulProducer,
	}

	if err != nil {
		return nil, err
	}
	return kci, nil
}

// Stop triggers asynchronous proxy shutdown. Use `Wait4Stop` to wait for
// the proxy to shutdown.
func (kci *KafkaClientImpl) Stop() {
	kci.producer.Stop()
}

// Wait4Stop blocks until all internal goroutines are stopped.
func (kci *KafkaClientImpl) Wait4Stop() {
	kci.producer.Wait4Stop()
	kci.saramaClient.Close()
}

// AsyncProduce submits a message to the specified topic of the Kafka cluster
// using `key` to navigate the message to a particular shard.
func (kci *KafkaClientImpl) AsyncProduce(topic string, key, message sarama.Encoder) {
	kci.producer.AsyncProduce(topic, key, message)
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
