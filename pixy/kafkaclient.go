package pixy

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

// KafkaProxy is a Kafka client that provides high-level functions to produce
// and consume messages as well as a number of topic introspection and
// management functions. Most of these functions are exposed by the Kafka-Pixy
// service via an HTTP API.
type KafkaClient interface {
	// Produce submits a message to the specified `topic` of the Kafka cluster
	// using `key` to identify a destination partition. The exact algorithm
	// used to map keys to partitions is implementation specific but it is
	// guaranteed that it returns consistent results. If `key` is `nil`, then
	// the message is placed into a random partition.
	//
	// Errors usually indicate a catastrophic failure of the Kafka cluster, or
	// missing topic if there cluster is not configured to auto create topics.
	Produce(topic string, key, message sarama.Encoder) error

	// AsyncProduce is an asynchronously counterpart of the `Produce` function.
	// Errors are silently ignored.
	//
	// TODO Consider implementing some sort of dead message processing.
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
	Producer    struct {
		// The period of time that a proxy should allow to `sarama.Producer` to
		// submit buffered messages to Kafka. It should be large enough to avoid
		// event loss when shutdown is performed during Kafka leader election.
		ShutdownTimeout time.Duration
		// DeadMessageCh is a channel to dump undelivered messages into. It is
		// used in testing only.
		DeadMessageCh chan<- *ProduceResult
	}
	Consumer struct {
		// A consume request will wait at most this much until a message from
		// the specified group/topic becomes available. This timeout is
		// necessary to account for consumer rebalancing that happens whenever
		// a new consumer joins a group or subscribes to a topic.
		RequestTimeout time.Duration
		// The period of time that a proxy should keep registration with a
		// consumer group or subscription for a topic in absence of requests
		// to the consumer group or topic.
		RegistrationTimeout time.Duration
		// If a request returns an error then it is repeated after this period
		// of time.
		BackOffTimeout time.Duration
		// A consumer should wait this much after it gets notification that a
		// consumer joined/left its consumer group before it should rebalance.
		RebalanceDelay time.Duration
	}
	ChannelBufferSize int
}

func NewKafkaClientCfg() *KafkaClientCfg {
	config := &KafkaClientCfg{}
	config.Producer.ShutdownTimeout = 30 * time.Second
	config.Consumer.RequestTimeout = 3 * time.Second
	config.Consumer.RegistrationTimeout = 20 * time.Second
	config.Consumer.BackOffTimeout = 500 * time.Millisecond
	config.Consumer.RebalanceDelay = 250 * time.Millisecond
	config.ChannelBufferSize = 256
	return config
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

	gracefulProducer, err := SpawnKafkaProducer(kafkaClient,
		config.Producer.ShutdownTimeout, config.Producer.DeadMessageCh)
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

func (kci *KafkaClientImpl) Produce(topic string, key, message sarama.Encoder) error {
	return kci.producer.Produce(topic, key, message)
}

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
