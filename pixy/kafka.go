package pixy

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

const (
	chanBufferSize = 100
)

// KafkaProxy defines interface of the engine behind Kafka-Pixy. It has the
// only concrete implementation that is `KafkaPixyImpl` and provided mainly to
// allow mocking it API frontend unit tests.
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
	Produce(topic string, key, message []byte)
}

type KafkaProxyImpl struct {
	cfg           *KafkaProxyCfg
	kafkaClient   sarama.Client
	kafkaProducer sarama.AsyncProducer
	dispatcherCh  chan *sarama.ProducerMessage
	resultCh      chan *producerResult
	deadMessageCh chan<- *producerResult
	wg            sync.WaitGroup
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
	DeadMessageCh chan<- *producerResult
}

type producerResult struct {
	msg *sarama.ProducerMessage
	err error
}

func NewKafkaProxyCfg() *KafkaProxyCfg {
	return &KafkaProxyCfg{
		ShutdownTimeout: 30 * time.Second,
	}
}

// SpawnKafkaProxy creates a `KafkaProxy` instance and starts its internal
// goroutines.
func SpawnKafkaProxy(cfg *KafkaProxyCfg) (*KafkaProxyImpl, error) {
	kpi, err := NewKafkaProxy(cfg)
	if err != nil {
		return nil, err
	}
	kpi.Start()
	return kpi, nil
}

// NewKafkaProxy creates a `KafkaProxyImpl` instance.
func NewKafkaProxy(cfg *KafkaProxyCfg) (*KafkaProxyImpl, error) {
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

	kafkaProducer, err := sarama.NewAsyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer, cause=(%v)", err)
	}

	kpi := &KafkaProxyImpl{
		cfg:           cfg,
		kafkaClient:   kafkaClient,
		kafkaProducer: kafkaProducer,
		dispatcherCh:  make(chan *sarama.ProducerMessage, chanBufferSize),
		resultCh:      make(chan *producerResult, chanBufferSize),
		deadMessageCh: cfg.DeadMessageCh,
	}
	return kpi, nil
}

// Start triggers asynchronous start of internal goroutines.
func (kpi *KafkaProxyImpl) Start() {
	goGo("Merger", &kpi.wg, kpi.merger)
	goGo("Dispatcher", &kpi.wg, kpi.dispatcher)
}

// Stop triggers asynchronous client shutdown. Caller should use `wg` passed
// to `SpawnKafkaProxy` in order to wait for shutdown completion.
func (kpi *KafkaProxyImpl) Stop() {
	close(kpi.dispatcherCh)
}

// Wait4Stop blocks until all internal goroutines are stopped.
func (kpi *KafkaProxyImpl) Wait4Stop() {
	kpi.wg.Wait()
}

// Dispose must be called if a `KafkaProxyImpl` instance, that has not been
// started with the `Start()` method needs to be discarded.
func (kpi *KafkaProxyImpl) Dispose() {
	kpi.kafkaProducer.Close()
	kpi.kafkaClient.Close()
}

// Produce submits a message to the specified topic of the Kafka cluster using
// `key` to navigate the message to a particular shard.
//
// An attempt to call `Produce` after the client stop has been triggered will
// result in panic.
func (kpi *KafkaProxyImpl) Produce(topic string, key, message []byte) {
	// Preserve `nil` values while converting from `[]byte` to `sarama.Encoder`.
	var keyEncoderOrNil sarama.Encoder
	if key != nil {
		keyEncoderOrNil = sarama.ByteEncoder(key)
	}

	prodMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   keyEncoderOrNil,
		Value: sarama.ByteEncoder(message),
	}
	kpi.dispatcherCh <- prodMsg
}

// merger receives both acknowledged messages and producer errors from the
// respective `sarama.Producer` channels, constructs `ProducerResult`s out of
// them and sends the constructed `producerResult` instances to `resultCh` to
// be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.Producer` output channels are closed,
// Then it closes the `KafkaProxyImpl.resultCh` to notify the `dispatcher`
// goroutine that all pending messages are processed and exits.
func (kpi *KafkaProxyImpl) merger() {
	prodSuccessesCh := kpi.kafkaProducer.Successes()
	prodErrorsCh := kpi.kafkaProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, channelOpened := <-prodSuccessesCh:
			if !channelOpened {
				channelsOpened -= 1
				prodSuccessesCh = nil
				continue mergeLoop
			}
			kpi.resultCh <- &producerResult{msg: ackedMsg}
		case prodErr, channelOpened := <-prodErrorsCh:
			if !channelOpened {
				channelsOpened -= 1
				prodErrorsCh = nil
				continue mergeLoop
			}
			kpi.resultCh <- &producerResult{msg: prodErr.Msg, err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(kpi.resultCh)
}

// dispatcher is the main `KafkaProxyImpl` goroutine. It receives messages from
// `dispatchedCh` where they are send to by `Produce` method and submits them
// to `sarama.Producer` for actual delivery. The dispatcher main goal is to
// ensure that buffered messages are not lost during graceful shutdown. It
// achieves that by allowing a time period after it stops receiving messages
// via `dispatchCh` and when the `sarama.Producer` stop is triggered.
func (kpi *KafkaProxyImpl) dispatcher() {
	dispatcherCh := kpi.dispatcherCh
	var prodInputCh chan<- *sarama.ProducerMessage
	pendingMsgCount := 0
	// The normal operation loop is implemented as two-stroke machine. On the
	// first stroke a message is received from `dispatchCh`, and on the second
	// it is sent to `prodInputCh`. Note that producer results can be received
	// at any time.
	prodMsg := (*sarama.ProducerMessage)(nil)
	channelOpened := true
	for {
		select {
		case prodMsg, channelOpened = <-dispatcherCh:
			if !channelOpened {
				goto gracefulShutdown
			}
			pendingMsgCount += 1
			dispatcherCh = nil
			prodInputCh = kpi.kafkaProducer.Input()
		case prodInputCh <- prodMsg:
			dispatcherCh = kpi.dispatcherCh
			prodInputCh = nil
		case prodResult := <-kpi.resultCh:
			pendingMsgCount -= 1
			kpi.processResult(prodResult)
		}
	}
gracefulShutdown:
	log.Infof("About to stop producer: pendingMsgCount=%d", pendingMsgCount)
	// `sarama.Producer` is designed so that if it is instructed to stop it
	// drops messages from the internal retry buffer down to its error channel.
	// To make sure that all pending messages are submitted we give the Kafka
	// cluster some time to recover, before we make the producer stop.
	shutdownTimeoutCh := time.After(kpi.cfg.ShutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto shutdownNow
		case prodResult := <-kpi.resultCh:
			pendingMsgCount -= 1
			kpi.processResult(prodResult)
		}
	}
shutdownNow:
	log.Infof("Stopping producer: pendingMsgCount=%d", pendingMsgCount)
	kpi.kafkaProducer.AsyncClose()
	for prodResult := range kpi.resultCh {
		kpi.processResult(prodResult)
	}
	kpi.kafkaClient.Close()
}

// processResult inspects the production results and in case if it is an error
// and a hand off channel was provided to `SpawnKafkaProxy` the result is sent
// down to that channel. The `handOffCh` is intended mainly for testing but can
// also be used to implement some disaster recovery logic. E.g. write failed
// messages to a local file, or something along these lines.
func (kpi *KafkaProxyImpl) processResult(result *producerResult) {
	if result.err != nil {
		prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
			result.msg.Topic, result.msg.Key, result.msg.Value)
		log.Errorf("Failed to submit message: msg=%v, cause=(%v)",
			prodMsgRepr, result.err)
		if kpi.deadMessageCh != nil {
			kpi.deadMessageCh <- result
		}
	}
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
