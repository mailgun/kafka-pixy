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

type Producer interface {
	// Produce accepts `message` to be asynchronously written to Kafka. The
	// message is guaranteed to be submitted to Kafka eventually, but that may
	// not happen instantaneously if the destination Kafka broker is down. In
	// that case the message is deferred and submitted to the broker as soon as
	// it gets back online.
	//
	// If `topic` does not exist then it is created with the default queue
	// parameters on the broker determined by `key`.
	// If `key` is `nil` then the message is placed into a random partition.
	Produce(topic string, key, message []byte)
}

type KafkaClientCfg struct {
	// Comma separated list of brokers.
	BrokerAddrs []string
	// A period of time that the client gives to `sarama.Producer` to submit
	// buffered messages to Kafka. Should be large enough to avoid event loss
	// when shutdown is performed during Kafka leader election.
	ShutdownTimeout time.Duration
	// A channel to dump failed messages into.
	HandOffCh chan<- *producerResult
}

type KafkaClient struct {
	cfg             *KafkaClientCfg
	wrappedClient   sarama.Client
	wrappedProducer sarama.AsyncProducer
	dispatcherCh    chan *sarama.ProducerMessage
	resultCh        chan *producerResult
	handOffCh       chan<- *producerResult
	wg              sync.WaitGroup
}

type producerResult struct {
	msg *sarama.ProducerMessage
	err error
}

func NewKafkaClientCfg() *KafkaClientCfg {
	return &KafkaClientCfg{
		ShutdownTimeout: 30 * time.Second,
	}
}

// SpawnKafkaClient creates a `KafkaClient` instance and starts its internal
// goroutines.
func SpawnKafkaClient(cfg *KafkaClientCfg) (*KafkaClient, error) {
	kc, err := NewKafkaClient(cfg)
	if err != nil {
		return nil, err
	}
	kc.Start()
	return kc, nil
}

// NewKafkaClient creates a `KafkaClient` instance.
func NewKafkaClient(cfg *KafkaClientCfg) (*KafkaClient, error) {
	clientID, err := newClientID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate client id, cause=(%v)", err)
	}

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.ClientID = clientID
	kafkaCfg.Producer.Return.Successes = true
	kafkaCfg.Producer.Return.Errors = true
	kafkaCfg.Producer.Compression = sarama.CompressionSnappy
	kafkaCfg.Producer.Retry.Backoff = time.Second
	kafkaCfg.Producer.Flush.Frequency = 500 * time.Millisecond
	kafkaCfg.Producer.Flush.Bytes = 1024 * 1024

	wrappedClient, err := sarama.NewClient(cfg.BrokerAddrs, kafkaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client, cause=(%v)", err)
	}

	wrappedProducer, err := sarama.NewAsyncProducerFromClient(wrappedClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer, cause=(%v)", err)
	}

	kc := &KafkaClient{
		cfg:             cfg,
		wrappedClient:   wrappedClient,
		wrappedProducer: wrappedProducer,
		dispatcherCh:    make(chan *sarama.ProducerMessage, chanBufferSize),
		resultCh:        make(chan *producerResult, chanBufferSize),
		handOffCh:       cfg.HandOffCh,
	}
	return kc, nil
}

// Start triggers asynchronous start of internal goroutines.
func (kc *KafkaClient) Start() {
	goGo("Merger", &kc.wg, kc.merger)
	goGo("Dispatcher", &kc.wg, kc.dispatcher)
}

// Stop triggers asynchronous client shutdown. Caller should use `wg` passed
// to `SpawnKafkaClient` in order to wait for shutdown completion.
func (kc *KafkaClient) Stop() {
	close(kc.dispatcherCh)
}

// Wait4Stop blocks until all internal goroutines are stopped.
func (kc *KafkaClient) Wait4Stop() {
	kc.wg.Wait()
}

// Close must be called if a `KafkaClient` instance, that has not been started,
// needs to be discarded.
func (kc *KafkaClient) Close() {
	kc.wrappedProducer.Close()
	kc.wrappedClient.Close()
}

// Produce submits a message to the specified topic of the Kafka cluster using
// `key` to navigate the message to a particular shard.
//
// An attempt to call `Produce` after the client stop has been triggered will
// result in panic.
func (kc *KafkaClient) Produce(topic string, key, message []byte) {
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
	kc.dispatcherCh <- prodMsg
}

// merger receives both acknowledged messages and producer errors from the
// respective `sarama.Producer` channels, constructs `ProducerResult`s out of
// them and sends the constructed `producerResult` instances to `resultCh` to
// be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.Producer` output channels are closed,
// Then it closes the `KafkaClient.resultCh` to notify the `dispatcher`
// goroutine that all pending messages are processed and exits.
func (kc *KafkaClient) merger() {
	prodSuccessesCh := kc.wrappedProducer.Successes()
	prodErrorsCh := kc.wrappedProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, channelOpened := <-prodSuccessesCh:
			if !channelOpened {
				channelsOpened -= 1
				prodSuccessesCh = nil
				continue mergeLoop
			}
			kc.resultCh <- &producerResult{msg: ackedMsg}
		case prodErr, channelOpened := <-prodErrorsCh:
			if !channelOpened {
				channelsOpened -= 1
				prodErrorsCh = nil
				continue mergeLoop
			}
			kc.resultCh <- &producerResult{msg: prodErr.Msg, err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(kc.resultCh)
}

// dispatcher is the main `KafkaClient` goroutine. It receives messages from
// `dispatchedCh` where they are send to by `Produce` method and submits them
// to `sarama.Producer` for actual delivery. The dispatcher main goal is to
// ensure that buffered messages are not lost during graceful shutdown. It
// achieves that by allowing a time period after it stops receiving messages
// via `dispatchCh` and when the `sarama.Producer` stop is triggered.
func (kc *KafkaClient) dispatcher() {
	dispatcherCh := kc.dispatcherCh
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
			prodInputCh = kc.wrappedProducer.Input()
		case prodInputCh <- prodMsg:
			dispatcherCh = kc.dispatcherCh
			prodInputCh = nil
		case prodResult := <-kc.resultCh:
			pendingMsgCount -= 1
			kc.processResult(prodResult)
		}
	}
gracefulShutdown:
	log.Infof("About to stop producer: pendingMsgCount=%d", pendingMsgCount)
	// `sarama.Producer` is designed so that if it is instructed to stop it
	// drops messages from the internal retry buffer down to its error channel.
	// To make sure that all pending messages are submitted we give the Kafka
	// cluster some time to recover, before we make the producer stop.
	shutdownTimeoutCh := time.After(kc.cfg.ShutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto shutdownNow
		case prodResult := <-kc.resultCh:
			pendingMsgCount -= 1
			kc.processResult(prodResult)
		}
	}
shutdownNow:
	log.Infof("Stopping producer: pendingMsgCount=%d", pendingMsgCount)
	kc.wrappedProducer.AsyncClose()
	for prodResult := range kc.resultCh {
		kc.processResult(prodResult)
	}
	kc.wrappedClient.Close()
}

// processResult inspects the production results and in case if it is an error
// and a hand off channel was provided to `SpawnKafkaClient` the result is sent
// down to that channel. The `handOffCh` is intended mainly for testing but can
// also be used to implement some disaster recovery logic. E.g. write failed
// messages to a local file, or something along these lines.
func (kc *KafkaClient) processResult(result *producerResult) {
	if result.err != nil {
		prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
			result.msg.Topic, result.msg.Key, result.msg.Value)
		log.Errorf("Failed to submit message: msg=%v, cause=(%v)",
			prodMsgRepr, result.err)
		if kc.handOffCh != nil {
			kc.handOffCh <- result
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
