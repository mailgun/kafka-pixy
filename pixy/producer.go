package pixy

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

const (
	chanBufferSize = 128
)

// GracefulProducer builds on top of `sarama.AsyncProducer` to improve the
// shutdown handling. The problem it solves is that `sarama.AsyncProducer`
// drops all buffered messages as soon as it is ordered to shutdown. On the
// contrary, when `GracefulProducer` is ordered to stop it allows some time
// for the buffered messages to be committed to the Kafka cluster, and only
// when that time has elapsed it drops uncommitted messages.
type GracefulProducer struct {
	kafkaClient     sarama.Client
	kafkaProducer   sarama.AsyncProducer
	shutdownTimeout time.Duration
	deadMessageCh   chan<- *ProductionResult
	dispatcherCh    chan *sarama.ProducerMessage
	resultCh        chan *ProductionResult
	wg              sync.WaitGroup
}

type ProductionResult struct {
	msg *sarama.ProducerMessage
	err error
}

// SpawnGracefulProducer creates a `GracefulProducer` instance and starts its internal
// goroutines.
func SpawnGracefulProducer(kafkaClient sarama.Client, shutdownTimeout time.Duration,
	deadMessageCh chan<- *ProductionResult) (*GracefulProducer, error) {

	kafkaProducer, err := sarama.NewAsyncProducerFromClient(kafkaClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka producer, cause=(%v)", err)
	}
	gp := &GracefulProducer{
		kafkaClient:     kafkaClient,
		kafkaProducer:   kafkaProducer,
		shutdownTimeout: shutdownTimeout,
		deadMessageCh:   deadMessageCh,
		dispatcherCh:    make(chan *sarama.ProducerMessage, chanBufferSize),
		resultCh:        make(chan *ProductionResult, chanBufferSize),
	}
	goGo("ProducerMerger", &gp.wg, gp.merger)
	goGo("ProducerDispatcher", &gp.wg, gp.dispatcher)
	return gp, nil
}

// Stop triggers asynchronous producer shutdown. Use `Wait4Stop` to wait for
// the producer to shutdown.
func (gp *GracefulProducer) Stop() {
	close(gp.dispatcherCh)
}

// Wait4Stop blocks until all internal goroutines are stopped.
func (gp *GracefulProducer) Wait4Stop() {
	gp.wg.Wait()
}

// AsyncProduce submits a message to the specified topic of the Kafka cluster
// using `key` to navigate the message to a particular shard.
func (gp *GracefulProducer) AsyncProduce(topic string, key, message []byte) {
	prodMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   toEncoderPreservingNil(key),
		Value: sarama.ByteEncoder(message),
	}
	gp.dispatcherCh <- prodMsg
}

// merger receives both message acknowledgements and producer errors from the
// respective `sarama.AsyncProducer` channels, constructs `ProducerResult`s out
// of them and sends the constructed `ProducerResult` instances to `resultCh`
// to be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.AsyncProducer` output channels are
// closed. Then it closes the `resultCh` to notify the `dispatcher` goroutine
// that all pending messages have been processed and exits.
func (gp *GracefulProducer) merger() {
	nilOrProdSuccessesCh := gp.kafkaProducer.Successes()
	nilOrProdErrorsCh := gp.kafkaProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, ok := <-nilOrProdSuccessesCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdSuccessesCh = nil
				continue mergeLoop
			}
			gp.resultCh <- &ProductionResult{msg: ackedMsg}
		case prodErr, ok := <-nilOrProdErrorsCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdErrorsCh = nil
				continue mergeLoop
			}
			gp.resultCh <- &ProductionResult{msg: prodErr.Msg, err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(gp.resultCh)
}

// dispatcher implements message processing and graceful shutdown. It receives
// messages from `dispatchedCh` where they are send to by `Produce` method and
// submits them to the embedded `sarama.AsyncProducer`. The dispatcher main
// purpose is to prevent loss of messages during shutdown. It achieves that by
// allowing some graceful period after it stops receiving messages and stopping
// the embedded `sarama.AsyncProducer`.
func (gp *GracefulProducer) dispatcher() {
	nilOrDispatcherCh := gp.dispatcherCh
	var nilOrProdInputCh chan<- *sarama.ProducerMessage
	pendingMsgCount := 0
	// The normal operation loop is implemented as two-stroke machine. On the
	// first stroke a message is received from `dispatchCh`, and on the second
	// it is sent to `prodInputCh`. Note that producer results can be received
	// at any time.
	prodMsg := (*sarama.ProducerMessage)(nil)
	channelOpened := true
	for {
		select {
		case prodMsg, channelOpened = <-nilOrDispatcherCh:
			if !channelOpened {
				goto gracefulShutdown
			}
			pendingMsgCount += 1
			nilOrDispatcherCh = nil
			nilOrProdInputCh = gp.kafkaProducer.Input()
		case nilOrProdInputCh <- prodMsg:
			nilOrDispatcherCh = gp.dispatcherCh
			nilOrProdInputCh = nil
		case prodResult := <-gp.resultCh:
			pendingMsgCount -= 1
			gp.handleProduceResult(prodResult)
		}
	}
gracefulShutdown:
	// Give the `sarama.AsyncProducer` some time to commit buffered messages.
	log.Infof("About to stop producer: pendingMsgCount=%d", pendingMsgCount)
	shutdownTimeoutCh := time.After(gp.shutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto shutdownNow
		case prodResult := <-gp.resultCh:
			pendingMsgCount -= 1
			gp.handleProduceResult(prodResult)
		}
	}
shutdownNow:
	log.Infof("Stopping producer: pendingMsgCount=%d", pendingMsgCount)
	gp.kafkaProducer.AsyncClose()
	for prodResult := range gp.resultCh {
		gp.handleProduceResult(prodResult)
	}
}

// handleProduceResult inspects a production results and if it is an error
// then logs it and flushes it down the `deadMessageCh` if one had been
// configured.
func (gp *GracefulProducer) handleProduceResult(result *ProductionResult) {
	if result.err == nil {
		return
	}
	prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
		result.msg.Topic, result.msg.Key, result.msg.Value)
	log.Errorf("Failed to submit message: msg=%v, cause=(%v)",
		prodMsgRepr, result.err)
	if gp.deadMessageCh != nil {
		gp.deadMessageCh <- result
	}
}
