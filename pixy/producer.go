package pixy

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

const (
	maxEncoderReprLength = 4096
)

// GracefulProducer builds on top of `sarama.AsyncProducer` to improve the
// shutdown handling. The problem it solves is that `sarama.AsyncProducer`
// drops all buffered messages as soon as it is ordered to shutdown. On the
// contrary, when `GracefulProducer` is ordered to stop it allows some time
// for the buffered messages to be committed to the Kafka cluster, and only
// when that time has elapsed it drops uncommitted messages.
type GracefulProducer struct {
	baseCID         *sarama.ContextID
	saramaClient    sarama.Client
	saramaProducer  sarama.AsyncProducer
	shutdownTimeout time.Duration
	deadMessageCh   chan<- *sarama.ProducerMessage
	dispatcherCh    chan *sarama.ProducerMessage
	resultCh        chan produceResult
	wg              sync.WaitGroup
}

type produceResult struct {
	Msg *sarama.ProducerMessage
	Err error
}

// SpawnGracefulProducer creates a `KafkaProducer` instance and starts its internal
// goroutines.
func SpawnGracefulProducer(config *Config) (*GracefulProducer, error) {
	saramaClient, err := sarama.NewClient(config.Kafka.SeedPeers, config.saramaConfig())
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama.Client, err=(%s)", err)
	}
	saramaProducer, err := sarama.NewAsyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create sarama.Producer, err=(%s)", err)
	}
	gp := &GracefulProducer{
		baseCID:         sarama.RootCID.NewChild("producer"),
		saramaClient:    saramaClient,
		saramaProducer:  saramaProducer,
		shutdownTimeout: config.Producer.ShutdownTimeout,
		deadMessageCh:   config.Producer.DeadMessageCh,
		dispatcherCh:    make(chan *sarama.ProducerMessage, config.ChannelBufferSize),
		resultCh:        make(chan produceResult, config.ChannelBufferSize),
	}
	spawn(&gp.wg, gp.merge)
	spawn(&gp.wg, gp.dispatch)
	return gp, nil
}

// Stop shuts down all producer goroutines and releases all resources.
func (gp *GracefulProducer) Stop() {
	close(gp.dispatcherCh)
	gp.wg.Wait()
}

// Produce submits a message to the specified `topic` of the Kafka cluster
// using `key` to identify a destination partition. The exact algorithm used to
// map keys to partitions is implementation specific but it is guaranteed that
// it returns consistent results. If `key` is `nil`, then the message is placed
// into a random partition.
//
// Errors usually indicate a catastrophic failure of the Kafka cluster, or
// missing topic if there cluster is not configured to auto create topics.
func (gp *GracefulProducer) Produce(topic string, key, message sarama.Encoder) (*sarama.ProducerMessage, error) {
	replyCh := make(chan produceResult, 1)
	prodMsg := &sarama.ProducerMessage{
		Topic:    topic,
		Key:      key,
		Value:    message,
		Metadata: replyCh,
	}
	gp.dispatcherCh <- prodMsg
	result := <-replyCh
	return result.Msg, result.Err
}

// AsyncProduce is an asynchronously counterpart of the `Produce` function.
// Errors are silently ignored.
//
// TODO Consider implementing some sort of dead message processing.
func (gp *GracefulProducer) AsyncProduce(topic string, key, message sarama.Encoder) {
	prodMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: message,
	}
	gp.dispatcherCh <- prodMsg
}

// merge receives both message acknowledgements and producer errors from the
// respective `sarama.AsyncProducer` channels, constructs `ProducerResult`s out
// of them and sends the constructed `ProducerResult` instances to `resultCh`
// to be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.AsyncProducer` output channels are
// closed. Then it closes the `resultCh` to notify the `dispatcher` goroutine
// that all pending messages have been processed and exits.
func (gp *GracefulProducer) merge() {
	cid := gp.baseCID.NewChild("merge")
	defer cid.LogScope()()
	nilOrProdSuccessesCh := gp.saramaProducer.Successes()
	nilOrProdErrorsCh := gp.saramaProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, ok := <-nilOrProdSuccessesCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdSuccessesCh = nil
				continue mergeLoop
			}
			gp.resultCh <- produceResult{Msg: ackedMsg}
		case prodErr, ok := <-nilOrProdErrorsCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdErrorsCh = nil
				continue mergeLoop
			}
			gp.resultCh <- produceResult{Msg: prodErr.Msg, Err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(gp.resultCh)
}

// dispatch implements message processing and graceful shutdown. It receives
// messages from `dispatchedCh` where they are send to by `Produce` method and
// submits them to the embedded `sarama.AsyncProducer`. The dispatcher main
// purpose is to prevent loss of messages during shutdown. It achieves that by
// allowing some graceful period after it stops receiving messages and stopping
// the embedded `sarama.AsyncProducer`.
func (gp *GracefulProducer) dispatch() {
	cid := gp.baseCID.NewChild("dispatch")
	defer cid.LogScope()()
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
			nilOrProdInputCh = gp.saramaProducer.Input()
		case nilOrProdInputCh <- prodMsg:
			nilOrDispatcherCh = gp.dispatcherCh
			nilOrProdInputCh = nil
		case prodResult := <-gp.resultCh:
			pendingMsgCount -= 1
			gp.handleProduceResult(cid, prodResult)
		}
	}
gracefulShutdown:
	// Give the `sarama.AsyncProducer` some time to commit buffered messages.
	log.Infof("<%v> About to stop producer: pendingMsgCount=%d", cid, pendingMsgCount)
	shutdownTimeoutCh := time.After(gp.shutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto shutdownNow
		case prodResult := <-gp.resultCh:
			pendingMsgCount -= 1
			gp.handleProduceResult(cid, prodResult)
		}
	}
shutdownNow:
	log.Infof("<%v> Stopping producer: pendingMsgCount=%d", cid, pendingMsgCount)
	gp.saramaProducer.AsyncClose()
	for prodResult := range gp.resultCh {
		gp.handleProduceResult(cid, prodResult)
	}
}

// handleProduceResult inspects a production results and if it is an error
// then logs it and flushes it down the `deadMessageCh` if one had been
// configured.
func (gp *GracefulProducer) handleProduceResult(cid *sarama.ContextID, result produceResult) {
	if replyCh, ok := result.Msg.Metadata.(chan produceResult); ok {
		replyCh <- result
	}
	if result.Err == nil {
		return
	}
	prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
		result.Msg.Topic, encoderRepr(result.Msg.Key), encoderRepr(result.Msg.Value))
	log.Errorf("<%v> Failed to submit message: msg=%v, err=(%s)",
		cid, prodMsgRepr, result.Err)
	if gp.deadMessageCh != nil {
		gp.deadMessageCh <- result.Msg
	}
}

// encoderRepr returns the string representation of an encoder value. The value
// is truncated to `maxEncoderReprLength`.
func encoderRepr(e sarama.Encoder) string {
	var repr string
	switch e := e.(type) {
	case sarama.StringEncoder:
		repr = string(e)
	case sarama.ByteEncoder:
		repr = fmt.Sprintf("%X", []byte(e))
	default:
		repr = fmt.Sprint(e)
	}
	if length := len(repr); length > maxEncoderReprLength {
		repr = fmt.Sprintf("%s... (%d bytes more)",
			repr[:maxEncoderReprLength], length-maxEncoderReprLength)
	}
	return repr
}
