package producer

import (
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

const (
	maxEncoderReprLength = 4096
)

// T builds on top of `sarama.AsyncProducer` to improve the shutdown handling.
// The problem it solves is that `sarama.AsyncProducer` drops all buffered
// messages as soon as it is ordered to shutdown. On the contrary, when `T` is
// ordered to stop it allows some time for the buffered messages to be
// committed to the Kafka cluster, and only when that time has elapsed it drops
// uncommitted messages.
//
// TODO Consider implementing some sort of dead message processing.
type T struct {
	mergerActorID     *actor.ID
	dispatcherActorID *actor.ID
	saramaClient      sarama.Client
	saramaProducer    sarama.AsyncProducer
	shutdownTimeout   time.Duration
	dispatcherCh      chan *sarama.ProducerMessage
	resultCh          chan produceResult
	wg                sync.WaitGroup

	// To be used in tests only
	testDroppedMsgCh chan<- *sarama.ProducerMessage
}

type produceResult struct {
	Msg *sarama.ProducerMessage
	Err error
}

// Spawn creates a producer instance and starts its internal goroutines.
func Spawn(namespace *actor.ID, cfg *config.Proxy) (*T, error) {
	saramaCfg := cfg.SaramaProducerCfg()
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	saramaClient, err := sarama.NewClient(cfg.Kafka.SeedPeers, saramaCfg)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama.Client")
	}
	saramaProducer, err := sarama.NewAsyncProducerFromClient(saramaClient)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create sarama.Producer")
	}

	prodNamespace := namespace.NewChild("prod")
	p := &T{
		mergerActorID:     prodNamespace.NewChild("merger"),
		dispatcherActorID: prodNamespace.NewChild("dispatcher"),
		saramaClient:      saramaClient,
		saramaProducer:    saramaProducer,
		shutdownTimeout:   cfg.Producer.ShutdownTimeout,
		dispatcherCh:      make(chan *sarama.ProducerMessage, cfg.Producer.ChannelBufferSize),
		resultCh:          make(chan produceResult, cfg.Producer.ChannelBufferSize),
	}
	actor.Spawn(p.mergerActorID, &p.wg, p.runMerger)
	actor.Spawn(p.dispatcherActorID, &p.wg, p.runDispatcher)
	return p, nil
}

// Stop shuts down all producer goroutines and releases all resources.
func (p *T) Stop() {
	close(p.dispatcherCh)
	p.wg.Wait()
}

// Produce submits a message to the specified `topic` of the Kafka cluster
// using `key` to identify a destination partition. The exact algorithm used to
// map keys to partitions is implementation specific but it is guaranteed that
// it returns consistent results. If `key` is `nil`, then the message is placed
// into a random partition.
//
// Errors usually indicate a catastrophic failure of the Kafka cluster, or
// missing topic if there cluster is not configured to auto create topics.
func (p *T) Produce(topic string, key, message sarama.Encoder) (*sarama.ProducerMessage, error) {
	replyCh := make(chan produceResult, 1)
	prodMsg := &sarama.ProducerMessage{
		Topic:    topic,
		Key:      key,
		Value:    message,
		Metadata: replyCh,
	}
	p.dispatcherCh <- prodMsg
	result := <-replyCh
	return result.Msg, result.Err
}

// AsyncProduce is an asynchronously counterpart of the `Produce` function.
// Errors are silently ignored.
func (p *T) AsyncProduce(topic string, key, message sarama.Encoder) {
	prodMsg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: message,
	}
	p.dispatcherCh <- prodMsg
}

// merge receives both message acknowledgements and producer errors from the
// respective `sarama.AsyncProducer` channels, constructs `ProducerResult`s out
// of them and sends the constructed `ProducerResult` instances to `resultCh`
// to be further inspected by the `dispatcher` goroutine.
//
// It keeps running until both `sarama.AsyncProducer` output channels are
// closed. Then it closes the `resultCh` to notify the `dispatcher` goroutine
// that all pending messages have been processed and exits.
func (p *T) runMerger() {
	nilOrProdSuccessesCh := p.saramaProducer.Successes()
	nilOrProdErrorsCh := p.saramaProducer.Errors()
mergeLoop:
	for channelsOpened := 2; channelsOpened > 0; {
		select {
		case ackedMsg, ok := <-nilOrProdSuccessesCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdSuccessesCh = nil
				continue mergeLoop
			}
			p.resultCh <- produceResult{Msg: ackedMsg}
		case prodErr, ok := <-nilOrProdErrorsCh:
			if !ok {
				channelsOpened -= 1
				nilOrProdErrorsCh = nil
				continue mergeLoop
			}
			p.resultCh <- produceResult{Msg: prodErr.Msg, Err: prodErr.Err}
		}
	}
	// Close the result channel to notify the `dispatcher` goroutine that all
	// pending messages have been processed.
	close(p.resultCh)
}

// dispatch implements message processing and graceful shutdown. It receives
// messages from `dispatchedCh` where they are send to by `Produce` method and
// submits them to the embedded `sarama.AsyncProducer`. The dispatcher main
// purpose is to prevent loss of messages during shutdown. It achieves that by
// allowing some graceful period after it stops receiving messages and stopping
// the embedded `sarama.AsyncProducer`.
func (p *T) runDispatcher() {
	nilOrDispatcherCh := p.dispatcherCh
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
			nilOrProdInputCh = p.saramaProducer.Input()
		case nilOrProdInputCh <- prodMsg:
			nilOrDispatcherCh = p.dispatcherCh
			nilOrProdInputCh = nil
		case prodResult := <-p.resultCh:
			pendingMsgCount -= 1
			p.handleProduceResult(prodResult)
		}
	}
gracefulShutdown:
	// Give the `sarama.AsyncProducer` some time to commit buffered messages.
	log.Infof("<%v> About to stop producer: pendingMsgCount=%d", p.dispatcherActorID, pendingMsgCount)
	shutdownTimeoutCh := time.After(p.shutdownTimeout)
	for pendingMsgCount > 0 {
		select {
		case <-shutdownTimeoutCh:
			goto shutdownNow
		case prodResult := <-p.resultCh:
			pendingMsgCount -= 1
			p.handleProduceResult(prodResult)
		}
	}
shutdownNow:
	log.Infof("<%v> Stopping producer: pendingMsgCount=%d", p.dispatcherActorID, pendingMsgCount)
	p.saramaProducer.AsyncClose()
	for prodResult := range p.resultCh {
		p.handleProduceResult(prodResult)
	}
}

// handleProduceResult inspects a production results and if it is an error
// then logs it.
func (p *T) handleProduceResult(result produceResult) {
	if replyCh, ok := result.Msg.Metadata.(chan produceResult); ok {
		replyCh <- result
	}
	if result.Err == nil {
		return
	}
	prodMsgRepr := fmt.Sprintf(`{Topic: "%s", Key: "%s", Value: "%s"}`,
		result.Msg.Topic, encoderRepr(result.Msg.Key), encoderRepr(result.Msg.Value))
	log.Errorf("<%v> Failed to submit message: msg=%v, err=(%s)",
		p.dispatcherActorID, prodMsgRepr, result.Err)
	if p.testDroppedMsgCh != nil {
		p.testDroppedMsgCh <- result.Msg
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
