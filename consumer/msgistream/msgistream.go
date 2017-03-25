package msgistream

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/mapper"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

// Factory provides API to spawn message streams to that read message from
// topic partitions. It ensures that there is only on message stream for a
// particular topic partition at a time.
type Factory interface {
	// SpawnMessageIStream creates a T instance for the given topic/partition
	// with the given offset. It will return an error if there is an instance
	// already consuming from the topic/partition.
	//
	// If offset is smaller then the oldest offset then the oldest offset is
	// returned. If offset is larger then the newest offset then the newest
	// offset is returned. If offset is either sarama.OffsetNewest or
	// sarama.OffsetOldest constant, then the actual offset value is returned.
	// otherwise offset is returned unchanged.
	SpawnMessageIStream(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error)

	// Stop shuts down the consumer. It must be called after all child partition
	// consumers have already been closed.
	Stop()
}

// T fetched messages from a given topic and partition.
type T interface {
	// Messages returns the read channel for the messages that are fetched from
	// the topic partition.
	Messages() <-chan consumer.Message

	// Stop synchronously stops the partition consumer. It must be called
	// before the factory that created the instance can be stopped.
	Stop()
}

var (
	// To be used in tests only! If true then offset manager will initialize
	// their errors channel and will send internal errors.
	testReportErrors bool

	errMessageTooLarge    = errors.New("message is larger than consumer.fetch_max_bytes")
	errIncompleteResponse = errors.New("response did not contain the expected topic/partition block")
)

type factory struct {
	namespace *actor.ID
	cfg       *config.Proxy
	kafkaClt  sarama.Client
	mapper    *mapper.T

	childrenMu sync.Mutex
	children   map[instanceID]*msgIStream
}

type instanceID struct {
	topic     string
	partition int32
}

// SpawnFactory creates a new message stream factory using the given client. It
// is still necessary to call Stop() on the underlying client after shutting
// down this factory.
func SpawnFactory(namespace *actor.ID, cfg *config.Proxy, kafkaClt sarama.Client) (Factory, error) {
	f := &factory{
		namespace: namespace.NewChild("msg_stream_f"),
		cfg:       cfg,
		kafkaClt:  kafkaClt,
		children:  make(map[instanceID]*msgIStream),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f, nil
}

// implements `Factory`.
func (f *factory) SpawnMessageIStream(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error) {
	realOffset, err := f.chooseStartingOffset(topic, partition, offset)
	if err != nil {
		return nil, sarama.OffsetNewest, err
	}

	f.childrenMu.Lock()
	defer f.childrenMu.Unlock()

	id := instanceID{topic, partition}
	if _, ok := f.children[id]; ok {
		return nil, sarama.OffsetNewest, sarama.ConfigurationError("That topic/partition is already being consumed")
	}
	ms := f.spawnMsgIStream(namespace, id, realOffset)
	f.mapper.OnWorkerSpawned(ms)
	f.children[id] = ms
	return ms, realOffset, nil
}

// implements `Factory`.
func (f *factory) Stop() {
	f.mapper.Stop()
}

// implements `mapper.Resolver.ResolveBroker()`.
func (f *factory) ResolveBroker(worker mapper.Worker) (*sarama.Broker, error) {
	ms := worker.(*msgIStream)
	if err := f.kafkaClt.RefreshMetadata(ms.id.topic); err != nil {
		return nil, err
	}
	return f.kafkaClt.Leader(ms.id.topic, ms.id.partition)
}

// implements `mapper.Resolver.Executor()`
func (f *factory) SpawnExecutor(brokerConn *sarama.Broker) mapper.Executor {
	be := &brokerExecutor{
		aggrActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "aggr"),
		execActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "exec"),
		cfg:             f.cfg,
		conn:            brokerConn,
		requestsCh:      make(chan fetchReq),
		batchRequestsCh: make(chan []fetchReq),
	}
	actor.Spawn(be.aggrActorID, &be.wg, be.runAggregator)
	actor.Spawn(be.execActorID, &be.wg, be.runExecutor)
	return be
}

// chooseStartingOffset takes an offset value that may be either an actual
// offset of two constants (`OffsetNewest` and `OffsetOldest`) and return an
// offset value. It checks if the offset value belongs to the current range.
//
// FIXME: The offset values corresponding to `OffsetNewest` and `OffsetOldest`
// may change during the function execution (e.g. an old log chunk gets
// deleted), so the offset value returned by the function may be incorrect.
func (f *factory) chooseStartingOffset(topic string, partition int32, offset int64) (int64, error) {
	newestOffset, err := f.kafkaClt.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}
	oldestOffset, err := f.kafkaClt.GetOffset(topic, partition, sarama.OffsetOldest)
	if err != nil {
		return 0, err
	}

	switch {
	case offset == sarama.OffsetNewest || offset > newestOffset:
		return newestOffset, nil
	case offset == sarama.OffsetOldest || offset < oldestOffset:
		return oldestOffset, nil
	default:
		return offset, nil
	}
}

// implements `mapper.Worker`.
type msgIStream struct {
	actorID      *actor.ID
	f            *factory
	id           instanceID
	offset       int64
	assignmentCh chan mapper.Executor
	messagesCh   chan consumer.Message
	errorsCh     chan error
	closingCh    chan none.T
	wg           sync.WaitGroup

	assignedBrokerRequestCh   chan<- fetchReq
	nilOrBrokerRequestsCh     chan<- fetchReq
	nilOrReassignRetryTimerCh <-chan time.Time
	lastReassignTime          time.Time
}

func (f *factory) spawnMsgIStream(namespace *actor.ID, id instanceID, offset int64) *msgIStream {
	mis := &msgIStream{
		actorID:      namespace.NewChild("msg_stream"),
		f:            f,
		id:           id,
		assignmentCh: make(chan mapper.Executor, 1),
		messagesCh:   make(chan consumer.Message, f.cfg.Consumer.ChannelBufferSize),
		closingCh:    make(chan none.T, 1),
		offset:       offset,
	}
	if testReportErrors {
		mis.errorsCh = make(chan error, f.cfg.Consumer.ChannelBufferSize)
	}
	actor.Spawn(mis.actorID, &mis.wg, mis.run)
	return mis
}

// implements `Factory`.
func (mis *msgIStream) Messages() <-chan consumer.Message {
	return mis.messagesCh
}

// implements `Factory`.
func (mis *msgIStream) Stop() {
	close(mis.closingCh)
	mis.wg.Wait()

	mis.f.childrenMu.Lock()
	delete(mis.f.children, mis.id)
	mis.f.childrenMu.Unlock()
	mis.f.mapper.OnWorkerStopped(mis)
}

// implements `mapper.Worker`.
func (mis *msgIStream) Assignment() chan<- mapper.Executor {
	return mis.assignmentCh
}

// pullMessages sends fetched requests to the broker executor assigned by the
// redispatch goroutine; parses broker fetch responses and pushes parsed
// `ConsumerMessages` to the message channel. It tries to keep the message
// channel buffer full making fetch requests to the assigned broker as needed.
func (mis *msgIStream) run() {
	defer close(mis.messagesCh)
	if mis.errorsCh != nil {
		defer close(mis.errorsCh)
	}
	var (
		fetchResultCh       = make(chan fetchRes, 1)
		nilOrFetchResultsCh <-chan fetchRes
		nilOrMessagesCh     chan<- consumer.Message
		fetchedMessages     []consumer.Message
		err                 error
		currMessage         consumer.Message
		currMessageIdx      int
	)
	for {
		select {
		case bw := <-mis.assignmentCh:
			log.Infof("<%s> assigned %s", mis.actorID, bw)
			if bw == nil {
				mis.triggerOrScheduleReassign("no broker assigned")
				continue
			}
			mis.nilOrReassignRetryTimerCh = nil

			be := bw.(*brokerExecutor)
			mis.assignedBrokerRequestCh = be.requestsCh

			// If there is a fetch request pending, then let it complete,
			// otherwise trigger one.
			if nilOrFetchResultsCh == nil && nilOrMessagesCh == nil {
				mis.nilOrBrokerRequestsCh = mis.assignedBrokerRequestCh
			}

		case mis.nilOrBrokerRequestsCh <- fetchReq{mis.id.topic, mis.id.partition, mis.offset, fetchResultCh}:
			mis.nilOrBrokerRequestsCh = nil
			nilOrFetchResultsCh = fetchResultCh

		case result := <-nilOrFetchResultsCh:
			nilOrFetchResultsCh = nil
			if fetchedMessages, err = mis.parseFetchResult(mis.actorID, result); err != nil {
				log.Infof("<%s> fetch failed: err=%s", mis.actorID, err)
				mis.reportError(err)
				if err == sarama.ErrOffsetOutOfRange {
					// There's no point in retrying this it will just fail the
					// same way, therefore is nothing to do but give up.
					return
				}
				mis.triggerOrScheduleReassign("fetch error")
				continue
			}
			// If no messages has been fetched, then trigger another request.
			if len(fetchedMessages) == 0 {
				mis.nilOrBrokerRequestsCh = mis.assignedBrokerRequestCh
				continue
			}
			// Some messages have been fetched, start pushing them to the user.
			currMessageIdx = 0
			currMessage = fetchedMessages[currMessageIdx]
			nilOrMessagesCh = mis.messagesCh

		case nilOrMessagesCh <- currMessage:
			mis.offset = currMessage.Offset + 1
			currMessageIdx++
			if currMessageIdx < len(fetchedMessages) {
				currMessage = fetchedMessages[currMessageIdx]
				continue
			}
			// All messages have been pushed, trigger a new fetch request.
			nilOrMessagesCh = nil
			mis.nilOrBrokerRequestsCh = mis.assignedBrokerRequestCh

		case <-mis.nilOrReassignRetryTimerCh:
			mis.f.mapper.TriggerReassign(mis)
			log.Infof("<%s> reassign triggered by timeout", mis.actorID)
			mis.nilOrReassignRetryTimerCh = time.After(mis.f.cfg.Consumer.RetryBackoff)

		case <-mis.closingCh:
			return
		}
	}
}

func (mis *msgIStream) triggerOrScheduleReassign(reason string) {
	mis.assignedBrokerRequestCh = nil
	now := time.Now().UTC()
	if now.Sub(mis.lastReassignTime) > mis.f.cfg.Consumer.RetryBackoff {
		log.Infof("<%s> trigger reassign: reason=(%s)", mis.actorID, reason)
		mis.lastReassignTime = now
		mis.f.mapper.TriggerReassign(mis)
	} else {
		log.Infof("<%s> schedule reassign: reason=(%s)", mis.actorID, reason)
	}
	mis.nilOrReassignRetryTimerCh = time.After(mis.f.cfg.Consumer.RetryBackoff)
}

// parseFetchResult parses a fetch response received a broker.
func (mis *msgIStream) parseFetchResult(cid *actor.ID, fetchResult fetchRes) ([]consumer.Message, error) {
	if fetchResult.Err != nil {
		return nil, fetchResult.Err
	}

	response := fetchResult.Response
	if response == nil {
		return nil, errIncompleteResponse
	}

	block := response.GetBlock(mis.id.topic, mis.id.partition)
	if block == nil {
		return nil, errIncompleteResponse
	}

	if block.Err != sarama.ErrNoError {
		return nil, block.Err
	}

	// We got no messages. If we got a trailing one, it means there is a
	// producer that writes messages larger then Consumer.FetchMaxBytes in size.
	if len(block.MsgSet.Messages) == 0 && block.MsgSet.PartialTrailingMessage {
		log.Errorf("<%s> oversized message skipped: offset=%d", cid, mis.offset)
		mis.reportError(errMessageTooLarge)
		return nil, nil
	}

	var fetchedMessages []consumer.Message
	for _, msgBlock := range block.MsgSet.Messages {
		lastMsgIdx := len(msgBlock.Messages()) - 1
		baseOffset := msgBlock.Offset - msgBlock.Messages()[lastMsgIdx].Offset
		for _, msg := range msgBlock.Messages() {
			offset := msg.Offset
			if msg.Msg.Version >= 1 {
				offset += baseOffset
			}
			if offset < mis.offset {
				continue
			}
			consumerMessage := consumer.Message{
				Topic:         mis.id.topic,
				Partition:     mis.id.partition,
				Key:           msg.Msg.Key,
				Value:         msg.Msg.Value,
				Offset:        offset,
				Timestamp:     msg.Msg.Timestamp,
				HighWaterMark: block.HighWaterMarkOffset,
			}
			fetchedMessages = append(fetchedMessages, consumerMessage)
		}
	}
	if len(fetchedMessages) == 0 {
		return nil, nil
	}
	return fetchedMessages, nil
}

// reportError sends message fetch errors to the error channel if the user
// configured the message stream to do so via `Config.Consumer.Return.Errors`.
func (mis *msgIStream) reportError(err error) {
	if mis.errorsCh == nil {
		return
	}
	select {
	case mis.errorsCh <- err:
	default:
	}
}

func (mis *msgIStream) String() string {
	return mis.actorID.String()
}

// brokerExecutor maintains a connection with a particular Kafka broker. It
// processes fetch requests from message stream instances and sends responses
// back. The dispatcher goroutine of the message stream factory is responsible
// for keeping a broker executor alive while it is assigned to at least one
// message stream instance.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	aggrActorID     *actor.ID
	execActorID     *actor.ID
	cfg             *config.Proxy
	conn            *sarama.Broker
	requestsCh      chan fetchReq
	batchRequestsCh chan []fetchReq
	wg              sync.WaitGroup
}

type fetchReq struct {
	Topic     string
	Partition int32
	Offset    int64
	ReplyToCh chan<- fetchRes
}

type fetchRes struct {
	Response *sarama.FetchResponse
	Err      error
}

// implements `mapper.Executor`.
func (be *brokerExecutor) BrokerConn() *sarama.Broker {
	return be.conn
}

// implements `mapper.Executor`.
func (be *brokerExecutor) Stop() {
	close(be.requestsCh)
	be.wg.Wait()
}

// runAggregator collects fetch requests from message streams into batches
// while the request executor goroutine is busy processing the previous batch.
// As soon as the executor is done, a new batch is handed over to it.
func (be *brokerExecutor) runAggregator() {
	defer close(be.batchRequestsCh)

	var nilOrBatchRequestCh chan<- []fetchReq
	var batchRequest []fetchReq
	for {
		select {
		case fr, ok := <-be.requestsCh:
			if !ok {
				return
			}
			batchRequest = append(batchRequest, fr)
			nilOrBatchRequestCh = be.batchRequestsCh
		case nilOrBatchRequestCh <- batchRequest:
			batchRequest = nil
			// Disable batchRequestsCh until we have at least one fetch request.
			nilOrBatchRequestCh = nil
		}
	}
}

// runExecutor executes fetch request aggregated into batches by the aggregator
// goroutine of the broker executor.
func (be *brokerExecutor) runExecutor() {
	var lastErr error
	var lastErrTime time.Time
	for fetchRequests := range be.batchRequestsCh {
		// Reject consume requests for awhile after a connection failure to
		// allow the Kafka cluster some time to recuperate.
		if time.Now().UTC().Sub(lastErrTime) < be.cfg.Consumer.RetryBackoff {
			for _, fr := range fetchRequests {
				fr.ReplyToCh <- fetchRes{nil, lastErr}
			}
			continue
		}
		// Make a batch fetch request for all hungry message streams.
		req := &sarama.FetchRequest{
			MinBytes:    1,
			MaxWaitTime: int32(be.cfg.Consumer.FetchMaxWait / time.Millisecond),
		}
		if be.cfg.Kafka.Version.IsAtLeast(sarama.V0_10_0_0) {
			req.Version = 2
		}

		for _, fr := range fetchRequests {
			req.AddBlock(fr.Topic, fr.Partition, fr.Offset, int32(be.cfg.Consumer.FetchMaxBytes))
		}
		var res *sarama.FetchResponse
		res, lastErr = be.conn.Fetch(req)
		if lastErr != nil {
			lastErrTime = time.Now().UTC()
			be.conn.Close()
			log.Infof("<%s> connection reset: err=(%s)", be.execActorID, lastErr)
		}
		// Fan the response out to the message streams.
		for _, fr := range fetchRequests {
			fr.ReplyToCh <- fetchRes{res, lastErr}
		}
	}
}

func (be *brokerExecutor) String() string {
	if be == nil {
		return "<nil>"
	}
	return be.aggrActorID.String()
}
