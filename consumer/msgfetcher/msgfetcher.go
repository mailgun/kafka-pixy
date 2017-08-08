package msgfetcher

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/mapper"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Factory provides API to spawn message fetcher that read messages from
// topic partitions. It ensures that there is only one fetcher instance for a
// particular topic partition at a time.
type Factory interface {
	// Spawn creates and starts a fetcher instance that reads messages from the
	// given topic-partition starting from the specified offset. It will return
	// an error if there is an fetcher instance reading from the topic-partition
	// already.
	//
	// If the given offset does not exists in the topic-partition, then a real
	// offset that the fetcher will start reading from is determined as follows:
	//  * if the given offset equals to sarama.OffsetOldest or it is smaller
	//    then the oldest partition offset, then the oldest partition offset is
	//    selected;
	//  * if the given offset equals to sarama.OffsetNewest, or it is larger
	//    then the newest partition offset, then the newest partition offset is
	//    selected.
	// The real offset value is returned by the function.
	Spawn(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error)

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
	children   map[instanceID]*msgFetcher
}

type instanceID struct {
	topic     string
	partition int32
}

// SpawnFactory creates a new message fetcher factory using the given client.
// It is still necessary to call Stop() on the underlying client after shutting
// down this factory.
func SpawnFactory(namespace *actor.ID, cfg *config.Proxy, kafkaClt sarama.Client) (Factory, error) {
	f := &factory{
		namespace: namespace.NewChild("msg_fetcher_f"),
		cfg:       cfg,
		kafkaClt:  kafkaClt,
		children:  make(map[instanceID]*msgFetcher),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f, nil
}

// implements `Factory`.
func (f *factory) Spawn(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error) {
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
	mf := &msgFetcher{
		actorID:      namespace.NewChild("msg_fetcher"),
		f:            f,
		id:           id,
		assignmentCh: make(chan mapper.Executor, 1),
		messagesCh:   make(chan consumer.Message, f.cfg.Consumer.ChannelBufferSize),
		closingCh:    make(chan none.T, 1),
		offset:       realOffset,
	}
	if testReportErrors {
		mf.errorsCh = make(chan error, f.cfg.Consumer.ChannelBufferSize)
	}
	f.children[id] = mf
	actor.Spawn(mf.actorID, &mf.wg, mf.run)
	return mf, realOffset, nil
}

// implements `Factory`.
func (f *factory) Stop() {
	f.mapper.Stop()
}

// implements `mapper.Resolver.ResolveBroker()`.
func (f *factory) ResolveBroker(worker mapper.Worker) (*sarama.Broker, error) {
	ms := worker.(*msgFetcher)
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

// chooseStartingOffset returns a real offset value selected based on the
// suggested offset. The real offset value is selected as follows:
//  * if the suggested offset equals to sarama.OffsetOldest or it is smaller
//    then the oldest partition offset, then the oldest partition offset is
//    selected;
//  * if the suggested offset equals to sarama.OffsetNewest, or it is larger
//    then the newest partition offset, then the newest partition offset is
//    selected.
// Note that by the time the fetcher starts reading, the offset can become
// invalid, e.g. when the selected offset belongs to an expired segment. In
// this case fetcher will terminate gracefully. The fetcher user can detect
// that by closure of the fetcher message channel and act accordingly.
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

func (f *factory) onMsgIStreamSpawned(mf *msgFetcher) {
	f.mapper.OnWorkerSpawned(mf)
}

func (f *factory) onMsgIStreamStopped(mf *msgFetcher) {
	f.childrenMu.Lock()
	delete(f.children, mf.id)
	f.childrenMu.Unlock()
	f.mapper.OnWorkerStopped(mf)
}

// implements `mapper.Worker`.
type msgFetcher struct {
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

// implements `Factory`.
func (mf *msgFetcher) Messages() <-chan consumer.Message {
	return mf.messagesCh
}

// implements `Factory`.
func (mf *msgFetcher) Stop() {
	close(mf.closingCh)
	mf.wg.Wait()
}

// implements `mapper.Worker`.
func (mf *msgFetcher) Assignment() chan<- mapper.Executor {
	return mf.assignmentCh
}

// pullMessages sends fetched requests to the broker executor assigned by the
// redispatch goroutine; parses broker fetch responses and pushes parsed
// `ConsumerMessages` to the message channel. It tries to keep the message
// channel buffer full making fetch requests to the assigned broker as needed.
func (mf *msgFetcher) run() {
	defer close(mf.messagesCh)
	if mf.errorsCh != nil {
		defer close(mf.errorsCh)
	}

	mf.f.onMsgIStreamSpawned(mf)
	defer mf.f.onMsgIStreamStopped(mf)

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
		case bw := <-mf.assignmentCh:
			log.Infof("<%s> assigned %s", mf.actorID, bw)
			if bw == nil {
				mf.triggerOrScheduleReassign("no broker assigned")
				continue
			}
			mf.nilOrReassignRetryTimerCh = nil

			be := bw.(*brokerExecutor)
			mf.assignedBrokerRequestCh = be.requestsCh

			// If there is a fetch request pending, then let it complete,
			// otherwise trigger one.
			if nilOrFetchResultsCh == nil && nilOrMessagesCh == nil {
				mf.nilOrBrokerRequestsCh = mf.assignedBrokerRequestCh
			}

		case mf.nilOrBrokerRequestsCh <- fetchReq{mf.id.topic, mf.id.partition, mf.offset, fetchResultCh}:
			mf.nilOrBrokerRequestsCh = nil
			nilOrFetchResultsCh = fetchResultCh

		case result := <-nilOrFetchResultsCh:
			nilOrFetchResultsCh = nil
			if fetchedMessages, err = mf.parseFetchResult(mf.actorID, result); err != nil {
				log.Infof("<%s> fetch failed: err=%s", mf.actorID, err)
				mf.reportError(err)
				if err == sarama.ErrOffsetOutOfRange {
					// There's no point in retrying this it will just fail the
					// same way, therefore is nothing to do but give up.
					return
				}
				mf.triggerOrScheduleReassign("fetch error")
				continue
			}
			// If no messages has been fetched, then trigger another request.
			if len(fetchedMessages) == 0 {
				mf.nilOrBrokerRequestsCh = mf.assignedBrokerRequestCh
				continue
			}
			// Some messages have been fetched, start pushing them to the user.
			currMessageIdx = 0
			currMessage = fetchedMessages[currMessageIdx]
			nilOrMessagesCh = mf.messagesCh

		case nilOrMessagesCh <- currMessage:
			mf.offset = currMessage.Offset + 1
			currMessageIdx++
			if currMessageIdx < len(fetchedMessages) {
				currMessage = fetchedMessages[currMessageIdx]
				continue
			}
			// All messages have been pushed, trigger a new fetch request.
			nilOrMessagesCh = nil
			mf.nilOrBrokerRequestsCh = mf.assignedBrokerRequestCh

		case <-mf.nilOrReassignRetryTimerCh:
			mf.f.mapper.TriggerReassign(mf)
			log.Infof("<%s> reassign triggered by timeout", mf.actorID)
			mf.nilOrReassignRetryTimerCh = time.After(mf.f.cfg.Consumer.RetryBackoff)

		case <-mf.closingCh:
			return
		}
	}
}

func (mf *msgFetcher) triggerOrScheduleReassign(reason string) {
	mf.assignedBrokerRequestCh = nil
	now := time.Now().UTC()
	if now.Sub(mf.lastReassignTime) > mf.f.cfg.Consumer.RetryBackoff {
		log.Infof("<%s> trigger reassign: reason=(%s)", mf.actorID, reason)
		mf.lastReassignTime = now
		mf.f.mapper.TriggerReassign(mf)
	} else {
		log.Infof("<%s> schedule reassign: reason=(%s)", mf.actorID, reason)
	}
	mf.nilOrReassignRetryTimerCh = time.After(mf.f.cfg.Consumer.RetryBackoff)
}

// parseFetchResult parses a fetch response received a broker.
func (mf *msgFetcher) parseFetchResult(cid *actor.ID, fetchResult fetchRes) ([]consumer.Message, error) {
	if fetchResult.Err != nil {
		return nil, fetchResult.Err
	}

	response := fetchResult.Response
	if response == nil {
		return nil, errIncompleteResponse
	}

	block := response.GetBlock(mf.id.topic, mf.id.partition)
	if block == nil {
		return nil, errIncompleteResponse
	}

	if block.Err != sarama.ErrNoError {
		return nil, block.Err
	}

	// We got no messages. If we got a trailing one, it means there is a
	// producer that writes messages larger then Consumer.FetchMaxBytes in size.
	if len(block.MsgSet.Messages) == 0 && block.MsgSet.PartialTrailingMessage {
		log.Errorf("<%s> oversized message skipped: offset=%d", cid, mf.offset)
		mf.reportError(errMessageTooLarge)
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
			if offset < mf.offset {
				continue
			}
			consumerMessage := consumer.Message{
				Topic:         mf.id.topic,
				Partition:     mf.id.partition,
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
func (mf *msgFetcher) reportError(err error) {
	if mf.errorsCh == nil {
		return
	}
	select {
	case mf.errorsCh <- err:
	default:
	}
}

func (mf *msgFetcher) String() string {
	return mf.actorID.String()
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
