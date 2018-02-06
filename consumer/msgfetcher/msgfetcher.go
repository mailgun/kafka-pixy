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
	Spawn(parentActDesc *actor.Descriptor, topic string, partition int32, offset int64) (T, int64, error)

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
	actDesc  *actor.Descriptor
	cfg      *config.Proxy
	kafkaClt sarama.Client
	mapper   *mapper.T

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
func SpawnFactory(parentActDesc *actor.Descriptor, cfg *config.Proxy, kafkaClt sarama.Client) Factory {
	f := &factory{
		actDesc:  parentActDesc.NewChild("msg_fetcher_f"),
		cfg:      cfg,
		kafkaClt: kafkaClt,
		children: make(map[instanceID]*msgFetcher),
	}
	f.mapper = mapper.Spawn(f.actDesc, cfg, f)
	return f
}

// implements `Factory`.
func (f *factory) Spawn(parentActDesc *actor.Descriptor, topic string, partition int32, offset int64) (T, int64, error) {
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

	actDesc := parentActDesc.NewChild("msg_fetcher")
	actDesc.AddLogField("kafka.topic", topic)
	actDesc.AddLogField("kafka.partition", partition)
	mf := &msgFetcher{
		actDesc:      actDesc,
		f:            f,
		id:           id,
		assignmentCh: make(chan mapper.Executor, 1),
		messagesCh:   make(chan consumer.Message, f.cfg.Consumer.ChannelBufferSize),
		stopCh:       make(chan none.T, 1),
		offset:       realOffset,
	}
	if testReportErrors {
		mf.errorsCh = make(chan error, f.cfg.Consumer.ChannelBufferSize)
	}
	f.children[id] = mf
	actor.Spawn(mf.actDesc, &mf.wg, mf.run)
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
		aggrActDesc:      f.actDesc.NewChild("broker", brokerConn.ID(), "aggr"),
		execActDesc:      f.actDesc.NewChild("broker", brokerConn.ID(), "exec"),
		cfg:              f.cfg,
		conn:             brokerConn,
		requestsCh:       make(chan fetchRq),
		requestBatchesCh: make(chan []fetchRq),
	}
	actor.Spawn(be.aggrActDesc, &be.wg, be.runAggregator)
	actor.Spawn(be.execActDesc, &be.wg, be.runExecutor)
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
	actDesc               *actor.Descriptor
	f                     *factory
	id                    instanceID
	offset                int64
	assignmentCh          chan mapper.Executor
	messagesCh            chan consumer.Message
	errorsCh              chan error
	brokerRequestCh       chan<- fetchRq
	nilOrBrokerRequestsCh chan<- fetchRq
	stopCh                chan none.T
	wg                    sync.WaitGroup
}

// implements `Factory`.
func (mf *msgFetcher) Messages() <-chan consumer.Message {
	return mf.messagesCh
}

// implements `Factory`.
func (mf *msgFetcher) Stop() {
	close(mf.stopCh)
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
		fetchResultCh       = make(chan fetchRs, 1)
		nilOrFetchResultsCh <-chan fetchRs
		nilOrMessagesCh     chan<- consumer.Message
		fetchedMessages     []consumer.Message
		err                 error
		currMessage         consumer.Message
		currMessageIdx      int
	)
	for {
		select {
		case bw := <-mf.assignmentCh:
			mf.actDesc.Log().Infof("Assigned executor: %s", bw)
			be := bw.(*brokerExecutor)
			mf.brokerRequestCh = be.requestsCh

			// If there is a fetch request pending, then let it complete,
			// otherwise trigger one.
			if nilOrFetchResultsCh == nil && nilOrMessagesCh == nil {
				mf.nilOrBrokerRequestsCh = mf.brokerRequestCh
			}

		case mf.nilOrBrokerRequestsCh <- fetchRq{mf.id.topic, mf.id.partition, mf.offset, fetchResultCh}:
			mf.nilOrBrokerRequestsCh = nil
			nilOrFetchResultsCh = fetchResultCh

		case fetchRs := <-nilOrFetchResultsCh:
			nilOrFetchResultsCh = nil
			if fetchedMessages, err = mf.parseFetchResponse(fetchRs); err != nil {
				mf.reportError(err)
				if err == sarama.ErrOffsetOutOfRange {
					mf.actDesc.Log().WithError(err).Error("Fatal request failure")
					// There's no point in retrying this it will just fail the
					// same way, therefore is nothing to do but give up.
					return
				}
				mf.actDesc.Log().WithError(err).Error("Request failed")
				mf.brokerRequestCh = nil
				mf.f.mapper.TriggerReassign(mf)
				continue
			}
			// If no messages has been fetched, then trigger another request.
			if len(fetchedMessages) == 0 {
				mf.nilOrBrokerRequestsCh = mf.brokerRequestCh
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
			mf.nilOrBrokerRequestsCh = mf.brokerRequestCh

		case <-mf.stopCh:
			return
		}
	}
}

// parseFetchResponse parses a fetch response received a broker.
func (mf *msgFetcher) parseFetchResponse(fetchRs fetchRs) ([]consumer.Message, error) {
	if fetchRs.Err != nil {
		return nil, fetchRs.Err
	}
	kafkaFetchRs := fetchRs.kafkaRs
	if kafkaFetchRs == nil {
		return nil, errIncompleteResponse
	}
	kafkaFetchRsBlock := kafkaFetchRs.GetBlock(mf.id.topic, mf.id.partition)
	if kafkaFetchRsBlock == nil {
		return nil, errIncompleteResponse
	}
	if kafkaFetchRsBlock.Err != sarama.ErrNoError {
		return nil, kafkaFetchRsBlock.Err
	}
	if kafkaFetchRsBlock.MessageSet() != nil {
		return mf.parseMessageSet(kafkaFetchRsBlock)
	}
	if kafkaFetchRsBlock.RecordBatch() != nil {
		return mf.parseRecordBatch(kafkaFetchRsBlock)
	}
	// There are no messages available.
	return nil, nil
}

func (mf *msgFetcher) parseMessageSet(kafkaFetchRsBlock *sarama.FetchResponseBlock) ([]consumer.Message, error) {
	messageSet := kafkaFetchRsBlock.MessageSet()
	// We got no messages. If we got a trailing one, it means there is a
	// producer that writes messages larger then Consumer.FetchMaxBytes in size.
	if len(messageSet.Messages) == 0 && messageSet.PartialTrailingMessage {
		mf.actDesc.Log().Errorf("Oversize message skipped: offset=%d", mf.offset)
		mf.reportError(errMessageTooLarge)
		return nil, nil
	}

	var fetchedMessages []consumer.Message
	for _, msgBlock := range messageSet.Messages {
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
			consumerMsg := consumer.Message{
				ConsumerMessage: sarama.ConsumerMessage{
					Topic:     mf.id.topic,
					Partition: mf.id.partition,
					Key:       msg.Msg.Key,
					Value:     msg.Msg.Value,
					Offset:    offset,
					Timestamp: msg.Msg.Timestamp,
				},
				HighWaterMark: kafkaFetchRsBlock.HighWaterMarkOffset,
			}
			fetchedMessages = append(fetchedMessages, consumerMsg)
		}
	}
	if len(fetchedMessages) == 0 {
		return nil, nil
	}
	return fetchedMessages, nil
}

func (mf *msgFetcher) parseRecordBatch(kafkaFetchRsBlock *sarama.FetchResponseBlock) ([]consumer.Message, error) {
	recordBatch := kafkaFetchRsBlock.RecordBatch()

	if recordBatch.Control {
		mf.actDesc.Log().Warn("Control message ignored")
		return nil, nil
	}

	// We got no messages. If we got a trailing one, it means there is a
	// producer that writes messages larger then Consumer.FetchMaxBytes in size.
	if len(recordBatch.Records) == 0 && recordBatch.PartialTrailingRecord {
		mf.actDesc.Log().Errorf("Oversize message skipped: offset=%d", mf.offset)
		mf.reportError(errMessageTooLarge)
		return nil, nil
	}

	var fetchedMessages []consumer.Message
	for _, record := range recordBatch.Records {
		offset := recordBatch.FirstOffset + record.OffsetDelta
		if offset < mf.offset {
			continue
		}
		consumerMsg := consumer.Message{
			ConsumerMessage: sarama.ConsumerMessage{
				Topic:     mf.id.topic,
				Partition: mf.id.partition,
				Key:       record.Key,
				Value:     record.Value,
				Offset:    offset,
				Timestamp: recordBatch.FirstTimestamp.Add(record.TimestampDelta),
			},
			HighWaterMark: kafkaFetchRsBlock.HighWaterMarkOffset,
		}
		fetchedMessages = append(fetchedMessages, consumerMsg)
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
	return mf.actDesc.String()
}

// brokerExecutor maintains a connection with a particular Kafka broker. It
// processes fetch requests from message stream instances and sends responses
// back. The dispatcher goroutine of the message stream factory is responsible
// for keeping a broker executor alive while it is assigned to at least one
// message stream instance.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	aggrActDesc      *actor.Descriptor
	execActDesc      *actor.Descriptor
	cfg              *config.Proxy
	conn             *sarama.Broker
	requestsCh       chan fetchRq
	requestBatchesCh chan []fetchRq
	wg               sync.WaitGroup
}

type fetchRq struct {
	Topic     string
	Partition int32
	Offset    int64
	ReplyToCh chan<- fetchRs
}

type fetchRs struct {
	kafkaRs *sarama.FetchResponse
	Err     error
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
	defer close(be.requestBatchesCh)

	var nilOrRequestBatchesCh chan<- []fetchRq
	var requestBatch []fetchRq
	for {
		select {
		case fr, ok := <-be.requestsCh:
			if !ok {
				return
			}
			requestBatch = append(requestBatch, fr)
			nilOrRequestBatchesCh = be.requestBatchesCh
		case nilOrRequestBatchesCh <- requestBatch:
			requestBatch = nil
			// Disable requestBatchesCh until we have at least one fetch request.
			nilOrRequestBatchesCh = nil
		}
	}
}

// runExecutor executes fetch request aggregated into batches by the aggregator
// goroutine of the broker executor.
func (be *brokerExecutor) runExecutor() {
	var lastErr error
	var lastErrTime time.Time
	for requestBatch := range be.requestBatchesCh {
		// Reject consume requests for awhile after a connection failure to
		// allow the Kafka cluster some time to recuperate.
		if time.Now().UTC().Sub(lastErrTime) < be.cfg.Consumer.RetryBackoff {
			for _, fr := range requestBatch {
				fr.ReplyToCh <- fetchRs{nil, lastErr}
			}
			continue
		}
		// Make a batch fetch request for all hungry message streams.
		kafkaFetchRq := &sarama.FetchRequest{
			MinBytes:    1,
			MaxWaitTime: int32(be.cfg.Consumer.FetchMaxWait / time.Millisecond),
		}
		if be.cfg.Kafka.Version.IsAtLeast(sarama.V0_10_0_0) {
			kafkaFetchRq.Version = 2
		}
		if be.cfg.Kafka.Version.IsAtLeast(sarama.V0_10_1_0) {
			kafkaFetchRq.Version = 3
			kafkaFetchRq.MaxBytes = sarama.MaxResponseSize
		}
		if be.cfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
			kafkaFetchRq.Version = 4
			kafkaFetchRq.Isolation = sarama.ReadUncommitted // We don't support yet transactions.
		}

		for _, fr := range requestBatch {
			kafkaFetchRq.AddBlock(fr.Topic, fr.Partition, fr.Offset, int32(be.cfg.Consumer.FetchMaxBytes))
		}
		var kafkaFetchRs *sarama.FetchResponse
		kafkaFetchRs, lastErr = be.conn.Fetch(kafkaFetchRq)
		if lastErr != nil {
			lastErrTime = time.Now().UTC()
			be.conn.Close()
			be.execActDesc.Log().WithError(lastErr).Info("Connection reset")
		}
		// Fan the response out to the message streams.
		for _, fr := range requestBatch {
			fr.ReplyToCh <- fetchRs{kafkaFetchRs, lastErr}
		}
	}
}

func (be *brokerExecutor) String() string {
	if be == nil {
		return "<nil>"
	}
	return be.aggrActDesc.String()
}
