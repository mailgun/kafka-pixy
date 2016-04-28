package partitioncsm

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/consumermsg"
	"github.com/mailgun/kafka-pixy/consumer/mapper"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
)

// Factory manages PartitionConsumers which fetch messages from brokers.
type Factory interface {
	// SpawnPartitionConsumer creates a T instance for the given topic/partition
	// with the given offset. It will return an error if there is an instance
	// already consuming from the topic/partition.
	//
	// Offset can be a literal offset, or OffsetNewest or OffsetOldest. If
	// offset is smaller then the oldest offset then the oldest offset is
	// returned. If offset is larger then the newest offset then the newest
	// offset is returned. If offset is either sarama.OffsetNewest or
	// sarama.OffsetOldest constant, then the actual offset value is returned.
	// otherwise offset is returned.
	SpawnPartitionConsumer(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error)

	// Stop shuts down the consumer. It must be called after all child partition
	// consumers have already been closed.
	Stop()
}

// T fetched messages from a given topic and partition.
type T interface {
	// Messages returns the read channel for the messages that are returned by the broker.
	Messages() <-chan *consumermsg.ConsumerMessage

	// Errors returns a read channel of errors that occured during consuming, if enabled. By default,
	// errors are logged and not returned over this channel. If you want to implement any custom error
	// handling, set your config's Consumer.Return.Errors setting to true, and read from this channel.
	Errors() <-chan *consumermsg.ConsumerError

	// Stop stops the PartitionConsumer from fetching messages. It must be
	// called before the factory that created the instance is stopped.
	Stop()
}

type factory struct {
	namespace    *actor.ID
	saramaCfg    *sarama.Config
	client       sarama.Client
	children     map[topicPartition]*partitionCsm
	childrenLock sync.Mutex
	mapper       *mapper.T
}

type topicPartition struct {
	topic     string
	partition int32
}

// SpawnFactory creates a new consumer using the given client. It is still
// necessary to call Close() on the underlying client when shutting down this consumer.
func SpawnFactory(namespace *actor.ID, client sarama.Client) (Factory, error) {
	// Check that we are not dealing with a closed Client before processing any other arguments
	if client.Closed() {
		return nil, sarama.ErrClosedClient
	}
	f := &factory{
		namespace: namespace.NewChild("partition_csm_f"),
		client:    client,
		saramaCfg: client.Config(),
		children:  make(map[topicPartition]*partitionCsm),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f, nil
}

// implements `Factory`.
func (f *factory) SpawnPartitionConsumer(namespace *actor.ID, topic string, partition int32, offset int64) (T, int64, error) {
	concreteOffset, err := f.chooseStartingOffset(topic, partition, offset)
	if err != nil {
		return nil, sarama.OffsetNewest, err
	}

	f.childrenLock.Lock()
	defer f.childrenLock.Unlock()

	tp := topicPartition{topic, partition}
	if _, ok := f.children[tp]; ok {
		return nil, sarama.OffsetNewest, sarama.ConfigurationError("That topic/partition is already being consumed")
	}
	pc := f.spawnPartitionCsm(namespace, tp, concreteOffset)
	f.mapper.WorkerSpawned() <- pc
	f.children[tp] = pc
	return pc, concreteOffset, nil
}

// implements `Factory`.
func (f *factory) Stop() {
	f.mapper.Stop()
}

// implements `mapper.Resolver.ResolveBroker()`.
func (f *factory) ResolveBroker(pw mapper.Worker) (*sarama.Broker, error) {
	pc := pw.(*partitionCsm)
	if err := f.client.RefreshMetadata(pc.tp.topic); err != nil {
		return nil, err
	}
	return f.client.Leader(pc.tp.topic, pc.tp.partition)
}

// implements `mapper.Resolver.Executor()`
func (f *factory) SpawnExecutor(brokerConn *sarama.Broker) mapper.Executor {
	bc := &brokerConsumer{
		aggrActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "aggr"),
		execActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "exec"),
		config:          f.saramaCfg,
		conn:            brokerConn,
		requestsCh:      make(chan fetchRequest),
		batchRequestsCh: make(chan []fetchRequest),
	}
	actor.Spawn(bc.aggrActorID, &bc.wg, bc.runAggregator)
	actor.Spawn(bc.execActorID, &bc.wg, bc.runExecutor)
	return bc
}

// chooseStartingOffset takes an offset value that may be either an actual
// offset of two constants (`OffsetNewest` and `OffsetOldest`) and return an
// offset value. It checks if the offset value belongs to the current range.
//
// FIXME: The offset values corresponding to `OffsetNewest` and `OffsetOldest`
// may change during the function execution (e.g. an old log chunk gets
// deleted), so the offset value returned by the function may be incorrect.
func (f *factory) chooseStartingOffset(topic string, partition int32, offset int64) (int64, error) {
	newestOffset, err := f.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return 0, err
	}
	oldestOffset, err := f.client.GetOffset(topic, partition, sarama.OffsetOldest)
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
type partitionCsm struct {
	actorID      *actor.ID
	f            *factory
	tp           topicPartition
	fetchSize    int32
	offset       int64
	lag          int64
	assignmentCh chan mapper.Executor
	initErrorCh  chan error
	messagesCh   chan *consumermsg.ConsumerMessage
	errorsCh     chan *consumermsg.ConsumerError
	closingCh    chan none.T
	wg           sync.WaitGroup
}

func (f *factory) spawnPartitionCsm(namespace *actor.ID, tp topicPartition, offset int64) *partitionCsm {
	pc := &partitionCsm{
		actorID:      namespace.NewChild("partition_csm"),
		f:            f,
		tp:           tp,
		assignmentCh: make(chan mapper.Executor, 1),
		initErrorCh:  make(chan error),
		messagesCh:   make(chan *consumermsg.ConsumerMessage, f.saramaCfg.ChannelBufferSize),
		errorsCh:     make(chan *consumermsg.ConsumerError, f.saramaCfg.ChannelBufferSize),
		closingCh:    make(chan none.T, 1),
		offset:       offset,
		fetchSize:    f.saramaCfg.Consumer.Fetch.Default,
	}
	actor.Spawn(pc.actorID, &pc.wg, pc.run)
	return pc
}

// implements `Factory`.
func (pc *partitionCsm) Messages() <-chan *consumermsg.ConsumerMessage {
	return pc.messagesCh
}

// implements `Factory`.
func (pc *partitionCsm) Errors() <-chan *consumermsg.ConsumerError {
	return pc.errorsCh
}

// implements `Factory`.
func (pc *partitionCsm) Stop() {
	close(pc.closingCh)
	pc.wg.Wait()

	pc.f.childrenLock.Lock()
	delete(pc.f.children, pc.tp)
	pc.f.childrenLock.Unlock()
	pc.f.mapper.WorkerStopped() <- pc
}

// implements `mapper.Worker`.
func (pc *partitionCsm) Assignment() chan<- mapper.Executor {
	return pc.assignmentCh
}

// pullMessages sends fetched requests to the broker consumer assigned by the
// redispatch goroutine; parses broker fetch responses and pushes parsed
// `ConsumerMessages` to the message channel. It tries to keep the message
// channel buffer full making fetch requests to the assigned broker as needed.
func (pc *partitionCsm) run() {
	var (
		assignedFetchRequestCh    chan<- fetchRequest
		nilOrFetchRequestsCh      chan<- fetchRequest
		fetchResultCh             = make(chan fetchResult, 1)
		nilOrFetchResultsCh       <-chan fetchResult
		nilOrMessagesCh           chan<- *consumermsg.ConsumerMessage
		nilOrReassignRetryTimerCh <-chan time.Time
		fetchedMessages           []*consumermsg.ConsumerMessage
		err                       error
		currMessage               *consumermsg.ConsumerMessage
		currMessageIdx            int
		lastReassignTime          time.Time
	)
	triggerOrScheduleReassign := func(reason string) {
		assignedFetchRequestCh = nil
		now := time.Now().UTC()
		if now.Sub(lastReassignTime) > pc.f.saramaCfg.Consumer.Retry.Backoff {
			log.Infof("<%s> trigger reassign: reason=(%s)", pc.actorID, reason)
			lastReassignTime = now
			pc.f.mapper.WorkerReassign() <- pc
		} else {
			log.Infof("<%s> schedule reassign: reason=(%s)", pc.actorID, reason)
		}
		nilOrReassignRetryTimerCh = time.After(pc.f.saramaCfg.Consumer.Retry.Backoff)
	}
pullMessagesLoop:
	for {
		select {
		case bw := <-pc.assignmentCh:
			log.Infof("<%s> assigned %s", pc.actorID, bw)
			if bw == nil {
				triggerOrScheduleReassign("no broker assigned")
				continue pullMessagesLoop
			}
			bc := bw.(*brokerConsumer)
			// A new leader broker has been assigned for the partition.
			assignedFetchRequestCh = bc.requestsCh
			// Cancel the reassign retry timer.
			nilOrReassignRetryTimerCh = nil
			// If there is a fetch request pending, then let it complete,
			// otherwise trigger one.
			if nilOrFetchResultsCh == nil && nilOrMessagesCh == nil {
				nilOrFetchRequestsCh = assignedFetchRequestCh
			}

		case nilOrFetchRequestsCh <- fetchRequest{pc.tp.topic, pc.tp.partition, pc.offset, pc.fetchSize, pc.lag, fetchResultCh}:
			nilOrFetchRequestsCh = nil
			nilOrFetchResultsCh = fetchResultCh

		case result := <-nilOrFetchResultsCh:
			nilOrFetchResultsCh = nil
			if fetchedMessages, err = pc.parseFetchResult(pc.actorID, result); err != nil {
				log.Infof("<%s> fetch failed: err=%s", pc.actorID, err)
				pc.reportError(err)
				if err == sarama.ErrOffsetOutOfRange {
					// There's no point in retrying this it will just fail the
					// same way, therefore is nothing to do but give up.
					goto done
				}
				triggerOrScheduleReassign("fetch error")
				continue pullMessagesLoop
			}
			// If no messages has been fetched, then trigger another request.
			if len(fetchedMessages) == 0 {
				nilOrFetchRequestsCh = assignedFetchRequestCh
				continue pullMessagesLoop
			}
			// Some messages have been fetched, start pushing them to the user.
			currMessageIdx = 0
			currMessage = fetchedMessages[currMessageIdx]
			nilOrMessagesCh = pc.messagesCh

		case nilOrMessagesCh <- currMessage:
			pc.offset = currMessage.Offset + 1
			currMessageIdx++
			if currMessageIdx < len(fetchedMessages) {
				currMessage = fetchedMessages[currMessageIdx]
				continue pullMessagesLoop
			}
			// All messages have been pushed, trigger a new fetch request.
			nilOrMessagesCh = nil
			nilOrFetchRequestsCh = assignedFetchRequestCh

		case <-nilOrReassignRetryTimerCh:
			pc.f.mapper.WorkerReassign() <- pc
			log.Infof("<%s> reassign triggered by timeout", pc.actorID)
			nilOrReassignRetryTimerCh = time.After(pc.f.saramaCfg.Consumer.Retry.Backoff)

		case <-pc.closingCh:
			goto done
		}
	}
done:
	close(pc.messagesCh)
	close(pc.errorsCh)
}

// parseFetchResult parses a fetch response received a broker.
func (pc *partitionCsm) parseFetchResult(cid *actor.ID, fetchResult fetchResult) ([]*consumermsg.ConsumerMessage, error) {
	if fetchResult.Err != nil {
		return nil, fetchResult.Err
	}

	response := fetchResult.Response
	if response == nil {
		return nil, sarama.ErrIncompleteResponse
	}

	block := response.GetBlock(pc.tp.topic, pc.tp.partition)
	if block == nil {
		return nil, sarama.ErrIncompleteResponse
	}

	if block.Err != sarama.ErrNoError {
		return nil, block.Err
	}

	if len(block.MsgSet.Messages) == 0 {
		// We got no messages. If we got a trailing one then we need to ask for more data.
		// Otherwise we just poll again and wait for one to be produced...
		if block.MsgSet.PartialTrailingMessage {
			if pc.f.saramaCfg.Consumer.Fetch.Max > 0 && pc.fetchSize == pc.f.saramaCfg.Consumer.Fetch.Max {
				// we can't ask for more data, we've hit the configured limit
				log.Infof("<%s> oversized message skipped: offset=%d", cid, pc.offset)
				pc.reportError(sarama.ErrMessageTooLarge)
				pc.offset++ // skip this one so we can keep processing future messages
			} else {
				pc.fetchSize *= 2
				if pc.f.saramaCfg.Consumer.Fetch.Max > 0 && pc.fetchSize > pc.f.saramaCfg.Consumer.Fetch.Max {
					pc.fetchSize = pc.f.saramaCfg.Consumer.Fetch.Max
				}
			}
		}

		return nil, nil
	}

	// we got messages, reset our fetch size in case it was increased for a previous request
	pc.fetchSize = pc.f.saramaCfg.Consumer.Fetch.Default
	var fetchedMessages []*consumermsg.ConsumerMessage
	for _, msgBlock := range block.MsgSet.Messages {
		for _, msg := range msgBlock.Messages() {
			if msg.Offset < pc.offset {
				continue
			}
			consumerMessage := &consumermsg.ConsumerMessage{
				Topic:         pc.tp.topic,
				Partition:     pc.tp.partition,
				Key:           msg.Msg.Key,
				Value:         msg.Msg.Value,
				Offset:        msg.Offset,
				HighWaterMark: block.HighWaterMarkOffset,
			}
			fetchedMessages = append(fetchedMessages, consumerMessage)
			pc.lag = block.HighWaterMarkOffset - msg.Offset
		}
	}

	if len(fetchedMessages) == 0 {
		return nil, sarama.ErrIncompleteResponse
	}
	return fetchedMessages, nil
}

// reportError sends partition consumer errors to the error channel if the user
// configured the consumer to do so via `Config.Consumer.Return.Errors`.
func (pc *partitionCsm) reportError(err error) {
	if !pc.f.saramaCfg.Consumer.Return.Errors {
		return
	}
	ce := &consumermsg.ConsumerError{
		Topic:     pc.tp.topic,
		Partition: pc.tp.partition,
		Err:       err,
	}
	select {
	case pc.errorsCh <- ce:
	default:
	}
}

func (pc *partitionCsm) String() string {
	return pc.actorID.String()
}

// brokerConsumer maintains a connection with a particular Kafka broker. It
// processes fetch requests from partition consumers and sends responses back.
// The dispatcher goroutine of the master consumer is responsible for keeping
// a broker consumer alive it is assigned to at least one partition consumer.
//
// implements `mapper.Executor`.
type brokerConsumer struct {
	aggrActorID     *actor.ID
	execActorID     *actor.ID
	config          *sarama.Config
	conn            *sarama.Broker
	requestsCh      chan fetchRequest
	batchRequestsCh chan []fetchRequest
	wg              sync.WaitGroup
}

type fetchRequest struct {
	Topic     string
	Partition int32
	Offset    int64
	MaxBytes  int32
	Lag       int64
	ReplyToCh chan<- fetchResult
}

type fetchResult struct {
	Response *sarama.FetchResponse
	Err      error
}

// implements `mapper.Executor`.
func (bc *brokerConsumer) BrokerConn() *sarama.Broker {
	return bc.conn
}

// implements `mapper.Executor`.
func (bc *brokerConsumer) Stop() {
	close(bc.requestsCh)
	bc.wg.Wait()
}

// runAggregator collects fetch requests from partition consumers into batches
// while the request executor goroutine is busy processing the previous batch.
// As soon as the executor is done, a new batch is handed over to it.
func (bc *brokerConsumer) runAggregator() {
	defer close(bc.batchRequestsCh)

	var nilOrBatchRequestCh chan<- []fetchRequest
	var batchRequest []fetchRequest
	for {
		select {
		case fr, ok := <-bc.requestsCh:
			if !ok {
				return
			}
			batchRequest = append(batchRequest, fr)
			nilOrBatchRequestCh = bc.batchRequestsCh
		case nilOrBatchRequestCh <- batchRequest:
			batchRequest = nil
			// Disable batchRequestsCh until we have at least one fetch request.
			nilOrBatchRequestCh = nil
		}
	}
}

// runExecutor executes fetch request received from partition consumers.
func (bc *brokerConsumer) runExecutor() {
	var lastErr error
	var lastErrTime time.Time
	for fetchRequests := range bc.batchRequestsCh {
		// Reject consume requests for awhile after a connection failure to
		// allow the Kafka cluster some time to recuperate.
		if time.Now().UTC().Sub(lastErrTime) < bc.config.Consumer.Retry.Backoff {
			for _, fr := range fetchRequests {
				fr.ReplyToCh <- fetchResult{nil, lastErr}
			}
			continue
		}
		// Make a batch fetch request for all hungry partition consumers.
		req := &sarama.FetchRequest{
			MinBytes:    bc.config.Consumer.Fetch.Min,
			MaxWaitTime: int32(bc.config.Consumer.MaxWaitTime / time.Millisecond),
		}
		for _, fr := range fetchRequests {
			req.AddBlock(fr.Topic, fr.Partition, fr.Offset, fr.MaxBytes)
		}
		var res *sarama.FetchResponse
		res, lastErr = bc.conn.Fetch(req)
		if lastErr != nil {
			lastErrTime = time.Now().UTC()
			bc.conn.Close()
			log.Infof("<%s> connection reset: err=(%s)", bc.execActorID, lastErr)
		}
		// Fan the response out to the partition consumers.
		for _, fr := range fetchRequests {
			fr.ReplyToCh <- fetchResult{res, lastErr}
		}
	}
}

func (bc *brokerConsumer) String() string {
	if bc == nil {
		return "<nil>"
	}
	return bc.aggrActorID.String()
}
