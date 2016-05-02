package offsetmgr

import (
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/mapper"
	"github.com/mailgun/log"
)

// Factory provides a method to spawn offset manager instances to commit
// offsets for a particular group/topic/partition. It makes sure that there is
// only one running manager instance for a particular group/topic/partition
// combination.
//
// One Factory instance per application is usually more then enough, but it is
// possible to create many of them.
//
// Factory spawns background goroutines so it must be explicitly stopped by the
// application. But first it should explicitly stop all spawned offset manager
// instances.
type Factory interface {
	// NewOffsetManager creates an OffsetManager for the given group/topic/partition.
	// It returns an error if given group/topic/partition already has a not stopped
	// OffsetManager instance. After an old offset manager instance is stopped a
	// new one can be started.
	SpawnOffsetManager(namespace *actor.ID, group, topic string, partition int32) (T, error)

	// Stop waits for the spawned offset managers to stop and then terminates. Note
	// that all spawned offset managers has to be explicitly stopped by calling
	// their Stop method.
	Stop()
}

// T provides interface to store and retrieve offsets for a particular
// group/topic/partition in Kafka.
type T interface {
	// InitialOffset returns a channel that an initial offset will be sent down
	// to, when retrieved by a background goroutine. At most one value is sent down
	// the channel, and the channel is closed immediately after that. If error
	// reporting is enabled with `Config.Consumer.Return.Errors` then errors may be
	// coming and has to be read from the `Errors()` channel, otherwise the offset
	// manager will get into a dead lock.
	InitialOffset() <-chan DecoratedOffset

	// SubmitOffset triggers saving of the specified offset in Kafka. Commits are
	// performed periodically in a background goroutine. The commit interval is
	// configured by `Config.Consumer.Offsets.CommitInterval`. Note that not every
	// submitted offset gets committed. Committed offsets are sent down to the
	// `CommittedOffsets()` channel. The `CommittedOffsets()` channel has to be
	// read alongside with submitting offsets, otherwise the partition offset
	// manager will block.
	SubmitOffset(offset int64, metadata string)

	// CommittedOffsets returns a channel that offsets committed to Kafka are
	// sent down to. The user must read from this channel otherwise the
	// `SubmitOffset` function will eventually block.
	CommittedOffsets() <-chan DecoratedOffset

	// Errors returns a read channel of errors that occur during offset management,
	// if enabled. By default errors are not returned. If you want to implement any
	// custom error handling logic then you need to set `Consumer.Return.Errors` to
	// true, and read from this channel.
	Errors() <-chan *OffsetCommitError

	// Stop stops the offset manager. It is required to stop all spawned offset
	// managers before their parent factory can be stopped.
	//
	// It is guaranteed that the most recent offset is committed before `Stop`
	// returns.
	Stop()
}

type DecoratedOffset struct {
	Offset   int64
	Metadata string
}

type OffsetCommitError struct {
	Group     string
	Topic     string
	Partition int32
	Err       error
}

var ErrNoCoordinator = errors.New("failed to resolve coordinator")
var ErrRequestTimeout = errors.New("request timeout")

// SpawnFactory creates a new offset manager factory from the given client.
func SpawnFactory(namespace *actor.ID, client sarama.Client) Factory {
	f := &factory{
		namespace: namespace.NewChild("offset_mgr_f"),
		client:    client,
		config:    client.Config(),
		children:  make(map[groupTopicPartition]*offsetManager),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f
}

// implements `Factory`
// implements `mapper.Resolver`
type factory struct {
	namespace    *actor.ID
	client       sarama.Client
	config       *sarama.Config
	mapper       *mapper.T
	children     map[groupTopicPartition]*offsetManager
	childrenLock sync.Mutex
}

type groupTopicPartition struct {
	group     string
	topic     string
	partition int32
}

// implements `Factory`
func (f *factory) SpawnOffsetManager(namespace *actor.ID, group, topic string, partition int32) (T, error) {
	gtp := groupTopicPartition{group, topic, partition}

	f.childrenLock.Lock()
	defer f.childrenLock.Unlock()
	if _, ok := f.children[gtp]; ok {
		return nil, sarama.ConfigurationError("This group/topic/partition is already being managed")
	}
	om := f.spawnOffsetManager(namespace, gtp)
	f.mapper.WorkerSpawned() <- om
	f.children[gtp] = om
	return om, nil
}

// implements `mapper.Resolver`.
func (f *factory) ResolveBroker(pw mapper.Worker) (*sarama.Broker, error) {
	om := pw.(*offsetManager)
	if err := f.client.RefreshCoordinator(om.gtp.group); err != nil {
		return nil, err
	}

	brokerConn, err := f.client.Coordinator(om.gtp.group)
	if err != nil {
		return nil, err
	}
	return brokerConn, nil
}

// implements `mapper.Resolver`.
func (f *factory) SpawnExecutor(brokerConn *sarama.Broker) mapper.Executor {
	be := &brokerExecutor{
		aggrActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "aggr"),
		execActorID:     f.namespace.NewChild("broker", brokerConn.ID(), "exec"),
		config:          f.config,
		conn:            brokerConn,
		requestsCh:      make(chan submitRequest),
		batchRequestsCh: make(chan map[string]map[groupTopicPartition]submitRequest),
	}
	actor.Spawn(be.aggrActorID, &be.wg, be.runAggregator)
	actor.Spawn(be.execActorID, &be.wg, be.runExecutor)
	return be
}

func (f *factory) spawnOffsetManager(namespace *actor.ID, gtp groupTopicPartition) *offsetManager {
	om := &offsetManager{
		actorID:            namespace.NewChild("offset_mgr"),
		f:                  f,
		gtp:                gtp,
		initialOffsetCh:    make(chan DecoratedOffset, 1),
		submitRequestsCh:   make(chan submitRequest),
		assignmentCh:       make(chan mapper.Executor, 1),
		committedOffsetsCh: make(chan DecoratedOffset, f.config.ChannelBufferSize),
		errorsCh:           make(chan *OffsetCommitError, f.config.ChannelBufferSize),
	}
	actor.Spawn(om.actorID, &om.wg, om.run)
	return om
}

// implements `Factory.Stop()`
func (f *factory) Stop() {
	f.mapper.Stop()
}

// implements `T`
// implements `mapper.Worker`
type offsetManager struct {
	actorID            *actor.ID
	f                  *factory
	gtp                groupTopicPartition
	initialOffsetCh    chan DecoratedOffset
	submitRequestsCh   chan submitRequest
	assignmentCh       chan mapper.Executor
	committedOffsetsCh chan DecoratedOffset
	errorsCh           chan *OffsetCommitError
	wg                 sync.WaitGroup
}

// implements `T`.
func (om *offsetManager) InitialOffset() <-chan DecoratedOffset {
	return om.initialOffsetCh
}

// implements `T`.
func (om *offsetManager) SubmitOffset(offset int64, metadata string) {
	om.submitRequestsCh <- submitRequest{
		gtp:      om.gtp,
		offset:   offset,
		metadata: metadata,
	}
}

// implements `T`.
func (om *offsetManager) CommittedOffsets() <-chan DecoratedOffset {
	return om.committedOffsetsCh
}

// implements `T`.
func (om *offsetManager) Errors() <-chan *OffsetCommitError {
	return om.errorsCh
}

// implements `T`.
func (om *offsetManager) Stop() {
	close(om.submitRequestsCh)
	om.wg.Wait()

	om.f.childrenLock.Lock()
	delete(om.f.children, om.gtp)
	om.f.childrenLock.Unlock()
	om.f.mapper.WorkerStopped() <- om
}

// implements `mapper.Worker`.
func (om *offsetManager) Assignment() chan<- mapper.Executor {
	return om.assignmentCh
}

func (om *offsetManager) String() string {
	return om.actorID.String()
}

func (om *offsetManager) run() {
	defer close(om.committedOffsetsCh)
	defer close(om.errorsCh)
	var (
		commitResultCh            = make(chan submitResponse, 1)
		initialOffsetFetched      = false
		isDirty                   = false
		isPending                 = false
		closed                    = false
		nilOrSubmittedOffsetsCh   = om.submitRequestsCh
		commitTicker              = time.NewTicker(om.f.config.Consumer.Offsets.CommitInterval)
		lastSubmitRequest         submitRequest
		assignedBrokerRequestsCh  chan<- submitRequest
		nilOrBrokerRequestsCh     chan<- submitRequest
		nilOrReassignRetryTimerCh <-chan time.Time
		lastSubmitTime            time.Time
		lastReassignTime          time.Time
		brokerRequestTimeout      = 3 * om.f.config.Consumer.Offsets.CommitInterval
	)
	defer commitTicker.Stop()
	triggerOrScheduleReassign := func(err error, reason string) {
		om.reportError(err)
		assignedBrokerRequestsCh = nil
		nilOrBrokerRequestsCh = nil
		now := time.Now().UTC()
		if now.Sub(lastReassignTime) > om.f.config.Consumer.Retry.Backoff {
			log.Infof("<%s> trigger reassign: reason=%s, err=(%s)", om.actorID, reason, err)
			lastReassignTime = now
			om.f.mapper.WorkerReassign() <- om
		} else {
			log.Infof("<%s> schedule reassign: reason=%s, err=(%s)", om.actorID, reason, err)
		}
		nilOrReassignRetryTimerCh = time.After(om.f.config.Consumer.Retry.Backoff)
	}
	for {
		select {
		case bw := <-om.assignmentCh:
			if bw == nil {
				assignedBrokerRequestsCh = nil
				triggerOrScheduleReassign(ErrNoCoordinator, "retry reassignment")
				continue
			}
			be := bw.(*brokerExecutor)
			nilOrReassignRetryTimerCh = nil
			assignedBrokerRequestsCh = be.requestsCh

			if !initialOffsetFetched {
				initialOffset, err := om.fetchInitialOffset(be.conn)
				if err != nil {
					triggerOrScheduleReassign(err, "failed to fetch initial offset")
					continue
				}
				om.initialOffsetCh <- initialOffset
				close(om.initialOffsetCh)
				initialOffsetFetched = true
			}
			if isDirty {
				nilOrBrokerRequestsCh = assignedBrokerRequestsCh
			}
		case so, ok := <-nilOrSubmittedOffsetsCh:
			if !ok {
				if isDirty || isPending {
					closed, nilOrSubmittedOffsetsCh = true, nil
					continue
				}
				return
			}
			log.Infof("*** received %v", so)
			lastSubmitRequest = so
			lastSubmitRequest.resultCh = commitResultCh
			isDirty = true
			nilOrBrokerRequestsCh = assignedBrokerRequestsCh

		case nilOrBrokerRequestsCh <- lastSubmitRequest:
			nilOrBrokerRequestsCh = nil
			isDirty = false
			if !isPending {
				isPending, lastSubmitTime = true, time.Now().UTC()
			}
		case cr := <-commitResultCh:
			isPending = false
			if err := om.getCommitError(cr.response); err != nil {
				isDirty = true
				triggerOrScheduleReassign(err, "offset commit failed")
				continue
			}
			om.committedOffsetsCh <- DecoratedOffset{cr.offsetCommit.offset, cr.offsetCommit.metadata}
			if closed && !isDirty {
				return
			}
		case <-commitTicker.C:
			if isPending && time.Now().UTC().Sub(lastSubmitTime) > brokerRequestTimeout {
				isDirty, isPending = true, false
				triggerOrScheduleReassign(ErrRequestTimeout, "offset commit failed")
			}
		case <-nilOrReassignRetryTimerCh:
			om.f.mapper.WorkerReassign() <- om
			log.Infof("<%s> reassign triggered by timeout", om.actorID)
			nilOrReassignRetryTimerCh = time.After(om.f.config.Consumer.Retry.Backoff)
		}
	}
}

func (om *offsetManager) fetchInitialOffset(conn *sarama.Broker) (DecoratedOffset, error) {
	request := new(sarama.OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = om.gtp.group
	request.AddPartition(om.gtp.topic, om.gtp.partition)

	response, err := conn.FetchOffset(request)
	if err != nil {
		// In case of network error the connection has to be explicitly closed,
		// otherwise it won't be re-establish and following requests to this
		// broker will fail as well.
		_ = conn.Close()
		return DecoratedOffset{}, err
	}
	block := response.GetBlock(om.gtp.topic, om.gtp.partition)
	if block == nil {
		return DecoratedOffset{}, sarama.ErrIncompleteResponse
	}
	if block.Err != sarama.ErrNoError {
		return DecoratedOffset{}, block.Err
	}
	fetchedOffset := DecoratedOffset{block.Offset, block.Metadata}
	return fetchedOffset, nil
}

func (om *offsetManager) reportError(err error) {
	if !om.f.config.Consumer.Return.Errors {
		return
	}
	oce := &OffsetCommitError{
		Group:     om.gtp.group,
		Topic:     om.gtp.topic,
		Partition: om.gtp.partition,
		Err:       err,
	}
	select {
	case om.errorsCh <- oce:
	default:
	}
}

func (om *offsetManager) getCommitError(res *sarama.OffsetCommitResponse) error {
	if res.Errors[om.gtp.topic] == nil {
		return sarama.ErrIncompleteResponse
	}
	err, ok := res.Errors[om.gtp.topic][om.gtp.partition]
	if !ok {
		return sarama.ErrIncompleteResponse
	}
	if err != sarama.ErrNoError {
		return err
	}
	return nil
}

type submitRequest struct {
	gtp      groupTopicPartition
	offset   int64
	metadata string
	resultCh chan<- submitResponse
}

type submitResponse struct {
	offsetCommit submitRequest
	response     *sarama.OffsetCommitResponse
}

// brokerExecutor aggregates submitted offsets from partition offset managers
// and periodically commits them to Kafka.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	aggrActorID     *actor.ID
	execActorID     *actor.ID
	config          *sarama.Config
	conn            *sarama.Broker
	requestsCh      chan submitRequest
	batchRequestsCh chan map[string]map[groupTopicPartition]submitRequest
	wg              sync.WaitGroup
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

func (be *brokerExecutor) runAggregator() {
	defer close(be.batchRequestsCh)

	batchRequests := make(map[string]map[groupTopicPartition]submitRequest)
	var nilOrOffsetBatchesCh chan map[string]map[groupTopicPartition]submitRequest
	for {
		select {
		case submitReq, ok := <-be.requestsCh:
			if !ok {
				return
			}
			groupRequests := batchRequests[submitReq.gtp.group]
			if groupRequests == nil {
				groupRequests = make(map[groupTopicPartition]submitRequest)
				batchRequests[submitReq.gtp.group] = groupRequests
			}
			groupRequests[submitReq.gtp] = submitReq
			nilOrOffsetBatchesCh = be.batchRequestsCh
		case nilOrOffsetBatchesCh <- batchRequests:
			nilOrOffsetBatchesCh = nil
			batchRequests = make(map[string]map[groupTopicPartition]submitRequest)
		}
	}
}

func (be *brokerExecutor) runExecutor() {
	var nilOrBatchRequestsCh chan map[string]map[groupTopicPartition]submitRequest
	var lastErr error
	var lastErrTime time.Time
	commitTicker := time.NewTicker(be.config.Consumer.Offsets.CommitInterval)
	defer commitTicker.Stop()
offsetCommitLoop:
	for {
		select {
		case <-commitTicker.C:
			nilOrBatchRequestsCh = be.batchRequestsCh
		case batchRequest, ok := <-nilOrBatchRequestsCh:
			if !ok {
				return
			}
			// Ignore submit requests for awhile after a connection failure to
			// allow the Kafka cluster some time to recuperate. Ignored requests
			// will be retried by originating partition offset managers.
			if time.Now().UTC().Sub(lastErrTime) < be.config.Consumer.Retry.Backoff {
				continue offsetCommitLoop
			}
			nilOrBatchRequestsCh = nil
			for group, groupRequests := range batchRequest {
				req := &sarama.OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
				}
				for _, submitReq := range groupRequests {
					req.AddBlock(submitReq.gtp.topic, submitReq.gtp.partition, submitReq.offset, sarama.ReceiveTime, submitReq.metadata)
				}
				var res *sarama.OffsetCommitResponse
				res, lastErr = be.conn.CommitOffset(req)
				if lastErr != nil {
					lastErrTime = time.Now().UTC()
					be.conn.Close()
					log.Infof("<%s> connection reset: err=(%v)", be.execActorID, lastErr)
					continue offsetCommitLoop
				}
				// Fan the response out to the partition offset managers.
				for _, submitReq := range groupRequests {
					submitReq.resultCh <- submitResponse{submitReq, res}
				}
			}
		}
	}
}

func (be *brokerExecutor) String() string {
	if be == nil {
		return "<nil>"
	}
	return be.aggrActorID.String()
}
