package offsetmgr

import (
	"errors"
	"math"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
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
	InitialOffset() <-chan Offset

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
	CommittedOffsets() <-chan Offset

	// Stop stops the offset manager. It is required to stop all spawned offset
	// managers before their parent factory can be stopped.
	//
	// It is guaranteed that the most recent offset is committed before `Stop`
	// returns.
	Stop()
}

// Offset represents an offset data as it is stored in Kafka, that is an offset
// value decorated with a metadata string.
type Offset struct {
	Val  int64
	Meta string
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
func SpawnFactory(namespace *actor.ID, cfg *config.Proxy, client sarama.Client) Factory {
	f := &factory{
		namespace: namespace.NewChild("offset_mgr_f"),
		client:    client,
		cfg:       cfg,
		children:  make(map[instanceID]*offsetMgr),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f
}

// implements `Factory`
// implements `mapper.Resolver`
type factory struct {
	namespace    *actor.ID
	client       sarama.Client
	cfg          *config.Proxy
	mapper       *mapper.T
	children     map[instanceID]*offsetMgr
	childrenLock sync.Mutex

	// To be used in tests only!
	testReportErrors bool
}

type instanceID struct {
	group     string
	topic     string
	partition int32
}

// implements `Factory`
func (f *factory) SpawnOffsetManager(namespace *actor.ID, group, topic string, partition int32) (T, error) {
	id := instanceID{group, topic, partition}

	f.childrenLock.Lock()
	defer f.childrenLock.Unlock()
	if _, ok := f.children[id]; ok {
		return nil, sarama.ConfigurationError("This group/topic/partition is already being managed")
	}
	om := f.spawnOffsetManager(namespace, id)
	f.mapper.WorkerSpawned() <- om
	f.children[id] = om
	return om, nil
}

// implements `mapper.Resolver`.
func (f *factory) ResolveBroker(pw mapper.Worker) (*sarama.Broker, error) {
	om := pw.(*offsetMgr)
	if err := f.client.RefreshCoordinator(om.id.group); err != nil {
		return nil, err
	}

	brokerConn, err := f.client.Coordinator(om.id.group)
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
		cfg:             f.cfg,
		conn:            brokerConn,
		requestsCh:      make(chan submitRequest),
		batchRequestsCh: make(chan map[string]map[instanceID]submitRequest),
	}
	actor.Spawn(be.aggrActorID, &be.wg, be.runAggregator)
	actor.Spawn(be.execActorID, &be.wg, be.runExecutor)
	return be
}

func (f *factory) spawnOffsetManager(namespace *actor.ID, id instanceID) *offsetMgr {
	om := &offsetMgr{
		actorID:            namespace.NewChild("offset_mgr"),
		f:                  f,
		id:                 id,
		initialOffsetCh:    make(chan Offset, 1),
		submitRequestsCh:   make(chan submitRequest),
		assignmentCh:       make(chan mapper.Executor, 1),
		committedOffsetsCh: make(chan Offset, f.cfg.Consumer.ChannelBufferSize),
	}
	if f.testReportErrors {
		om.testErrorsCh = make(chan *OffsetCommitError, f.cfg.Consumer.ChannelBufferSize)
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
type offsetMgr struct {
	actorID            *actor.ID
	f                  *factory
	id                 instanceID
	initialOffsetCh    chan Offset
	submitRequestsCh   chan submitRequest
	assignmentCh       chan mapper.Executor
	committedOffsetsCh chan Offset
	wg                 sync.WaitGroup

	assignedBrokerRequestsCh  chan<- submitRequest
	nilOrBrokerRequestsCh     chan<- submitRequest
	nilOrReassignRetryTimerCh <-chan time.Time
	lastSubmitTime            time.Time
	lastReassignTime          time.Time

	// To be used in tests only!
	testErrorsCh chan *OffsetCommitError
}

// implements `T`.
func (om *offsetMgr) InitialOffset() <-chan Offset {
	return om.initialOffsetCh
}

// implements `T`.
func (om *offsetMgr) SubmitOffset(offset int64, metadata string) {
	om.submitRequestsCh <- submitRequest{
		id:       om.id,
		offset:   offset,
		metadata: metadata,
	}
}

// implements `T`.
func (om *offsetMgr) CommittedOffsets() <-chan Offset {
	return om.committedOffsetsCh
}

// implements `T`.
func (om *offsetMgr) Stop() {
	close(om.submitRequestsCh)
	om.wg.Wait()

	om.f.childrenLock.Lock()
	delete(om.f.children, om.id)
	om.f.childrenLock.Unlock()
	om.f.mapper.WorkerStopped() <- om
}

// implements `mapper.Worker`.
func (om *offsetMgr) Assignment() chan<- mapper.Executor {
	return om.assignmentCh
}

func (om *offsetMgr) String() string {
	return om.actorID.String()
}

func (om *offsetMgr) run() {
	defer close(om.committedOffsetsCh)
	if om.testErrorsCh != nil {
		defer close(om.testErrorsCh)
	}
	var (
		lastSubmitRequest     = submitRequest{offset: math.MinInt64}
		lastCommittedOffset   = Offset{Val: math.MinInt64}
		nilOrSubmitRequestsCh = om.submitRequestsCh
		submitResponseCh      = make(chan submitResponse, 1)
		initialOffsetFetched  = false
		stopped               = false
		commitTicker          = time.NewTicker(om.f.cfg.Consumer.OffsetsCommitInterval)
		offsetCommitTimeout   = om.f.cfg.Consumer.OffsetsCommitInterval * 3
	)
	defer commitTicker.Stop()
	for {
		select {
		case bw := <-om.assignmentCh:
			if bw == nil {
				om.assignedBrokerRequestsCh = nil
				om.triggerOrScheduleReassign(ErrNoCoordinator, "retry reassignment")
				continue
			}
			be := bw.(*brokerExecutor)
			om.nilOrReassignRetryTimerCh = nil
			om.assignedBrokerRequestsCh = be.requestsCh

			if !initialOffsetFetched {
				initialOffset, err := om.fetchInitialOffset(be.conn)
				if err != nil {
					om.triggerOrScheduleReassign(err, "failed to fetch initial offset")
					continue
				}
				om.initialOffsetCh <- initialOffset
				close(om.initialOffsetCh)
				initialOffsetFetched = true
			}
			if !isSameDecoratedOffset(lastSubmitRequest, lastCommittedOffset) {
				om.nilOrBrokerRequestsCh = om.assignedBrokerRequestsCh
			}
		case submitReq, ok := <-nilOrSubmitRequestsCh:
			if !ok {
				if isSameDecoratedOffset(lastSubmitRequest, lastCommittedOffset) {
					return
				}
				stopped, nilOrSubmitRequestsCh = true, nil
				continue
			}
			lastSubmitRequest = submitReq
			lastSubmitRequest.resultCh = submitResponseCh
			om.nilOrBrokerRequestsCh = om.assignedBrokerRequestsCh

		case om.nilOrBrokerRequestsCh <- lastSubmitRequest:
			om.nilOrBrokerRequestsCh = nil
			om.lastSubmitTime = time.Now().UTC()

		case submitRes := <-submitResponseCh:
			if err := om.getCommitError(submitRes.kafkaRes); err != nil {
				om.triggerOrScheduleReassign(err, "offset commit failed")
				continue
			}
			lastCommittedOffset = Offset{submitRes.req.offset, submitRes.req.metadata}
			om.committedOffsetsCh <- lastCommittedOffset
			if stopped && isSameDecoratedOffset(lastSubmitRequest, lastCommittedOffset) {
				return
			}
		case <-commitTicker.C:
			isRequestTimeout := time.Now().UTC().Sub(om.lastSubmitTime) > offsetCommitTimeout
			if isRequestTimeout && !isSameDecoratedOffset(lastSubmitRequest, lastCommittedOffset) {
				om.triggerOrScheduleReassign(ErrRequestTimeout, "offset commit failed")
			}
		case <-om.nilOrReassignRetryTimerCh:
			om.f.mapper.WorkerReassign() <- om
			log.Infof("<%s> reassign triggered by timeout", om.actorID)
			om.nilOrReassignRetryTimerCh = time.After(om.f.cfg.Consumer.BackOffTimeout)
		}
	}
}

func (om *offsetMgr) triggerOrScheduleReassign(err error, reason string) {
	om.reportError(err)
	om.assignedBrokerRequestsCh = nil
	om.nilOrBrokerRequestsCh = nil
	now := time.Now().UTC()
	if now.Sub(om.lastReassignTime) > om.f.cfg.Consumer.BackOffTimeout {
		log.Infof("<%s> trigger reassign: reason=%s, err=(%s)", om.actorID, reason, err)
		om.lastReassignTime = now
		om.f.mapper.WorkerReassign() <- om
	} else {
		log.Infof("<%s> schedule reassign: reason=%s, err=(%s)", om.actorID, reason, err)
	}
	om.nilOrReassignRetryTimerCh = time.After(om.f.cfg.Consumer.BackOffTimeout)
}

func (om *offsetMgr) fetchInitialOffset(conn *sarama.Broker) (Offset, error) {
	request := new(sarama.OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = om.id.group
	request.AddPartition(om.id.topic, om.id.partition)

	response, err := conn.FetchOffset(request)
	if err != nil {
		// In case of network error the connection has to be explicitly closed,
		// otherwise it won't be re-establish and following requests to this
		// broker will fail as well.
		_ = conn.Close()
		return Offset{}, err
	}
	block := response.GetBlock(om.id.topic, om.id.partition)
	if block == nil {
		return Offset{}, sarama.ErrIncompleteResponse
	}
	if block.Err != sarama.ErrNoError {
		return Offset{}, block.Err
	}
	fetchedOffset := Offset{block.Offset, block.Metadata}
	return fetchedOffset, nil
}

func (om *offsetMgr) reportError(err error) {
	if om.testErrorsCh == nil {
		return
	}
	oce := &OffsetCommitError{
		Group:     om.id.group,
		Topic:     om.id.topic,
		Partition: om.id.partition,
		Err:       err,
	}
	select {
	case om.testErrorsCh <- oce:
	default:
	}
}

func (om *offsetMgr) getCommitError(res *sarama.OffsetCommitResponse) error {
	if res.Errors[om.id.topic] == nil {
		return sarama.ErrIncompleteResponse
	}
	err, ok := res.Errors[om.id.topic][om.id.partition]
	if !ok {
		return sarama.ErrIncompleteResponse
	}
	if err != sarama.ErrNoError {
		return err
	}
	return nil
}

type submitRequest struct {
	id       instanceID
	offset   int64
	metadata string
	resultCh chan<- submitResponse
}

type submitResponse struct {
	req      submitRequest
	kafkaRes *sarama.OffsetCommitResponse
}

// brokerExecutor aggregates submitted offsets from partition offset managers
// and periodically commits them to Kafka.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	aggrActorID     *actor.ID
	execActorID     *actor.ID
	cfg             *config.Proxy
	conn            *sarama.Broker
	requestsCh      chan submitRequest
	batchRequestsCh chan map[string]map[instanceID]submitRequest
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

	batchRequests := make(map[string]map[instanceID]submitRequest)
	var nilOrOffsetBatchesCh chan map[string]map[instanceID]submitRequest
	for {
		select {
		case submitReq, ok := <-be.requestsCh:
			if !ok {
				return
			}
			groupRequests := batchRequests[submitReq.id.group]
			if groupRequests == nil {
				groupRequests = make(map[instanceID]submitRequest)
				batchRequests[submitReq.id.group] = groupRequests
			}
			groupRequests[submitReq.id] = submitReq
			nilOrOffsetBatchesCh = be.batchRequestsCh
		case nilOrOffsetBatchesCh <- batchRequests:
			nilOrOffsetBatchesCh = nil
			batchRequests = make(map[string]map[instanceID]submitRequest)
		}
	}
}

func (be *brokerExecutor) runExecutor() {
	var nilOrBatchRequestsCh chan map[string]map[instanceID]submitRequest
	var lastErr error
	var lastErrTime time.Time
	commitTicker := time.NewTicker(be.cfg.Consumer.OffsetsCommitInterval)
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
			if time.Now().UTC().Sub(lastErrTime) < be.cfg.Consumer.BackOffTimeout {
				continue offsetCommitLoop
			}
			nilOrBatchRequestsCh = nil
			for group, groupRequests := range batchRequest {
				kafkaReq := &sarama.OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
				}
				for _, submitReq := range groupRequests {
					kafkaReq.AddBlock(submitReq.id.topic, submitReq.id.partition, submitReq.offset, sarama.ReceiveTime, submitReq.metadata)
				}
				var kafkaRes *sarama.OffsetCommitResponse
				kafkaRes, lastErr = be.conn.CommitOffset(kafkaReq)
				if lastErr != nil {
					lastErrTime = time.Now().UTC()
					be.conn.Close()
					log.Infof("<%s> connection reset: err=(%v)", be.execActorID, lastErr)
					continue offsetCommitLoop
				}
				// Fan the response out to the partition offset managers.
				for _, submitReq := range groupRequests {
					submitReq.resultCh <- submitResponse{submitReq, kafkaRes}
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

func isSameDecoratedOffset(r submitRequest, o Offset) bool {
	return r.offset == o.Val && r.metadata == o.Meta
}
