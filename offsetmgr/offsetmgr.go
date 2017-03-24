package offsetmgr

import (
	"math"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer/mapper"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
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
	// SubmitOffset triggers saving of the specified offset in Kafka. Commits are
	// performed periodically in a background goroutine. The commit interval is
	// configured by `Config.Consumer.Offsets.CommitInterval`. Note that not every
	// submitted offset gets committed. Committed offsets are sent down to the
	// `CommittedOffsets()` channel. The `CommittedOffsets()` channel has to be
	// read alongside with submitting offsets, otherwise the partition offset
	// manager will block.
	SubmitOffset(offset Offset)

	// CommittedOffsets returns a channel that offsets committed to Kafka are
	// sent to. The first offset sent to this channel is the initial offset
	// fetched from Kafka for the group+topic+partition. The user must read
	// from this channel otherwise the `SubmitOffset` function will eventually
	// block forever.
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

var (
	errNoCoordinator  = errors.New("failed to resolve coordinator")
	errRequestTimeout = errors.New("request timeout")

	// To be used in tests only! If true then offset manager will initialize
	// their errors channel and will send internal errors.
	testReportErrors bool
)

// SpawnFactory creates a new offset manager factory from the given client.
func SpawnFactory(namespace *actor.ID, cfg *config.Proxy, kafkaClt sarama.Client) Factory {
	f := &factory{
		namespace: namespace.NewChild("offset_mgr_f"),
		kafkaClt:  kafkaClt,
		cfg:       cfg,
		children:  make(map[instanceID]*offsetMgr),
	}
	f.mapper = mapper.Spawn(f.namespace, f)
	return f
}

// implements `Factory`
// implements `mapper.Resolver`
type factory struct {
	namespace *actor.ID
	kafkaClt  sarama.Client
	cfg       *config.Proxy
	mapper    *mapper.T

	childrenMu sync.Mutex
	children   map[instanceID]*offsetMgr
}

type instanceID struct {
	group     string
	topic     string
	partition int32
}

// implements `Factory`
func (f *factory) SpawnOffsetManager(namespace *actor.ID, group, topic string, partition int32) (T, error) {
	id := instanceID{group, topic, partition}

	f.childrenMu.Lock()
	defer f.childrenMu.Unlock()
	if _, ok := f.children[id]; ok {
		return nil, errors.Errorf("offset manager %v already exists", id)
	}
	om := f.spawnOffsetManager(namespace, id)
	f.mapper.OnWorkerSpawned(om)
	f.children[id] = om
	return om, nil
}

// implements `mapper.Resolver`.
func (f *factory) ResolveBroker(pw mapper.Worker) (*sarama.Broker, error) {
	om := pw.(*offsetMgr)
	if err := f.kafkaClt.RefreshCoordinator(om.id.group); err != nil {
		return nil, err
	}

	brokerConn, err := f.kafkaClt.Coordinator(om.id.group)
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
		requestsCh:      make(chan submitReq),
		batchRequestsCh: make(chan map[string]map[instanceID]submitReq),
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
		submitRequestsCh:   make(chan submitReq),
		assignmentCh:       make(chan mapper.Executor, 1),
		committedOffsetsCh: make(chan Offset, f.cfg.Consumer.ChannelBufferSize),
	}
	if testReportErrors {
		om.testErrorsCh = make(chan error, f.cfg.Consumer.ChannelBufferSize)
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
	submitRequestsCh   chan submitReq
	assignmentCh       chan mapper.Executor
	committedOffsetsCh chan Offset
	wg                 sync.WaitGroup

	assignedBrokerRequestsCh  chan<- submitReq
	nilOrBrokerRequestsCh     chan<- submitReq
	nilOrReassignRetryTimerCh <-chan time.Time
	lastReassignTime          time.Time

	// To be used in tests only!
	testErrorsCh chan error
}

// implements `T`.
func (om *offsetMgr) SubmitOffset(offset Offset) {
	om.submitRequestsCh <- submitReq{
		id:     om.id,
		offset: offset,
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

	om.f.childrenMu.Lock()
	delete(om.f.children, om.id)
	om.f.childrenMu.Unlock()
	om.f.mapper.OnWorkerStopped(om)
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
		lastCommittedOffset   = Offset{Val: math.MinInt64}
		lastSubmitRequest     = submitReq{offset: lastCommittedOffset}
		nilOrSubmitRequestsCh = om.submitRequestsCh
		submitResponseCh      = make(chan submitRes, 1)
		initialOffsetFetched  = false
		stopped               = false
		commitTicker          = time.NewTicker(om.f.cfg.Consumer.OffsetsCommitInterval)
		offsetCommitTimeout   = om.f.cfg.Consumer.OffsetsCommitInterval * 3
		lastSubmitTime        time.Time
	)
	defer commitTicker.Stop()
	for {
		select {
		case bw := <-om.assignmentCh:
			log.Infof("<%s> assigned %s", om.actorID, bw)
			if bw == nil {
				om.triggerOrScheduleReassign(errNoCoordinator, "no broker assigned")
				continue
			}
			om.nilOrReassignRetryTimerCh = nil

			be := bw.(*brokerExecutor)
			om.assignedBrokerRequestsCh = be.requestsCh

			if !initialOffsetFetched {
				initialOffset, err := om.fetchInitialOffset(be.conn)
				if err != nil {
					om.triggerOrScheduleReassign(err, "failed to fetch initial offset")
					continue
				}
				om.committedOffsetsCh <- initialOffset
				initialOffsetFetched = true
			}
			if lastSubmitRequest.offset != lastCommittedOffset {
				om.nilOrBrokerRequestsCh = om.assignedBrokerRequestsCh
			}
		case submitReq, ok := <-nilOrSubmitRequestsCh:
			if !ok {
				if lastSubmitRequest.offset == lastCommittedOffset {
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
			lastSubmitTime = time.Now().UTC()

		case submitRes := <-submitResponseCh:
			if err := om.getCommitError(submitRes.kafkaRes); err != nil {
				om.triggerOrScheduleReassign(err, "offset commit failed")
				continue
			}
			lastCommittedOffset = submitRes.req.offset
			om.committedOffsetsCh <- lastCommittedOffset
			if stopped && lastSubmitRequest.offset == lastCommittedOffset {
				return
			}
		case <-commitTicker.C:
			isRequestTimeout := time.Now().UTC().Sub(lastSubmitTime) > offsetCommitTimeout
			if isRequestTimeout && lastSubmitRequest.offset != lastCommittedOffset {
				om.triggerOrScheduleReassign(errRequestTimeout, "offset commit failed")
			}
		case <-om.nilOrReassignRetryTimerCh:
			om.f.mapper.TriggerReassign(om)
			log.Infof("<%s> reassign triggered by timeout", om.actorID)
			om.nilOrReassignRetryTimerCh = time.After(om.f.cfg.Consumer.RetryBackoff)
		}
	}
}

func (om *offsetMgr) triggerOrScheduleReassign(err error, reason string) {
	om.reportError(err)
	om.assignedBrokerRequestsCh = nil
	om.nilOrBrokerRequestsCh = nil
	now := time.Now().UTC()
	if now.Sub(om.lastReassignTime) > om.f.cfg.Consumer.RetryBackoff {
		log.Infof("<%s> trigger reassign: reason=%s, err=(%s)", om.actorID, reason, err)
		om.lastReassignTime = now
		om.f.mapper.TriggerReassign(om)
	} else {
		log.Infof("<%s> schedule reassign: reason=%s, err=(%s)", om.actorID, reason, err)
	}
	om.nilOrReassignRetryTimerCh = time.After(om.f.cfg.Consumer.RetryBackoff)
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
	select {
	case om.testErrorsCh <- err:
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

type submitReq struct {
	id       instanceID
	offset   Offset
	resultCh chan<- submitRes
}

type submitRes struct {
	req      submitReq
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
	requestsCh      chan submitReq
	batchRequestsCh chan map[string]map[instanceID]submitReq
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

	batchRequests := make(map[string]map[instanceID]submitReq)
	var nilOrOffsetBatchesCh chan map[string]map[instanceID]submitReq
	for {
		select {
		case req, ok := <-be.requestsCh:
			if !ok {
				return
			}
			groupRequests := batchRequests[req.id.group]
			if groupRequests == nil {
				groupRequests = make(map[instanceID]submitReq)
				batchRequests[req.id.group] = groupRequests
			}
			groupRequests[req.id] = req
			nilOrOffsetBatchesCh = be.batchRequestsCh
		case nilOrOffsetBatchesCh <- batchRequests:
			nilOrOffsetBatchesCh = nil
			batchRequests = make(map[string]map[instanceID]submitReq)
		}
	}
}

func (be *brokerExecutor) runExecutor() {
	var nilOrBatchRequestsCh chan map[string]map[instanceID]submitReq
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
			if time.Now().UTC().Sub(lastErrTime) < be.cfg.Consumer.RetryBackoff {
				continue offsetCommitLoop
			}
			nilOrBatchRequestsCh = nil
			for group, groupRequests := range batchRequest {
				kafkaReq := &sarama.OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
				}
				for _, req := range groupRequests {
					kafkaReq.AddBlock(req.id.topic, req.id.partition, req.offset.Val, sarama.ReceiveTime, req.offset.Meta)
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
				for _, req := range groupRequests {
					req.resultCh <- submitRes{req, kafkaRes}
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
