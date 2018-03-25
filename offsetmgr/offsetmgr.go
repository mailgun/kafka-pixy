package offsetmgr

import (
	"math"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/mapper"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/pkg/errors"
)

// Factory provides a method to spawn offset manager instances to commit
// offsets for a particular group-topic-partition. It makes sure that there is
// only one running manager instance for a particular group-topic-partition
// combination.
//
// One Factory instance per application is usually more then enough, but it is
// possible to create many of them.
//
// Factory spawns background goroutines so it must be explicitly stopped by the
// application. But first it should explicitly stop all spawned offset manager
// instances.
type Factory interface {
	// Span creates and starts an offset manager for a group-topic-partition.
	// It returns an error if given group-topic-partition has a running
	// OffsetManager instance already. After an old offset manager instance is
	// stopped a new one can be started.
	Spawn(parentActDesc *actor.Descriptor, group, topic string, partition int32) (T, error)

	// Stop waits for the spawned offset managers to stop and then terminates. Note
	// that all spawned offset managers has to be explicitly stopped by calling
	// their Stop method.
	Stop()
}

// T provides interface to store and retrieve offsets for a particular
// group-topic-partition in Kafka.
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
	// fetched from Kafka for the group-topic-partition. The user must read
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
	undefinedOffset = Offset{Val: math.MaxInt64}

	// To be used in tests only! If true then offset manager will initialize
	// their errors channel and will send internal errors.
	testReportErrors bool

	errRequestTimeout = errors.New("request timeout")
)

// SpawnFactory creates a new offset manager factory from the given client.
func SpawnFactory(parentActDesc *actor.Descriptor, cfg *config.Proxy, kafkaClt sarama.Client) Factory {
	f := &factory{
		actDesc:  parentActDesc.NewChild("offset_mgr_f"),
		kafkaClt: kafkaClt,
		cfg:      cfg,
		children: make(map[instanceID]*offsetMgr),
	}
	f.mapper = mapper.Spawn(f.actDesc, cfg, f)
	return f
}

// implements `Factory`
// implements `mapper.Resolver`
type factory struct {
	actDesc  *actor.Descriptor
	kafkaClt sarama.Client
	cfg      *config.Proxy
	mapper   *mapper.T

	childrenMu sync.Mutex
	children   map[instanceID]*offsetMgr
}

type instanceID struct {
	group     string
	topic     string
	partition int32
}

// implements `Factory`
func (f *factory) Spawn(namespace *actor.Descriptor, group, topic string, partition int32) (T, error) {
	id := instanceID{group, topic, partition}

	f.childrenMu.Lock()
	defer f.childrenMu.Unlock()
	if _, ok := f.children[id]; ok {
		return nil, errors.Errorf("offset manager %v already exists", id)
	}
	actDesc := namespace.NewChild("offset_mgr")
	actDesc.AddLogField("kafka.group", group)
	actDesc.AddLogField("kafka.topic", topic)
	actDesc.AddLogField("kafka.partition", partition)
	om := &offsetMgr{
		actDesc:            actDesc,
		f:                  f,
		id:                 id,
		submitRequestsCh:   make(chan submitRq),
		assignmentCh:       make(chan mapper.Executor, 1),
		committedOffsetsCh: make(chan Offset, f.cfg.Consumer.ChannelBufferSize),
	}
	if testReportErrors {
		om.testErrorsCh = make(chan error, f.cfg.Consumer.ChannelBufferSize)
	}

	om.retryTimer = time.NewTimer(0)
	<-om.retryTimer.C

	f.children[id] = om
	actor.Spawn(om.actDesc, &om.wg, om.run)
	return om, nil
}

// implements `mapper.Resolver`.
func (f *factory) ResolveBroker(worker mapper.Worker) (*sarama.Broker, error) {
	om := worker.(*offsetMgr)
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
		aggrActDesc:       f.actDesc.NewChild("broker", brokerConn.ID(), "aggr"),
		execActDesc:       f.actDesc.NewChild("broker", brokerConn.ID(), "exec"),
		cfg:               f.cfg,
		conn:              brokerConn,
		requestsCh:        make(chan submitRq),
		requestBatchesCh:  make(chan map[string]map[instanceID]submitRq),
		flushAggregatorCh: make(chan none.T, 1),
		flushExecutorCh:   make(chan none.T, 1),
		execStopCh:        make(chan none.T),
	}
	actor.Spawn(be.aggrActDesc, &be.wg, be.runAggregator)
	actor.Spawn(be.execActDesc, &be.wg, be.runExecutor)
	return be
}

// implements `Factory.Stop()`
func (f *factory) Stop() {
	f.mapper.Stop()
}

func (f *factory) onOffsetMgrSpawned(om *offsetMgr) {
	f.mapper.OnWorkerSpawned(om)
}

func (f *factory) onOffsetMgrStopped(om *offsetMgr) {
	f.childrenMu.Lock()
	delete(f.children, om.id)
	f.childrenMu.Unlock()
	f.mapper.OnWorkerStopped(om)
}

// implements `T`
// implements `mapper.Worker`
type offsetMgr struct {
	actDesc               *actor.Descriptor
	f                     *factory
	id                    instanceID
	submitRequestsCh      chan submitRq
	assignmentCh          chan mapper.Executor
	committedOffsetsCh    chan Offset
	brokerRequestsCh      chan<- submitRq
	nilOrBrokerRequestsCh chan<- submitRq
	retryTimer            *time.Timer
	nilOrRetryTimerCh     <-chan time.Time
	wg                    sync.WaitGroup

	// To be used in tests only!
	testErrorsCh chan error
}

// implements `T`.
func (om *offsetMgr) SubmitOffset(offset Offset) {
	om.submitRequestsCh <- submitRq{
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
}

// implements `mapper.Worker`.
func (om *offsetMgr) Assignment() chan<- mapper.Executor {
	return om.assignmentCh
}

func (om *offsetMgr) String() string {
	return om.actDesc.String()
}

func (om *offsetMgr) run() {
	defer close(om.committedOffsetsCh)
	if om.testErrorsCh != nil {
		defer close(om.testErrorsCh)
	}
	defer om.retryTimer.Stop()
	om.f.onOffsetMgrSpawned(om)
	defer om.f.onOffsetMgrStopped(om)

	var (
		initialOffset   = undefinedOffset
		receivedRq      = submitRq{offset: undefinedOffset}
		nilOrRequestsCh = om.submitRequestsCh
		responseCh      = make(chan submitRs, 1)
		stopped         = false
	)
	// Retrieve the initial offset.
	for {
		select {
		case bw := <-om.assignmentCh:
			om.actDesc.Log().Infof("Assigned executor: %s", bw)
			be := bw.(*brokerExecutor)
			om.brokerRequestsCh = be.requestsCh

			initialOffset, err := om.fetchInitialOffset(be.conn)
			if err != nil {
				om.triggerReassign(err, "Failed to fetch initial offset")
				continue
			}
			om.committedOffsetsCh <- initialOffset
			goto handleRequests

		case rq, ok := <-nilOrRequestsCh:
			if !ok {
				// It was signalled to stop, but return only if there is no
				// uncommitted offset, otherwise keep running.
				if receivedRq.offset == initialOffset {
					return
				}
				stopped = true
				nilOrRequestsCh = nil
				continue
			}
			receivedRq = rq
			receivedRq.resultCh = responseCh
		}
	}
handleRequests:
	committedOffset := initialOffset
	if receivedRq.offset == undefinedOffset {
		receivedRq.offset = committedOffset
	}
	if receivedRq.offset != committedOffset {
		om.nilOrBrokerRequestsCh = om.brokerRequestsCh
	}
	var handedOffRq submitRq
	var handOffTime time.Time
	var be *brokerExecutor
	offsetCommitTimeout := om.f.cfg.Consumer.OffsetsCommitInterval * 2
	for {
		select {
		case bw := <-om.assignmentCh:
			om.actDesc.Log().Infof("Assigned executor: %s", bw)
			be = bw.(*brokerExecutor)
			om.brokerRequestsCh = be.requestsCh

			if receivedRq.offset != committedOffset {
				om.nilOrBrokerRequestsCh = om.brokerRequestsCh
			}
		case rq, ok := <-nilOrRequestsCh:
			if !ok {
				if receivedRq.offset == committedOffset {
					return
				}
				// Keep running until the last submitter offset is committed.
				stopped = true
				// Signal broker executor to flush offsets to Kafka.
				if be != nil {
					be.flushAggregatorCh <- none.V
				}
				nilOrRequestsCh = nil
				continue
			}
			receivedRq = rq
			receivedRq.resultCh = responseCh
			om.nilOrBrokerRequestsCh = om.brokerRequestsCh

		case om.nilOrBrokerRequestsCh <- receivedRq:
			om.nilOrBrokerRequestsCh = nil
			handedOffRq = receivedRq
			handOffTime = time.Now()
			if om.nilOrRetryTimerCh == nil {
				om.retryTimer.Reset(offsetCommitTimeout)
				om.nilOrRetryTimerCh = om.retryTimer.C
			}
		case rs := <-responseCh:
			if err := om.getCommitError(rs); err != nil {
				om.triggerReassign(err, "Request failed")
				continue
			}
			committedOffset = rs.offset
			om.committedOffsetsCh <- committedOffset
			if stopped && receivedRq.offset == committedOffset {
				return
			}
		case <-om.nilOrRetryTimerCh:
			om.nilOrRetryTimerCh = nil
			sinceHandOff := time.Since(handOffTime)
			if sinceHandOff >= offsetCommitTimeout {
				if handedOffRq.offset == committedOffset {
					continue
				}
				om.triggerReassign(errRequestTimeout, "Request timeout %v", sinceHandOff)
				continue
			}
			timeoutLeft := offsetCommitTimeout - sinceHandOff
			om.retryTimer.Reset(timeoutLeft)
			om.nilOrRetryTimerCh = om.retryTimer.C
		}
	}
}

func (om *offsetMgr) stopRetryTimer() {
	if om.nilOrRetryTimerCh == nil {
		return
	}
	if !om.retryTimer.Stop() {
		<-om.retryTimer.C
	}
	om.nilOrRetryTimerCh = nil
}

func (om *offsetMgr) triggerReassign(err error, format string, args ...interface{}) {
	om.actDesc.Log().WithError(err).Errorf(format, args...)
	om.stopRetryTimer()
	if om.testErrorsCh != nil {
		om.testErrorsCh <- err
	}
	om.brokerRequestsCh = nil
	om.nilOrBrokerRequestsCh = nil
	om.f.mapper.TriggerReassign(om)
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

func (om *offsetMgr) getCommitError(rs submitRs) error {
	if rs.connErr != nil {
		return rs.connErr
	}
	if rs.kafkaRs.Errors[om.id.topic] == nil {
		return sarama.ErrIncompleteResponse
	}
	err, ok := rs.kafkaRs.Errors[om.id.topic][om.id.partition]
	if !ok {
		return sarama.ErrIncompleteResponse
	}
	if err != sarama.ErrNoError {
		return err
	}
	return nil
}

type submitRq struct {
	id       instanceID
	offset   Offset
	resultCh chan<- submitRs
}

type submitRs struct {
	offset  Offset
	kafkaRs *sarama.OffsetCommitResponse
	connErr error
}

// brokerExecutor aggregates submitted offsets from partition offset managers
// and periodically commits them to Kafka.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	aggrActDesc       *actor.Descriptor
	execActDesc       *actor.Descriptor
	cfg               *config.Proxy
	conn              *sarama.Broker
	requestsCh        chan submitRq
	requestBatchesCh  chan map[string]map[instanceID]submitRq
	flushAggregatorCh chan none.T
	flushExecutorCh   chan none.T
	execStopCh        chan none.T
	wg                sync.WaitGroup
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
	defer close(be.execStopCh)

	requestBatch := make(map[string]map[instanceID]submitRq)
	var nilOrOffsetBatchesCh chan map[string]map[instanceID]submitRq
	for {
		select {
		case rq, ok := <-be.requestsCh:
			if !ok {
				return
			}
			groupRequests := requestBatch[rq.id.group]
			if groupRequests == nil {
				groupRequests = make(map[instanceID]submitRq)
				requestBatch[rq.id.group] = groupRequests
			}
			groupRequests[rq.id] = rq
			nilOrOffsetBatchesCh = be.requestBatchesCh

		case <-be.flushAggregatorCh:
			be.flushExecutorCh <- none.V

		case nilOrOffsetBatchesCh <- requestBatch:
			nilOrOffsetBatchesCh = nil
			requestBatch = make(map[string]map[instanceID]submitRq)
		}
	}
}

func (be *brokerExecutor) runExecutor() {
	nilOrRequestBatchesCh := be.requestBatchesCh
	var lastErrTime time.Time
	commitTicker := time.NewTicker(be.cfg.Consumer.OffsetsCommitInterval)
	defer commitTicker.Stop()
offsetCommitLoop:
	for {
		select {
		case requestBatch := <-nilOrRequestBatchesCh:
			nilOrRequestBatchesCh = nil
			for group, groupRequests := range requestBatch {
				kafkaRq := &sarama.OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
				}
				for _, rq := range groupRequests {
					kafkaRq.AddBlock(rq.id.topic, rq.id.partition, rq.offset.Val, sarama.ReceiveTime, rq.offset.Meta)
				}
				kafkaRs, err := be.conn.CommitOffset(kafkaRq)
				if err != nil {
					lastErrTime = time.Now()
					be.execActDesc.Log().WithError(err).Error("Connection reset")
					be.conn.Close()
				}
				// Fan the response out to the partition offset managers.
				for _, rq := range groupRequests {
					rq.resultCh <- submitRs{rq.offset, kafkaRs, err}
				}
			}
		case <-be.flushExecutorCh:
			nilOrRequestBatchesCh = be.requestBatchesCh

		case <-commitTicker.C:
			// Skip several circles after a connection failure to allow a Kafka
			// cluster some time to recuperate. Some requests will timeout
			// waiting in the channel, but that is ok, for they will be retried.
			if time.Since(lastErrTime) < be.cfg.Consumer.RetryBackoff {
				be.execActDesc.Log().Warn("Backing off after connection error")
				continue offsetCommitLoop
			}
			nilOrRequestBatchesCh = be.requestBatchesCh

		case <-be.execStopCh:
			return
		}
	}
}

func (be *brokerExecutor) String() string {
	if be == nil {
		return "<nil>"
	}
	return be.aggrActDesc.String()
}
