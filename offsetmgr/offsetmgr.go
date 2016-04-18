package offsetmgr

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/mapper"
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
	NewOffsetManager(group, topic string, partition int32) (T, error)

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

// NewFactory creates a new offset manager factory from the given client.
func NewFactory(client sarama.Client) Factory {
	f := &factory{
		baseCID:  actor.RootID.NewChild("offsetManagerFactory"),
		client:   client,
		config:   client.Config(),
		children: make(map[groupTopicPartition]*offsetManager),
	}
	f.mapper = mapper.Spawn(f.baseCID, f)
	return f
}

// implements `Factory`
// implements `mapper.Resolver`
type factory struct {
	baseCID      *actor.ID
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
func (f *factory) NewOffsetManager(group, topic string, partition int32) (T, error) {
	gtp := groupTopicPartition{group, topic, partition}

	f.childrenLock.Lock()
	defer f.childrenLock.Unlock()
	if _, ok := f.children[gtp]; ok {
		return nil, sarama.ConfigurationError("That topic/partition is already being managed")
	}
	om := f.spawnOffsetManager(gtp)
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
		baseCID:            f.baseCID.NewChild(fmt.Sprintf("broker:%d", brokerConn.ID())),
		config:             f.config,
		conn:               brokerConn,
		submittedOffsetsCh: make(chan submittedOffset),
		offsetBatches:      make(chan map[string]map[groupTopicPartition]submittedOffset),
	}
	spawn(&be.wg, be.batchSubmitted)
	spawn(&be.wg, be.executeBatches)
	return be
}

func (f *factory) spawnOffsetManager(gtp groupTopicPartition) *offsetManager {
	om := &offsetManager{
		baseCID:            f.baseCID.NewChild(fmt.Sprintf("%s:%s:%d", gtp.group, gtp.topic, gtp.partition)),
		f:                  f,
		gtp:                gtp,
		initialOffsetCh:    make(chan DecoratedOffset, 1),
		submittedOffsetsCh: make(chan submittedOffset),
		assignmentCh:       make(chan mapper.Executor, 1),
		committedOffsetsCh: make(chan DecoratedOffset, f.config.ChannelBufferSize),
		errorsCh:           make(chan *OffsetCommitError, f.config.ChannelBufferSize),
	}
	spawn(&om.wg, om.processCommits)
	return om
}

// implements `Factory.Stop()`
func (f *factory) Stop() {
	f.mapper.Stop()
}

// implements `T`
// implements `mapper.Worker`
type offsetManager struct {
	baseCID            *actor.ID
	f                  *factory
	gtp                groupTopicPartition
	initialOffsetCh    chan DecoratedOffset
	submittedOffsetsCh chan submittedOffset
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
	om.submittedOffsetsCh <- submittedOffset{
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
	om.f.childrenLock.Lock()
	close(om.submittedOffsetsCh)
	delete(om.f.children, om.gtp)
	om.f.childrenLock.Unlock()
	om.wg.Wait()
	om.f.mapper.WorkerStopped() <- om
}

// implements `mapper.Worker`.
func (om *offsetManager) Assignment() chan<- mapper.Executor {
	return om.assignmentCh
}

func (om *offsetManager) String() string {
	return om.baseCID.String()
}

func (om *offsetManager) processCommits() {
	cid := om.baseCID.NewChild("processCommits")
	defer cid.LogScope()()
	defer close(om.committedOffsetsCh)
	defer close(om.errorsCh)
	var (
		commitResultCh            = make(chan commitResult, 1)
		initialOffsetFetched      = false
		isDirty                   = false
		isPending                 = false
		closed                    = false
		nilOrSubmittedOffsetsCh   = om.submittedOffsetsCh
		commitTicker              = time.NewTicker(om.f.config.Consumer.Offsets.CommitInterval)
		recentSubmittedOffset     submittedOffset
		assignedBrokerCommitsCh   chan<- submittedOffset
		nilOrBrokerCommitsCh      chan<- submittedOffset
		nilOrReassignRetryTimerCh <-chan time.Time
		lastCommitTime            time.Time
		lastReassignTime          time.Time
		commitTimeout             = 3 * om.f.config.Consumer.Offsets.CommitInterval
	)
	defer commitTicker.Stop()
	triggerOrScheduleReassign := func(err error, reason string) {
		om.reportError(err)
		assignedBrokerCommitsCh = nil
		nilOrBrokerCommitsCh = nil
		now := time.Now().UTC()
		if now.Sub(lastReassignTime) > om.f.config.Consumer.Retry.Backoff {
			log.Infof("<%s> trigger reassign: reason=%s, err=(%s)", cid, reason, err)
			lastReassignTime = now
			om.f.mapper.WorkerReassign() <- om
		} else {
			log.Infof("<%s> schedule reassign: reason=%s, err=(%s)", cid, reason, err)
		}
		nilOrReassignRetryTimerCh = time.After(om.f.config.Consumer.Retry.Backoff)
	}
	for {
		select {
		case bw := <-om.assignmentCh:
			if bw == nil {
				assignedBrokerCommitsCh = nil
				triggerOrScheduleReassign(ErrNoCoordinator, "retry reassignment")
				continue
			}
			be := bw.(*brokerExecutor)
			nilOrReassignRetryTimerCh = nil
			assignedBrokerCommitsCh = be.submittedOffsetsCh

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
				nilOrBrokerCommitsCh = assignedBrokerCommitsCh
			}
		case so, ok := <-nilOrSubmittedOffsetsCh:
			if !ok {
				if isDirty || isPending {
					closed, nilOrSubmittedOffsetsCh = true, nil
					continue
				}
				return
			}
			recentSubmittedOffset = so
			recentSubmittedOffset.resultCh = commitResultCh
			isDirty = true
			nilOrBrokerCommitsCh = assignedBrokerCommitsCh

		case nilOrBrokerCommitsCh <- recentSubmittedOffset:
			nilOrBrokerCommitsCh = nil
			isDirty = false
			if !isPending {
				isPending, lastCommitTime = true, time.Now().UTC()
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
			if isPending && time.Now().UTC().Sub(lastCommitTime) > commitTimeout {
				isDirty, isPending = true, false
				triggerOrScheduleReassign(ErrRequestTimeout, "offset commit failed")
			}
		case <-nilOrReassignRetryTimerCh:
			om.f.mapper.WorkerReassign() <- om
			log.Infof("<%s> reassign triggered by timeout", cid)
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

type submittedOffset struct {
	gtp      groupTopicPartition
	offset   int64
	metadata string
	resultCh chan<- commitResult
}

type commitResult struct {
	offsetCommit submittedOffset
	response     *sarama.OffsetCommitResponse
}

// brokerExecutor aggregates submitted offsets from partition offset managers
// and periodically commits them to Kafka.
//
// implements `mapper.Executor`.
type brokerExecutor struct {
	baseCID            *actor.ID
	config             *sarama.Config
	conn               *sarama.Broker
	submittedOffsetsCh chan submittedOffset
	offsetBatches      chan map[string]map[groupTopicPartition]submittedOffset
	wg                 sync.WaitGroup
}

// implements `mapper.Executor`.
func (be *brokerExecutor) BrokerConn() *sarama.Broker {
	return be.conn
}

// implements `mapper.Executor`.
func (be *brokerExecutor) Stop() {
	close(be.submittedOffsetsCh)
	be.wg.Wait()
}

func (be *brokerExecutor) batchSubmitted() {
	defer be.baseCID.NewChild("aggregator").LogScope()()
	defer close(be.offsetBatches)

	batchCommit := make(map[string]map[groupTopicPartition]submittedOffset)
	var nilOrOffsetBatchesCh chan map[string]map[groupTopicPartition]submittedOffset
	for {
		select {
		case so, ok := <-be.submittedOffsetsCh:
			if !ok {
				return
			}
			groupOffsets := batchCommit[so.gtp.group]
			if groupOffsets == nil {
				groupOffsets = make(map[groupTopicPartition]submittedOffset)
				batchCommit[so.gtp.group] = groupOffsets
			}
			groupOffsets[so.gtp] = so
			nilOrOffsetBatchesCh = be.offsetBatches
		case nilOrOffsetBatchesCh <- batchCommit:
			nilOrOffsetBatchesCh = nil
			batchCommit = make(map[string]map[groupTopicPartition]submittedOffset)
		}
	}
}

func (be *brokerExecutor) executeBatches() {
	cid := be.baseCID.NewChild("executor")
	defer cid.LogScope()()

	var nilOrOffsetBatchesCh chan map[string]map[groupTopicPartition]submittedOffset
	var lastErr error
	var lastErrTime time.Time
	commitTicker := time.NewTicker(be.config.Consumer.Offsets.CommitInterval)
	defer commitTicker.Stop()
offsetCommitLoop:
	for {
		select {
		case <-commitTicker.C:
			nilOrOffsetBatchesCh = be.offsetBatches
		case batchedOffsets, ok := <-nilOrOffsetBatchesCh:
			if !ok {
				return
			}
			// Ignore submit requests for awhile after a connection failure to
			// allow the Kafka cluster some time to recuperate. Ignored requests
			// will be retried by originating partition offset managers.
			if time.Now().UTC().Sub(lastErrTime) < be.config.Consumer.Retry.Backoff {
				continue offsetCommitLoop
			}
			nilOrOffsetBatchesCh = nil
			for group, groupOffsets := range batchedOffsets {
				req := &sarama.OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
				}
				for _, so := range groupOffsets {
					req.AddBlock(so.gtp.topic, so.gtp.partition, so.offset, sarama.ReceiveTime, so.metadata)
				}
				var res *sarama.OffsetCommitResponse
				res, lastErr = be.conn.CommitOffset(req)
				if lastErr != nil {
					lastErrTime = time.Now().UTC()
					be.conn.Close()
					log.Infof("<%s> connection reset: err=(%v)", cid, lastErr)
					continue offsetCommitLoop
				}
				// Fan the response out to the partition offset managers.
				for _, so := range groupOffsets {
					so.resultCh <- commitResult{so, res}
				}
			}
		}
	}
}

func (be *brokerExecutor) String() string {
	if be == nil {
		return "<nil>"
	}
	return be.baseCID.String()
}
