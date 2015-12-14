package sarama

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var ErrOffsetMgrNoCoordinator = errors.New("failed to resolve coordinator")
var ErrOffsetMgrRequestTimeout = errors.New("request timeout")

// OffsetManager uses Kafka to store consumed partition offsets.
type OffsetManager interface {
	// ManagePartition creates a PartitionOffsetManager on the given
	// group/topic/partition. It returns an error if this OffsetManager is
	// already managing the given group/topic/partition.
	ManagePartition(group, topic string, partition int32) (PartitionOffsetManager, error)

	// Close terminates all spawned PartitionOffsetManager's and waits for them
	// to commit pending offsets. So it is not necessary to call Close methods
	// of spawned PartitionOffsetManagers explicitly.
	Close()
}

type offsetManager struct {
	baseCID      *ContextID
	client       Client
	config       *Config
	mapper       *partition2BrokerMapper
	children     map[groupTopicPartition]*partitionOffsetMgr
	childrenLock sync.Mutex
}

type groupTopicPartition struct {
	group     string
	topic     string
	partition int32
}

// NewOffsetManagerFromClient creates a new OffsetManager from the given client.
// It is still necessary to call Close() on the underlying client when finished
// with the partition manager.
func NewOffsetManagerFromClient(client Client) (OffsetManager, error) {
	// Check that we are not dealing with a closed Client before processing any
	// other arguments
	if client.Closed() {
		return nil, ErrClosedClient
	}
	om := &offsetManager{
		baseCID:  RootCID.NewChild("offsetManager"),
		client:   client,
		config:   client.Config(),
		children: make(map[groupTopicPartition]*partitionOffsetMgr),
	}
	om.mapper = spawnPartition2BrokerMapper(om.baseCID, om)
	return om, nil
}

func (om *offsetManager) Close() {
	om.childrenLock.Lock()
	for _, pom := range om.children {
		close(pom.submittedOffsetsCh)
		pom.wg.Wait()
		pom.om.mapper.workerClosed() <- pom
	}
	om.childrenLock.Unlock()
	om.mapper.close()
}

func (om *offsetManager) ManagePartition(group, topic string, partition int32) (PartitionOffsetManager, error) {
	gtp := groupTopicPartition{group, topic, partition}

	om.childrenLock.Lock()
	defer om.childrenLock.Unlock()
	if _, ok := om.children[gtp]; ok {
		return nil, ConfigurationError("That topic/partition is already being managed")
	}
	pom := om.spawnPartitionOffsetManager(gtp)
	om.mapper.workerCreated() <- pom
	om.children[gtp] = pom
	return pom, nil
}

func (om *offsetManager) resolveBroker(pw partitionWorker) (*Broker, error) {
	pom := pw.(*partitionOffsetMgr)
	if err := om.client.RefreshCoordinator(pom.gtp.group); err != nil {
		return nil, err
	}

	brokerConn, err := om.client.Coordinator(pom.gtp.group)
	if err != nil {
		return nil, err
	}
	return brokerConn, nil
}

// PartitionOffsetManager uses Kafka to fetch consumed partition offsets. You
// MUST call Close() on a partition offset manager to avoid leaks, it will not
// be garbage-collected automatically when it passes out of scope.
type PartitionOffsetManager interface {
	// InitialOffset returns a channel that an initial offset will be sent down
	// to, when retrieved. At most one value will be sent down to this channel,
	// and it will be closed immediately after that. If error reporting is
	// enabled with `Config.Consumer.Return.Errors` then errors may be coming
	// and has to be read from the `Errors()` channel, otherwise the partition
	// offset manager will get into a dead lock.
	InitialOffset() <-chan DecoratedOffset

	// SubmitOffset triggers saving of the specified offset in Kafka. Commits
	// are performed periodically in a background goroutine. The commit
	// interval is configured by `Config.Consumer.Offsets.CommitInterval`. Note
	// that not every submitted offset gets committed. Committed offsets are
	// sent down to the `CommittedOffsets()` channel. The `CommittedOffsets()`
	// channel has to be read alongside with submitting offsets, otherwise the
	// partition offset manager will block.
	SubmitOffset(offset int64, metadata string)

	// CommittedOffsets returns a channel that offsets committed to Kafka are
	// sent down to. The user must read from this channel otherwise a
	// `SubmitOffset` will eventually block.
	CommittedOffsets() <-chan DecoratedOffset

	// Errors returns a read channel of errors that occur during offset
	// management, if enabled. By default errors are not returned. If you want
	// to implement any custom error handling logic then you need to set
	// `Consumer.Return.Errors` to true, and read from this channel.
	Errors() <-chan *OffsetCommitError

	// Close stops the PartitionOffsetManager from managing offsets. It is
	// required to call this function before a PartitionOffsetManager object
	// passes out of scope, as it will otherwise leak memory.
	//
	// It is guaranteed that the most recent offset is committed before `Close`
	// returns.
	Close()
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

type partitionOffsetMgr struct {
	baseCID            *ContextID
	om                 *offsetManager
	gtp                groupTopicPartition
	initialOffsetCh    chan DecoratedOffset
	submittedOffsetsCh chan submittedOffset
	assignmentCh       chan brokerExecutor
	committedOffsetsCh chan DecoratedOffset
	errorsCh           chan *OffsetCommitError
	wg                 sync.WaitGroup
}

func (om *offsetManager) spawnPartitionOffsetManager(gtp groupTopicPartition) *partitionOffsetMgr {
	pom := &partitionOffsetMgr{
		baseCID:            om.baseCID.NewChild(fmt.Sprintf("%s:%s:%d", gtp.group, gtp.topic, gtp.partition)),
		om:                 om,
		gtp:                gtp,
		initialOffsetCh:    make(chan DecoratedOffset, 1),
		submittedOffsetsCh: make(chan submittedOffset),
		assignmentCh:       make(chan brokerExecutor, 1),
		committedOffsetsCh: make(chan DecoratedOffset, om.config.ChannelBufferSize),
		errorsCh:           make(chan *OffsetCommitError, om.config.ChannelBufferSize),
	}
	spawn(&pom.wg, pom.processCommits)
	return pom
}

func (pom *partitionOffsetMgr) InitialOffset() <-chan DecoratedOffset {
	return pom.initialOffsetCh
}

func (pom *partitionOffsetMgr) SubmitOffset(offset int64, metadata string) {
	pom.submittedOffsetsCh <- submittedOffset{
		gtp:      pom.gtp,
		offset:   offset,
		metadata: metadata,
	}
}

func (pom *partitionOffsetMgr) CommittedOffsets() <-chan DecoratedOffset {
	return pom.committedOffsetsCh
}

func (pom *partitionOffsetMgr) Errors() <-chan *OffsetCommitError {
	return pom.errorsCh
}

func (pom *partitionOffsetMgr) Close() {
	pom.om.childrenLock.Lock()
	close(pom.submittedOffsetsCh)
	delete(pom.om.children, pom.gtp)
	pom.om.childrenLock.Unlock()
	pom.wg.Wait()
	pom.om.mapper.workerClosed() <- pom
}

func (pom *partitionOffsetMgr) String() string {
	return pom.baseCID.String()
}

func (pom *partitionOffsetMgr) assignment() chan<- brokerExecutor {
	return pom.assignmentCh
}

func (pom *partitionOffsetMgr) processCommits() {
	cid := pom.baseCID.NewChild("processCommits")
	defer cid.LogScope()()
	defer close(pom.committedOffsetsCh)
	defer close(pom.errorsCh)
	var (
		commitResultCh            = make(chan commitResult, 1)
		initialOffsetFetched      = false
		isDirty                   = false
		isPending                 = false
		closed                    = false
		nilOrSubmittedOffsetsCh   = pom.submittedOffsetsCh
		commitTicker              = time.NewTicker(pom.om.config.Consumer.Offsets.CommitInterval)
		recentSubmittedOffset     submittedOffset
		assignedBrokerCommitsCh   chan<- submittedOffset
		nilOrBrokerCommitsCh      chan<- submittedOffset
		nilOrReassignRetryTimerCh <-chan time.Time
		lastCommitTime            time.Time
		lastReassignTime          time.Time
	)
	defer commitTicker.Stop()
	triggerOrScheduleReassign := func(err error, reason string) {
		pom.reportError(err)
		assignedBrokerCommitsCh = nil
		nilOrBrokerCommitsCh = nil
		now := time.Now().UTC()
		if now.Sub(lastReassignTime) > pom.om.config.Consumer.Retry.Backoff {
			Logger.Printf("<%s> trigger reassign: reason=%s, err=(%s)", cid, reason, err)
			lastReassignTime = now
			pom.om.mapper.workerReassign() <- pom
		} else {
			Logger.Printf("<%s> schedule reassign: reason=%s, err=(%s)", cid, reason, err)
		}
		nilOrReassignRetryTimerCh = time.After(pom.om.config.Consumer.Retry.Backoff)
	}
	for {
		select {
		case bw := <-pom.assignmentCh:
			if bw == nil {
				assignedBrokerCommitsCh = nil
				triggerOrScheduleReassign(ErrOffsetMgrNoCoordinator, "retry reassignment")
				continue
			}
			bom := bw.(*brokerOffsetMgr)
			nilOrReassignRetryTimerCh = nil
			assignedBrokerCommitsCh = bom.submittedOffsetsCh

			if !initialOffsetFetched {
				initialOffset, err := pom.fetchInitialOffset(bom.conn)
				if err != nil {
					triggerOrScheduleReassign(err, "failed to fetch initial offset")
					continue
				}
				pom.initialOffsetCh <- initialOffset
				close(pom.initialOffsetCh)
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
			if err := pom.getCommitError(cr.response); err != nil {
				isDirty = true
				triggerOrScheduleReassign(err, "offset commit failed")
				continue
			}
			pom.committedOffsetsCh <- DecoratedOffset{cr.offsetCommit.offset, cr.offsetCommit.metadata}
			if closed && !isDirty {
				return
			}
		case <-commitTicker.C:
			if isPending && time.Now().UTC().Sub(lastCommitTime) > pom.om.config.Consumer.Offsets.Timeout {
				isDirty, isPending = true, false
				triggerOrScheduleReassign(ErrOffsetMgrRequestTimeout, "offset commit failed")
			}
		case <-nilOrReassignRetryTimerCh:
			pom.om.mapper.workerReassign() <- pom
			Logger.Printf("<%s> reassign triggered by timeout", cid)
			nilOrReassignRetryTimerCh = time.After(pom.om.config.Consumer.Retry.Backoff)
		}
	}
}

func (pom *partitionOffsetMgr) fetchInitialOffset(conn *Broker) (DecoratedOffset, error) {
	request := new(OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = pom.gtp.group
	request.AddPartition(pom.gtp.topic, pom.gtp.partition)

	response, err := conn.FetchOffset(request)
	if err != nil {
		// In case of network error the connection has to be explicitly closed,
		// otherwise it won't be re-establish and following requests to this
		// broker will fail as well.
		_ = conn.Close()
		return DecoratedOffset{}, err
	}
	block := response.GetBlock(pom.gtp.topic, pom.gtp.partition)
	if block == nil {
		return DecoratedOffset{}, ErrIncompleteResponse
	}
	if block.Err != ErrNoError {
		return DecoratedOffset{}, block.Err
	}
	fetchedOffset := DecoratedOffset{block.Offset, block.Metadata}
	return fetchedOffset, nil
}

func (pom *partitionOffsetMgr) reportError(err error) {
	if !pom.om.config.Consumer.Return.Errors {
		return
	}
	oce := &OffsetCommitError{
		Group:     pom.gtp.group,
		Topic:     pom.gtp.topic,
		Partition: pom.gtp.partition,
		Err:       err,
	}
	select {
	case pom.errorsCh <- oce:
	default:
	}
}

func (pom *partitionOffsetMgr) getCommitError(res *OffsetCommitResponse) error {
	if res.Errors[pom.gtp.topic] == nil {
		return ErrIncompleteResponse
	}
	err, ok := res.Errors[pom.gtp.topic][pom.gtp.partition]
	if !ok {
		return ErrIncompleteResponse
	}
	if err != ErrNoError {
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
	response     *OffsetCommitResponse
}

// brokerOffsetMgr aggregates submitted offsets from partition offset managers
// and periodically commits them to Kafka.
type brokerOffsetMgr struct {
	baseCID            *ContextID
	config             *Config
	conn               *Broker
	submittedOffsetsCh chan submittedOffset
	offsetBatches      chan map[string]map[groupTopicPartition]submittedOffset
	wg                 sync.WaitGroup
}

func (om *offsetManager) spawnBrokerExecutor(brokerConn *Broker) brokerExecutor {
	bom := &brokerOffsetMgr{
		baseCID:            om.baseCID.NewChild(fmt.Sprintf("broker:%d", brokerConn.ID())),
		config:             om.config,
		conn:               brokerConn,
		submittedOffsetsCh: make(chan submittedOffset),
		offsetBatches:      make(chan map[string]map[groupTopicPartition]submittedOffset),
	}
	spawn(&bom.wg, bom.batchSubmitted)
	spawn(&bom.wg, bom.executeBatches)
	return bom
}

func (bom *brokerOffsetMgr) brokerConn() *Broker {
	return bom.conn
}

func (bom *brokerOffsetMgr) close() {
	close(bom.submittedOffsetsCh)
	bom.wg.Wait()
}

func (bom *brokerOffsetMgr) batchSubmitted() {
	defer bom.baseCID.NewChild("batchSubmitted").LogScope()()
	defer close(bom.offsetBatches)

	batchCommit := make(map[string]map[groupTopicPartition]submittedOffset)
	var nilOrOffsetBatchesCh chan map[string]map[groupTopicPartition]submittedOffset
	for {
		select {
		case so, ok := <-bom.submittedOffsetsCh:
			if !ok {
				return
			}
			groupOffsets := batchCommit[so.gtp.group]
			if groupOffsets == nil {
				groupOffsets = make(map[groupTopicPartition]submittedOffset)
				batchCommit[so.gtp.group] = groupOffsets
			}
			groupOffsets[so.gtp] = so
			nilOrOffsetBatchesCh = bom.offsetBatches
		case nilOrOffsetBatchesCh <- batchCommit:
			nilOrOffsetBatchesCh = nil
			batchCommit = make(map[string]map[groupTopicPartition]submittedOffset)
		}
	}
}

func (bom *brokerOffsetMgr) executeBatches() {
	cid := bom.baseCID.NewChild("executeBatches")
	defer cid.LogScope()()

	var nilOrOffsetBatchesCh chan map[string]map[groupTopicPartition]submittedOffset
	var lastErr error
	var lastErrTime time.Time
	commitTicker := time.NewTicker(bom.config.Consumer.Offsets.CommitInterval)
	defer commitTicker.Stop()
offsetCommitLoop:
	for {
		select {
		case <-commitTicker.C:
			nilOrOffsetBatchesCh = bom.offsetBatches
		case batchedOffsets, ok := <-nilOrOffsetBatchesCh:
			if !ok {
				return
			}
			// Ignore submit requests for awhile after a connection failure to
			// allow the Kafka cluster some time to recuperate. Ignored requests
			// will be retried by originating partition offset managers.
			if time.Now().UTC().Sub(lastErrTime) < bom.config.Consumer.Retry.Backoff {
				continue offsetCommitLoop
			}
			nilOrOffsetBatchesCh = nil
			for group, groupOffsets := range batchedOffsets {
				req := &OffsetCommitRequest{
					Version:                 1,
					ConsumerGroup:           group,
					ConsumerGroupGeneration: GroupGenerationUndefined,
				}
				for _, so := range groupOffsets {
					req.AddBlock(so.gtp.topic, so.gtp.partition, so.offset, ReceiveTime, so.metadata)
				}
				var res *OffsetCommitResponse
				res, lastErr = bom.conn.CommitOffset(req)
				if lastErr != nil {
					lastErrTime = time.Now().UTC()
					bom.conn.Close()
					Logger.Printf("<%s> connection reset: err=(%v)", cid, lastErr)
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

func (bom *brokerOffsetMgr) String() string {
	if bom == nil {
		return "<nil>"
	}
	return bom.baseCID.String()
}
