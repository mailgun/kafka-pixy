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
		close(pom.commitsCh)
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
	// to, when retrieved.
	InitialOffset() <-chan FetchedOffset

	// CommitOffset triggers saving of the specified commit in Kafka. Actual
	// commits are performed periodically in a background goroutine. The commit
	// interval is configured by `Config.Consumer.Offsets.CommitInterval`.
	CommitOffset(offset int64, metadata string)

	// Errors returns a read channel of errors that occur during offset
	// management, if enabled. By default errors are not returned. If you want
	// to implement any custom error handling logic then you need to set
	// `Consumer.Return.Errors` to true, and read from this channel.
	Errors() <-chan *OffsetCommitError

	// Close stops the PartitionOffsetManager from managing offsets. It is
	// required to call this function before a PartitionOffsetManager object
	// passes out of scope, as it will otherwise leak memory.
	Close()
}

type FetchedOffset struct {
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
	baseCID         *ContextID
	om              *offsetManager
	gtp             groupTopicPartition
	initialOffsetCh chan FetchedOffset
	commitsCh       chan offsetCommit
	assignmentCh    chan brokerExecutor
	errorsCh        chan *OffsetCommitError
	wg              sync.WaitGroup
}

func (om *offsetManager) spawnPartitionOffsetManager(gtp groupTopicPartition) *partitionOffsetMgr {
	pom := &partitionOffsetMgr{
		baseCID:         om.baseCID.NewChild(fmt.Sprintf("%s:%s:%d", gtp.group, gtp.topic, gtp.partition)),
		om:              om,
		gtp:             gtp,
		initialOffsetCh: make(chan FetchedOffset, 1),
		commitsCh:       make(chan offsetCommit),
		assignmentCh:    make(chan brokerExecutor, 1),
		errorsCh:        make(chan *OffsetCommitError, om.config.ChannelBufferSize),
	}
	spawn(&pom.wg, pom.processCommits)
	return pom
}

func (pom *partitionOffsetMgr) InitialOffset() <-chan FetchedOffset {
	return pom.initialOffsetCh
}

func (pom *partitionOffsetMgr) CommitOffset(offset int64, metadata string) {
	pom.commitsCh <- offsetCommit{
		gtp:      pom.gtp,
		offset:   offset,
		metadata: metadata,
	}
}

func (pom *partitionOffsetMgr) Errors() <-chan *OffsetCommitError {
	return pom.errorsCh
}

func (pom *partitionOffsetMgr) Close() {
	pom.om.childrenLock.Lock()
	close(pom.commitsCh)
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
	defer close(pom.errorsCh)
	var (
		commitResultCh            = make(chan commitResult, 1)
		initialOffsetFetched      = false
		isDirty                   = false
		isPending                 = false
		closed                    = false
		nilOrCommitsCh            = pom.commitsCh
		commitTicker              = time.NewTicker(pom.om.config.Consumer.Offsets.CommitInterval)
		recentCommit              offsetCommit
		assignedBrokerCommitsCh   chan<- offsetCommit
		nilOrBrokerCommitsCh      chan<- offsetCommit
		nilOrReassignRetryTimerCh <-chan time.Time
		commitTimestamp           time.Time
	)
	defer commitTicker.Stop()
	triggerReassign := func(err error, reason string) {
		Logger.Printf("<%s> %s: err=(%s)", cid, reason, err)
		pom.reportError(err)
		assignedBrokerCommitsCh = nil
		nilOrBrokerCommitsCh = nil
		pom.om.mapper.workerReassign() <- pom
		nilOrReassignRetryTimerCh = time.After(pom.om.config.Consumer.Retry.Backoff)
	}
	for {
		select {
		case bw := <-pom.assignmentCh:
			if bw == nil {
				assignedBrokerCommitsCh = nil
				nilOrReassignRetryTimerCh = time.After(pom.om.config.Consumer.Retry.Backoff)
				continue
			}
			bom := bw.(*brokerOffsetMgr)
			nilOrReassignRetryTimerCh = nil
			assignedBrokerCommitsCh = bom.commitsCh

			if !initialOffsetFetched {
				fo, err := pom.fetchInitialOffset(bom.conn)
				if err != nil {
					triggerReassign(err, "failed to fetch initial offset")
					continue
				}
				pom.initialOffsetCh <- fo
				Logger.Printf("<%s> initial offset fetched: offset=%d, metadata=%s", cid, fo.Offset, fo.Metadata)
				initialOffsetFetched = true
			}
			if isDirty {
				nilOrBrokerCommitsCh = assignedBrokerCommitsCh
			}
		case oc, ok := <-nilOrCommitsCh:
			if !ok {
				if isDirty || isPending {
					closed, nilOrCommitsCh = true, nil
					continue
				}
				return
			}
			recentCommit = oc
			recentCommit.resultCh = commitResultCh
			isDirty = true
			nilOrBrokerCommitsCh = assignedBrokerCommitsCh

		case nilOrBrokerCommitsCh <- recentCommit:
			nilOrBrokerCommitsCh = nil
			isDirty = false
			if !isPending {
				isPending, commitTimestamp = true, time.Now().UTC()
			}
		case cr := <-commitResultCh:
			isPending = false
			if err := pom.getCommitError(cr.response); err != nil {
				isDirty = true
				triggerReassign(err, "offset commit failed")
				continue
			}
			if closed && !isDirty {
				return
			}
		case <-commitTicker.C:
			if isPending && time.Now().UTC().Sub(commitTimestamp) > pom.om.config.Consumer.Offsets.Timeout {
				isDirty, isPending = true, false
				triggerReassign(ErrOffsetMgrRequestTimeout, "offset commit failed")
			}
		case <-nilOrReassignRetryTimerCh:
			triggerReassign(ErrOffsetMgrNoCoordinator, "retry reassignment")
		}
	}
}

func (pom *partitionOffsetMgr) fetchInitialOffset(conn *Broker) (FetchedOffset, error) {
	request := new(OffsetFetchRequest)
	request.Version = 1
	request.ConsumerGroup = pom.gtp.group
	request.AddPartition(pom.gtp.topic, pom.gtp.partition)

	response, err := conn.FetchOffset(request)
	if err != nil {
		return FetchedOffset{}, err
	}
	block := response.GetBlock(pom.gtp.topic, pom.gtp.partition)
	if block == nil {
		return FetchedOffset{}, ErrIncompleteResponse
	}
	if block.Err != ErrNoError {
		return FetchedOffset{}, block.Err
	}
	fetchedOffset := FetchedOffset{block.Offset, block.Metadata}
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

type offsetCommit struct {
	gtp      groupTopicPartition
	offset   int64
	metadata string
	resultCh chan<- commitResult
}

type commitResult struct {
	offsetCommit offsetCommit
	response     *OffsetCommitResponse
}

// brokerOffsetMgr
type brokerOffsetMgr struct {
	baseCID        *ContextID
	om             *offsetManager
	conn           *Broker
	commitsCh      chan offsetCommit
	batchCommitsCh chan map[string]map[groupTopicPartition]offsetCommit
	wg             sync.WaitGroup
}

func (om *offsetManager) spawnBrokerExecutor(brokerConn *Broker) brokerExecutor {
	bom := &brokerOffsetMgr{
		baseCID:        om.baseCID.NewChild(fmt.Sprintf("broker:%d", brokerConn.ID())),
		om:             om,
		conn:           brokerConn,
		commitsCh:      make(chan offsetCommit),
		batchCommitsCh: make(chan map[string]map[groupTopicPartition]offsetCommit),
	}
	spawn(&bom.wg, bom.batchCommits)
	spawn(&bom.wg, bom.executeBatches)
	return bom
}

func (bom *brokerOffsetMgr) brokerConn() *Broker {
	return bom.conn
}

func (bom *brokerOffsetMgr) close() {
	close(bom.commitsCh)
	bom.wg.Wait()
}

func (bom *brokerOffsetMgr) batchCommits() {
	defer bom.baseCID.NewChild("batchCommits").LogScope()()
	defer close(bom.batchCommitsCh)

	batchCommit := make(map[string]map[groupTopicPartition]offsetCommit)
	var nilOrBatchCommitsCh chan map[string]map[groupTopicPartition]offsetCommit
	for {
		select {
		case oc, ok := <-bom.commitsCh:
			if !ok {
				return
			}
			groupCommits := batchCommit[oc.gtp.group]
			if groupCommits == nil {
				groupCommits = make(map[groupTopicPartition]offsetCommit)
				batchCommit[oc.gtp.group] = groupCommits
			}
			groupCommits[oc.gtp] = oc
			nilOrBatchCommitsCh = bom.batchCommitsCh
		case nilOrBatchCommitsCh <- batchCommit:
			nilOrBatchCommitsCh = nil
			batchCommit = make(map[string]map[groupTopicPartition]offsetCommit)
		}
	}
}

func (bom *brokerOffsetMgr) executeBatches() {
	cid := bom.baseCID.NewChild("executeBatches")
	defer cid.LogScope()()

	var nilOrBatchCommitsCh chan map[string]map[groupTopicPartition]offsetCommit
	commitTicker := time.NewTicker(bom.om.config.Consumer.Offsets.CommitInterval)
	defer commitTicker.Stop()
	for {
		select {
		case <-commitTicker.C:
			nilOrBatchCommitsCh = bom.batchCommitsCh
		case batchCommit, ok := <-nilOrBatchCommitsCh:
			if !ok {
				return
			}
			nilOrBatchCommitsCh = nil
			for group, groupCommits := range batchCommit {
				req := &OffsetCommitRequest{
					Version:       1,
					ConsumerGroup: group,
				}
				for _, oc := range groupCommits {
					req.AddBlock(oc.gtp.topic, oc.gtp.partition, oc.offset, ReceiveTime, oc.metadata)
				}

				res, err := bom.conn.CommitOffset(req)
				if err != nil {
					// Make sure the broker connection is closed before notification.
					_ = bom.conn.Close()
					bom.om.mapper.brokerFailed() <- bom
					Logger.Printf("<%s> connection failed: err=(%v)", cid, err)
					return
				}

				for _, oc := range groupCommits {
					oc.resultCh <- commitResult{oc, res}
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
