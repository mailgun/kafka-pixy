package subscriber

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/mailgun/kazoo-go"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
)

// It is ok for an attempt to claim a partition to fail, for it might take
// some time for the current partition owner to release it. So we won't report
// first several failures to claim a partition as an error.
const safeClaimRetriesCount = 10

// T is a subscriber implementation based on ZooKeeper. It maintains consumer
// group membership and topic subscriptions, watches for other members to join,
// leave and update their subscriptions, and generates notifications of such
// changes. It also provides an API to for a partition consumer to claim and
// release a group-topic-partition.
//
// FIXME: It is assumed that all members of the group are registered with the
// FIXME: `static` pattern. If a member that pattern is either `white_list` or
// FIXME: `black_list` joins the group the result will be unpredictable.
type T struct {
	actDesc          *actor.Descriptor
	cfg              *config.Proxy
	group            string
	groupZNode       *kazoo.Consumergroup
	groupMemberZNode *kazoo.ConsumergroupInstance
	registered       bool
	topicsCh         chan []string
	subscriptionsCh  chan map[string][]string
	stopCh           chan none.T
	claimErrorsCh    chan none.T
	wg               sync.WaitGroup
}

// Spawn creates a subscriber instance and starts its goroutine.
func Spawn(parentActDesc *actor.Descriptor, group string, cfg *config.Proxy, kazooClt *kazoo.Kazoo) *T {
	groupZNode := kazooClt.Consumergroup(group)
	groupMemberZNode := groupZNode.Instance(cfg.ClientID)
	actDesc := parentActDesc.NewChild("member")
	actDesc.AddLogField("kafka.group", group)
	ss := &T{
		actDesc:          actDesc,
		cfg:              cfg,
		group:            group,
		groupZNode:       groupZNode,
		groupMemberZNode: groupMemberZNode,
		topicsCh:         make(chan []string),
		subscriptionsCh:  make(chan map[string][]string),
		stopCh:           make(chan none.T),
		claimErrorsCh:    make(chan none.T, 1),
	}
	actor.Spawn(ss.actDesc, &ss.wg, ss.run)
	return ss
}

// Topics returns a channel to receive a list of topics the member should
// subscribe to. To make the member unsubscribe from all topics either nil or
// an empty topic list can be sent.
func (ss *T) Topics() chan<- []string {
	return ss.topicsCh
}

// Subscriptions returns a channel that subscriptions will be sent whenever a
// member joins or leaves the group or when an existing member updates its
// subscription.
func (ss *T) Subscriptions() <-chan map[string][]string {
	return ss.subscriptionsCh
}

// ClaimPartition claims a topic/partition to be consumed by this member of the
// consumer group. It blocks until either succeeds or canceled by the caller. It
// returns a function that should be called to release the claim.
func (ss *T) ClaimPartition(claimerActDesc *actor.Descriptor, topic string, partition int32, cancelCh <-chan none.T) func() {
	beginAt := time.Now()
	retries := 0
	err := ss.groupMemberZNode.ClaimPartition(topic, partition)
	for err != nil {
		logEntry := claimerActDesc.Log().WithError(err)
		logFailureFn := logEntry.Infof
		if retries++; retries > safeClaimRetriesCount {
			logFailureFn = logEntry.Errorf
		}
		logFailureFn("Failed to claim partition: via=%s, retries=%d, took=%s",
			ss.actDesc, retries, millisSince(beginAt))

		// Let the subscriber actor know that a claim attempt failed.
		select {
		case ss.claimErrorsCh <- none.V:
		default:
		}
		// Wait until either the retry timeout expires or the claim is canceled.
		select {
		case <-time.After(ss.cfg.Consumer.RetryBackoff):
		case <-cancelCh:
			return func() {}
		}
		err = ss.groupMemberZNode.ClaimPartition(topic, partition)
	}
	claimerActDesc.Log().Infof("Partition claimed: via=%s, retries=%d, took=%s",
		ss.actDesc, retries, millisSince(beginAt))
	return func() {
		beginAt := time.Now()
		retries := 0
		err := ss.groupMemberZNode.ReleasePartition(topic, partition)
		for err != nil && err != kazoo.ErrPartitionNotClaimed {
			logEntry := claimerActDesc.Log().WithError(err)
			logFailureFn := logEntry.Infof
			if retries++; retries > safeClaimRetriesCount {
				logFailureFn = logEntry.Errorf
			}
			logFailureFn("Failed to release partition: via=%s, retries=%d, took=%s",
				ss.actDesc, retries, millisSince(beginAt))
			<-time.After(ss.cfg.Consumer.RetryBackoff)
			err = ss.groupMemberZNode.ReleasePartition(topic, partition)
		}
		claimerActDesc.Log().Infof("Partition released: via=%s, retries=%d, took=%s",
			ss.actDesc, retries, millisSince(beginAt))
	}
}

// Stop signals the consumer group member to stop and blocks until its
// goroutines are over.
func (ss *T) Stop() {
	close(ss.stopCh)
	ss.wg.Wait()
}

func (ss *T) run() {
	defer close(ss.subscriptionsCh)

	// Ensure a group ZNode exists.
	err := ss.groupZNode.Create()
	for err != nil {
		ss.actDesc.Log().WithError(err).Error("Failed to create a group znode")
		select {
		case <-time.After(ss.cfg.Consumer.RetryBackoff):
		case <-ss.stopCh:
			return
		}
		err = ss.groupZNode.Create()
	}

	// Ensure that the member leaves the group in ZooKeeper on stop. We retry
	// indefinitely here until ZooKeeper confirms that there is no registration.
	defer func() {
		err := ss.groupMemberZNode.Deregister()
		for err != nil && err != kazoo.ErrInstanceNotRegistered {
			ss.actDesc.Log().WithError(err).Error("Failed to deregister")
			<-time.After(ss.cfg.Consumer.RetryBackoff)
			err = ss.groupMemberZNode.Deregister()
		}
	}()

	var (
		nilOrSubscriptionsCh     chan<- map[string][]string
		nilOrWatchCh             <-chan none.T
		nilOrTimeoutCh           <-chan time.Time
		cancelWatch              context.CancelFunc
		shouldSubmitTopics       = false
		shouldFetchSubscriptions = false
		topics                   []string
		subscriptions            map[string][]string
		submittedAt              = time.Now()
	)
	for {
		select {
		case topics = <-ss.topicsCh:
			sort.Strings(topics)
			shouldSubmitTopics = true

		case nilOrSubscriptionsCh <- subscriptions:
			nilOrSubscriptionsCh = nil

		case <-nilOrWatchCh:
			nilOrWatchCh = nil
			cancelWatch()
			shouldFetchSubscriptions = true

		case <-ss.claimErrorsCh:
			sinceLastSubmit := time.Now().Sub(submittedAt)
			if sinceLastSubmit > ss.cfg.Consumer.RetryBackoff {
				ss.actDesc.Log().Infof("Resubmit triggered by claim failure: since=%v", sinceLastSubmit)
				shouldSubmitTopics = true
			}
		case <-nilOrTimeoutCh:
			nilOrTimeoutCh = nil

		case <-ss.stopCh:
			if cancelWatch != nil {
				cancelWatch()
			}
			return
		}

		if shouldSubmitTopics {
			if err = ss.submitTopics(topics); err != nil {
				ss.actDesc.Log().WithError(err).Error("Failed to submit topics")
				nilOrTimeoutCh = time.After(ss.cfg.Consumer.RetryBackoff)
				continue
			}
			submittedAt = time.Now()
			ss.actDesc.Log().Infof("Submitted: topics=%v", topics)
			shouldSubmitTopics = false
			if cancelWatch != nil {
				cancelWatch()
			}
			shouldFetchSubscriptions = true
		}

		if shouldFetchSubscriptions {
			subscriptions, nilOrWatchCh, cancelWatch, err = ss.fetchSubscriptions()
			if err != nil {
				ss.actDesc.Log().WithError(err).Error("Failed to fetch subscriptions")
				nilOrTimeoutCh = time.After(ss.cfg.Consumer.RetryBackoff)
				continue
			}
			shouldFetchSubscriptions = false
			ss.actDesc.Log().Infof("Fetched subscriptions: %s", prettyfmt.Val(subscriptions))
			nilOrSubscriptionsCh = ss.subscriptionsCh
		}
	}
}

// fetchSubscriptions retrieves subscription topics for all group members and
// returns a channel that will be closed
func (ss *T) fetchSubscriptions() (map[string][]string, <-chan none.T, context.CancelFunc, error) {
	members, groupUpdateWatchCh, err := ss.groupZNode.WatchInstances()
	if err != nil {
		return nil, nil, nil, errors.Wrapf(err, "failed to watch members")
	}

	memberUpdateWatchChs := make(map[string]<-chan zk.Event, len(members))
	subscriptions := make(map[string][]string, len(members))
	for _, member := range members {
		var registration *kazoo.Registration
		registration, memberUpdateWatchCh, err := member.WatchRegistration()
		for err != nil {
			return nil, nil, nil, errors.Wrapf(err, "failed to watch registration, member=%s", member.ID)
		}
		memberUpdateWatchChs[member.ID] = memberUpdateWatchCh

		topics := make([]string, 0, len(registration.Subscription))
		for topic := range registration.Subscription {
			topics = append(topics, topic)
		}
		// Sort topics to ensure deterministic output.
		sort.Strings(topics)
		subscriptions[member.ID] = topics
	}
	aggregateWatchCh := make(chan none.T)
	ctx, cancel := context.WithCancel(context.Background())

	go ss.forwardWatch(ctx, "members", groupUpdateWatchCh, aggregateWatchCh)
	for memberID, memberUpdateWatchCh := range memberUpdateWatchChs {
		go ss.forwardWatch(ctx, memberID, memberUpdateWatchCh, aggregateWatchCh)
	}
	return subscriptions, aggregateWatchCh, cancel, nil
}

func (ss *T) submitTopics(topics []string) error {
	if len(topics) == 0 {
		err := ss.groupMemberZNode.Deregister()
		if err != nil && err != kazoo.ErrInstanceNotRegistered {
			return errors.Wrap(err, "failed to deregister")
		}
		ss.registered = false
		return nil
	}

	if ss.registered {
		err := ss.groupMemberZNode.UpdateRegistration(topics)
		if err != kazoo.ErrInstanceNotRegistered {
			return errors.Wrap(err, "failed to update registration")
		}
		ss.registered = false
		ss.actDesc.Log().Errorf("Registration disappeared")
	}

	if err := ss.groupMemberZNode.Register(topics); err != nil {
		return errors.Wrap(err, "failed to register")
	}
	ss.registered = true
	return nil
}

func (ss *T) forwardWatch(ctx context.Context, alias string, fromCh <-chan zk.Event, toCh chan<- none.T) {
	select {
	case <-fromCh:
		ss.actDesc.Log().Infof("Watch triggered: alias=%s", alias)
		select {
		case toCh <- none.V:
		case <-ctx.Done():
		}
	case <-ctx.Done():
	}
}

func millisSince(t time.Time) time.Duration {
	return time.Now().Sub(t) / time.Millisecond * time.Millisecond
}
