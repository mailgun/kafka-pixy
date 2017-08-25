package subscriber

import (
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
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
type T struct {
	actDesc          *actor.Descriptor
	cfg              *config.Proxy
	group            string
	groupZNode       *kazoo.Consumergroup
	groupMemberZNode *kazoo.ConsumergroupInstance
	topics           []string
	subscriptions    map[string][]string
	topicsCh         chan []string
	subscriptionsCh  chan map[string][]string
	stopCh           chan none.T
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
		logFailureFn("failed to claim partition: via=%s, retries=%d, took=%s",
			ss.actDesc, retries, millisSince(beginAt))
		select {
		case <-time.After(ss.cfg.Consumer.RetryBackoff):
		case <-cancelCh:
			return func() {}
		}
		err = ss.groupMemberZNode.ClaimPartition(topic, partition)
	}
	claimerActDesc.Log().Infof("partition claimed: via=%s, retries=%d, took=%s",
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
			logFailureFn("failed to release partition: via=%s, retries=%d, took=%s",
				ss.actDesc, retries, millisSince(beginAt))
			<-time.After(ss.cfg.Consumer.RetryBackoff)
			err = ss.groupMemberZNode.ReleasePartition(topic, partition)
		}
		claimerActDesc.Log().Infof("partition released: via=%s, retries=%d, took=%s",
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

	// Ensure a group ZNode exist.
	err := ss.groupZNode.Create()
	for err != nil {
		ss.actDesc.Log().WithError(err).Error("failed to create a group znode")
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
			ss.actDesc.Log().WithError(err).Error("failed to deregister")
			<-time.After(ss.cfg.Consumer.RetryBackoff)
			err = ss.groupMemberZNode.Deregister()
		}
	}()

	var (
		nilOrSubscriptionsCh     chan<- map[string][]string
		nilOrGroupUpdatedCh      <-chan zk.Event
		nilOrTimeoutCh           <-chan time.Time
		pendingTopics            []string
		pendingSubscriptions     map[string][]string
		shouldSubmitTopics       = false
		shouldFetchMembers       = false
		shouldFetchSubscriptions = false
		members                  []*kazoo.ConsumergroupInstance
	)
	for {
		select {
		case topics := <-ss.topicsCh:
			pendingTopics = normalizeTopics(topics)
			shouldSubmitTopics = !topicsEqual(pendingTopics, ss.topics)
		case nilOrSubscriptionsCh <- pendingSubscriptions:
			nilOrSubscriptionsCh = nil
			ss.subscriptions = pendingSubscriptions
		case <-nilOrGroupUpdatedCh:
			nilOrGroupUpdatedCh = nil
			shouldFetchMembers = true
		case <-nilOrTimeoutCh:
		case <-ss.stopCh:
			return
		}

		if shouldSubmitTopics {
			if err = ss.submitTopics(pendingTopics); err != nil {
				ss.actDesc.Log().WithError(err).Error("failed to submit topics")
				nilOrTimeoutCh = time.After(ss.cfg.Consumer.RetryBackoff)
				continue
			}
			ss.actDesc.Log().Infof("submitted: topics=%v", pendingTopics)
			shouldSubmitTopics = false
			shouldFetchMembers = true
		}

		if shouldFetchMembers {
			members, nilOrGroupUpdatedCh, err = ss.groupZNode.WatchInstances()
			if err != nil {
				ss.actDesc.Log().WithError(err).Error("failed to watch members")
				nilOrTimeoutCh = time.After(ss.cfg.Consumer.RetryBackoff)
				continue
			}
			shouldFetchMembers = false
			shouldFetchSubscriptions = true
			// To avoid unnecessary rebalancing in case of a deregister/register
			// sequences that happen when a member updates its topic subscriptions,
			// we delay subscription fetching.
			nilOrTimeoutCh = time.After(ss.cfg.Consumer.RebalanceDelay)
			continue
		}

		if shouldFetchSubscriptions {
			pendingSubscriptions, err = ss.fetchSubscriptions(members)
			if err != nil {
				ss.actDesc.Log().WithError(err).Error("failed to fetch subscriptions")
				nilOrTimeoutCh = time.After(ss.cfg.Consumer.RetryBackoff)
				continue
			}
			shouldFetchSubscriptions = false
			ss.actDesc.Log().Infof("fetched subscriptions: %v", pendingSubscriptions)
			if subscriptionsEqual(pendingSubscriptions, ss.subscriptions) {
				nilOrSubscriptionsCh = nil
				pendingSubscriptions = nil
				ss.actDesc.Log().Infof("redundant group update ignored: %v", ss.subscriptions)
				continue
			}
			nilOrSubscriptionsCh = ss.subscriptionsCh
		}
	}
}

// fetchSubscriptions retrieves registration records for the specified members
// from ZooKeeper.
//
// FIXME: It is assumed that all members of the group are registered with the
// FIXME: `static` pattern. If a member that pattern is either `white_list` or
// FIXME: `black_list` joins the group the result will be unpredictable.
func (ss *T) fetchSubscriptions(members []*kazoo.ConsumergroupInstance) (map[string][]string, error) {
	subscriptions := make(map[string][]string, len(members))
	for _, member := range members {
		var registration *kazoo.Registration
		registration, err := member.Registration()
		for err != nil {
			return nil, errors.Wrapf(err, "failed to fetch registration, member=%s", member.ID)
		}
		// Sort topics to ensure deterministic output.
		topics := make([]string, 0, len(registration.Subscription))
		for topic := range registration.Subscription {
			topics = append(topics, topic)
		}
		subscriptions[member.ID] = normalizeTopics(topics)
	}
	return subscriptions, nil
}

func (ss *T) submitTopics(topics []string) error {
	if ss.topics != nil {
		err := ss.groupMemberZNode.Deregister()
		if err != nil && err != kazoo.ErrInstanceNotRegistered {
			return errors.Wrap(err, "failed to deregister")
		}
	}
	ss.topics = nil
	err := ss.groupMemberZNode.Register(topics)
	for err != nil {
		return errors.Wrap(err, "failed to register")
	}
	ss.topics = topics
	return nil
}

func normalizeTopics(s []string) []string {
	if s == nil || len(s) == 0 {
		return nil
	}
	sort.Strings(s)
	return s
}

func topicsEqual(lhs, rhs []string) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for i := range lhs {
		if lhs[i] != rhs[i] {
			return false
		}
	}
	return true
}

func subscriptionsEqual(lhs, rhs map[string][]string) bool {
	if len(lhs) != len(rhs) {
		return false
	}
	for member, lhsTopics := range lhs {
		if !topicsEqual(lhsTopics, rhs[member]) {
			return false
		}
	}
	return true
}

func millisSince(t time.Time) time.Duration {
	return time.Now().Sub(t) / time.Millisecond * time.Millisecond
}
