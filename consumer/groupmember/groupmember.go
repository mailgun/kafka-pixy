package groupmember

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

// GroupMember maintains a consumer group member registration in ZooKeeper,
// watches for other members to join, leave and update their subscriptions, and
// generates notifications of such changes.
type T struct {
	actorID          *actor.ID
	group            string
	config           *config.T
	groupZNode       *kazoo.Consumergroup
	groupMemberZNode *kazoo.ConsumergroupInstance
	topics           []string
	subscriptions    map[string][]string
	topicsCh         chan []string
	subscriptionsCh  chan map[string][]string
	stopCh           chan none.T
	wg               sync.WaitGroup
}

// Spawn creates a consumer group member instance and starts its background
// goroutines.
func Spawn(group, memberID string, config *config.T, kazooConn *kazoo.Kazoo) *T {
	groupZNode := kazooConn.Consumergroup(group)
	groupMemberZNode := groupZNode.Instance(memberID)
	gm := &T{
		actorID:          actor.RootID.NewChild(fmt.Sprintf("groupMember:%s", group)),
		group:            group,
		config:           config,
		groupZNode:       groupZNode,
		groupMemberZNode: groupMemberZNode,
		topicsCh:         make(chan []string),
		subscriptionsCh:  make(chan map[string][]string),
		stopCh:           make(chan none.T),
	}
	actor.Spawn(gm.actorID, &gm.wg, gm.run)
	return gm
}

// Topics returns a channel to receive a list of topics the member should
// subscribe to. To make the member unsubscribe from all topics either nil or
// an empty topic list can be sent.
func (gm *T) Topics() chan<- []string {
	return gm.topicsCh
}

// Subscriptions returns a channel that subscriptions will be sent whenever a
// member joins or leaves the group or when an existing member updates its
// subscription.
func (gm *T) Subscriptions() <-chan map[string][]string {
	return gm.subscriptionsCh
}

// ClaimPartition claims a topic/partition to be consumed by this member of the
// consumer group. It blocks until either succeeds or canceled by the caller. It
// returns a function that should be called to release the claim.
func (gm *T) ClaimPartition(actorID *actor.ID, topic string, partition int32, cancelCh <-chan none.T) func() {
	err := gm.groupMemberZNode.ClaimPartition(topic, partition)
	for err != nil {
		log.Errorf("<%s> failed to claim partition: err=(%s)", gm.actorID, err)
		select {
		case <-time.After(gm.config.Consumer.BackOffTimeout):
		case <-cancelCh:
			return func() {}
		}
		err = gm.groupMemberZNode.ClaimPartition(topic, partition)
	}
	log.Infof("<%s> partition claimed", actorID)
	return func() {
		err := gm.groupMemberZNode.ReleasePartition(topic, partition)
		for err != nil && err != kazoo.ErrPartitionNotClaimed {
			log.Errorf("<%s> failed to release partition: err=(%s)", gm.actorID, err)
			<-time.After(gm.config.Consumer.BackOffTimeout)
			err = gm.groupMemberZNode.ReleasePartition(topic, partition)
		}
		log.Infof("<%s> partition released", actorID)
	}
}

// Stop signals the consumer group member to stop and blocks until its
// goroutines are over.
func (gm *T) Stop() {
	close(gm.stopCh)
	gm.wg.Wait()
}

func (gm *T) run() {
	defer close(gm.subscriptionsCh)

	// Ensure a group ZNode exist.
	err := gm.groupZNode.Create()
	for err != nil {
		log.Errorf("<%s> failed to create a group znode: err=(%s)", gm.actorID, err)
		select {
		case <-time.After(gm.config.Consumer.BackOffTimeout):
		case <-gm.stopCh:
			return
		}
		err = gm.groupZNode.Create()
	}

	defer func() {
		err := gm.submitTopics(nil)
		for err != nil {
			log.Errorf("<%s> failed to create a group znode: err=(%s)", gm.actorID, err)
			<-time.After(gm.config.Consumer.BackOffTimeout)
			err = gm.submitTopics(nil)
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
		case topics := <-gm.topicsCh:
			pendingTopics = normalizeTopics(topics)
			shouldSubmitTopics = !topicsEqual(pendingTopics, gm.topics)
		case nilOrSubscriptionsCh <- pendingSubscriptions:
			nilOrSubscriptionsCh = nil
			gm.subscriptions = pendingSubscriptions
		case <-nilOrGroupUpdatedCh:
			nilOrGroupUpdatedCh = nil
			shouldFetchMembers = (gm.topics != nil)
		case <-nilOrTimeoutCh:
		case <-gm.stopCh:
			return
		}

		if shouldSubmitTopics {
			if err = gm.submitTopics(pendingTopics); err != nil {
				log.Errorf("<%s> failed to submit topics: err=(%s)", gm.actorID, err)
				nilOrTimeoutCh = time.After(gm.config.Consumer.BackOffTimeout)
				continue
			}
			log.Infof("<%s> submitted: topics=%v", gm.actorID, pendingTopics)
			shouldSubmitTopics = false
			shouldFetchMembers = (gm.topics != nil)
		}

		if shouldFetchMembers {
			members, nilOrGroupUpdatedCh, err = gm.groupZNode.WatchInstances()
			if err != nil {
				log.Errorf("<%s> failed to watch members: err=(%s)", gm.actorID, err)
				nilOrTimeoutCh = time.After(gm.config.Consumer.BackOffTimeout)
				continue
			}
			shouldFetchMembers = false
			shouldFetchSubscriptions = true
			// To avoid unnecessary rebalancing in case of a deregister/register
			// sequences that happen when a member updates its topic subscriptions,
			// we delay subscription fetching.
			nilOrTimeoutCh = time.After(gm.config.Consumer.RebalanceDelay)
			continue
		}

		if shouldFetchSubscriptions {
			pendingSubscriptions, err = gm.fetchSubscriptions(members)
			if err != nil {
				log.Errorf("<%s> failed to fetch subscriptions: err=(%s)", gm.actorID, err)
				nilOrTimeoutCh = time.After(gm.config.Consumer.BackOffTimeout)
				continue
			}
			log.Infof("<%s> fetched subscriptions: %v", gm.actorID, pendingSubscriptions)
			shouldFetchSubscriptions = false
			if subscriptionsEqual(pendingSubscriptions, gm.subscriptions) {
				log.Infof("<%s> redundent group update ignored: %v", gm.actorID, pendingSubscriptions)
				continue
			}
			nilOrSubscriptionsCh = gm.subscriptionsCh
		}
	}
}

// fetchSubscriptions retrieves registration records for the specified members
// from ZooKeeper.
//
// FIXME: It is assumed that all members of the group are registered with the
// FIXME: `static` pattern. If a member that pattern is either `white_list` or
// FIXME: `black_list` joins the group the result will be unpredictable.
func (gm *T) fetchSubscriptions(members []*kazoo.ConsumergroupInstance) (map[string][]string, error) {
	subscriptions := make(map[string][]string, len(members))
	for _, member := range members {
		var registration *kazoo.Registration
		registration, err := member.Registration()
		for err != nil {
			return nil, fmt.Errorf("failed to fetch registration: member=%s, err=(%s)", member.ID, err)
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

func (gm *T) submitTopics(topics []string) error {
	if gm.topics != nil {
		err := gm.groupMemberZNode.Deregister()
		if err != nil && err != kazoo.ErrInstanceNotRegistered {
			return fmt.Errorf("failed to deregister: err=(%s)", err)
		}
	}
	gm.topics = nil
	if topics != nil {
		err := gm.groupMemberZNode.Register(topics)
		for err != nil {
			return fmt.Errorf("failed to register: err=(%s)", err)
		}
	}
	gm.topics = topics
	return nil
}

func normalizeTopics(s []string) []string {
	if s == nil || len(s) == 0 {
		return nil
	}
	sort.Sort(sort.StringSlice(s))
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
