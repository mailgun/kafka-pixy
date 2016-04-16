package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/context"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

// GroupRegistrator maintains proper consumer group member registration in
// ZooKeeper based on the number of topics the respective consumer group member
// consumes at the moment. The list of consumed topics is managed via `topics()`
// channel. It also watches for other group members that join/leave the group.
// Updated group member subscriptions are sent down to the `membershipChanges()`
// channel.
type groupRegistrator struct {
	baseCID             *context.ID
	group               string
	config              *config.T
	groupZNode          *kazoo.Consumergroup
	groupMemberZNode    *kazoo.ConsumergroupInstance
	topicsCh            chan []string
	membershipChangesCh chan map[string][]string
	stopCh              chan none.T
	wg                  sync.WaitGroup
}

func spawnGroupRegistrator(group, memberID string, config *config.T, kazooConn *kazoo.Kazoo) *groupRegistrator {
	groupZNode := kazooConn.Consumergroup(group)
	groupMemberZNode := groupZNode.Instance(memberID)
	gr := &groupRegistrator{
		baseCID:             context.RootID.NewChild(fmt.Sprintf("registry:%s", group)),
		group:               group,
		config:              config,
		groupZNode:          groupZNode,
		groupMemberZNode:    groupMemberZNode,
		topicsCh:            make(chan []string),
		membershipChangesCh: make(chan map[string][]string),
		stopCh:              make(chan none.T),
	}
	spawn(&gr.wg, gr.watcher)
	spawn(&gr.wg, gr.register)
	return gr
}

func (gr *groupRegistrator) topics() chan<- []string {
	return gr.topicsCh
}

func (gr *groupRegistrator) membershipChanges() <-chan map[string][]string {
	return gr.membershipChangesCh
}

func (gr *groupRegistrator) claimPartition(cid *context.ID, topic string, partition int32, cancelCh <-chan none.T) func() {
	if !retry(func() error { return gr.groupMemberZNode.ClaimPartition(topic, partition) }, nil,
		fmt.Sprintf("<%s> failed to claim partition", cid), gr.config.Consumer.BackOffTimeout, cancelCh,
	) {
		log.Infof("<%s> partition claimed", cid)
	}
	return func() {
		if !retry(func() error { return gr.groupMemberZNode.ReleasePartition(topic, partition) },
			func(err error) bool { return err != nil && err != kazoo.ErrPartitionNotClaimed },
			fmt.Sprintf("<%s> failed to release partition", cid), gr.config.Consumer.BackOffTimeout, cancelCh,
		) {
			log.Infof("<%s> partition released", cid)
		}
	}
}

func (gr *groupRegistrator) stop() {
	close(gr.stopCh)
	gr.wg.Wait()
}

// partitionOwner returns the id of the consumer group member that has claimed
// the specified topic/partition.
func (gr *groupRegistrator) partitionOwner(topic string, partition int32) (string, error) {
	owner, err := gr.groupZNode.PartitionOwner(topic, partition)
	if err != nil {
		return "", err
	}
	if owner == nil {
		return "", nil
	}
	return owner.ID, nil
}

// watcher keeps an eye on the consumer group membership/subscription changes,
// that is when members join/leave the group and sends notifications about such
// changes down to the `membershipChangesCh` channel. Note that subscription
// changes introduced by the `register` goroutine of this `GroupRegistrator`
// instance are also detected, and that is by design.
func (gr *groupRegistrator) watcher() {
	cid := gr.baseCID.NewChild("watcher")
	defer cid.LogScope()()
	defer close(gr.membershipChangesCh)

	if gr.retry(gr.groupZNode.Create, nil, fmt.Sprintf("<%s> failed to create a group znode", cid)) {
		return
	}
watchLoop:
	for {
		var members []*kazoo.ConsumergroupInstance
		var membershipChangedCh <-chan zk.Event
		if gr.retry(
			func() error {
				var err error
				members, membershipChangedCh, err = gr.groupZNode.WatchInstances()
				// FIXME: Empty member list means that our ZooKeeper session
				// FIXME: has expired. That can be a result of a severe network
				// FIXME: interruption. When a session expires, it means that
				// FIXME: other Kafka-Pixy instance will be able to claim same
				// FIXME: partitions, so we should immediately stop consuming
				// FIXME: them to avoid duplicate consumption.
				if len(members) == 0 {
					return fmt.Errorf("empty members retrieved")
				}
				return err
			},
			nil, fmt.Sprintf("<%s> failed to watch members", cid),
		) {
			return
		}
		// To avoid unnecessary rebalancing in case of a deregister/register
		// sequences that happen when a member updates its topic subscriptions,
		// we delay notification a little bit.
		select {
		case <-membershipChangedCh:
			continue watchLoop
		case <-gr.stopCh:
			return
		case <-time.After(gr.config.Consumer.RebalanceDelay):
		}

		gr.sendMembershipUpdate(cid, members)

		// Wait for another membership change or a signal to quit.
		select {
		case <-membershipChangedCh:
			continue watchLoop
		case <-gr.stopCh:
			return
		}
	}
}

// sendMembershipUpdate retrieves registration records for the specified members
// from ZooKeeper and sends current list of members along with topics they are
// subscribed to down the `membershipChangesCh`. The method can be interrupted
// any time by the stop signal.
//
// FIXME: It is assumed that all members of the group are registered with the
// FIXME: `static` pattern. If a member that pattern is either `white_list` or
// FIXME: `black_list` joins the group the result will be unpredictable.
func (gr *groupRegistrator) sendMembershipUpdate(cid *context.ID, members []*kazoo.ConsumergroupInstance) {
	log.Infof("<%s> fetching group subscriptions...", cid)
	subscriptions := make(map[string][]string, len(members))
	for _, member := range members {
		var registration *kazoo.Registration
		if gr.retry(
			func() error {
				var err error
				registration, err = member.Registration()
				return err
			},
			nil, fmt.Sprintf("<%s> failed to get member registration", cid),
		) {
			return
		}
		// Sort topics to ensure deterministic output.
		topics := make([]string, 0, len(registration.Subscription))
		for topic := range registration.Subscription {
			topics = append(topics, topic)
		}
		sort.Sort(sort.StringSlice(topics))
		subscriptions[member.ID] = topics
	}
	log.Infof("<%s> group subscriptions changed: %v", cid, subscriptions)
	select {
	case gr.membershipChangesCh <- subscriptions:
	case <-gr.stopCh:
		return
	}
}

// register listens for topic subscription updates on the `topicsCh` channel
// and updates the member registration in ZooKeeper accordingly.
func (gr *groupRegistrator) register() {
	cid := gr.baseCID.NewChild("register")
	defer cid.LogScope()()
	defer gr.retry(gr.groupMemberZNode.Deregister,
		func(err error) bool { return err != nil && err != kazoo.ErrInstanceNotRegistered },
		fmt.Sprintf("<%s> failed to deregister", cid))

	for {
		var topics []string
		select {
		case topics = <-gr.topicsCh:
		case <-gr.stopCh:
			return
		}
		sort.Sort(sort.StringSlice(topics))

		log.Infof("<%s> registering...: id=%s, topics=%v", cid, gr.groupMemberZNode.ID, topics)
		if gr.retry(
			func() error {
				if err := gr.groupMemberZNode.Deregister(); err != nil && err != kazoo.ErrInstanceNotRegistered {
					return fmt.Errorf("could not deregister: err=(%s)", err)
				}
				return gr.groupMemberZNode.Register(topics)
			},
			nil, fmt.Sprintf("<%s> failed to register", cid),
		) {
			return
		}
		log.Infof("<%s> registered: id=%s, topics=%v", cid, gr.groupMemberZNode.ID, topics)
	}
}

func (gr *groupRegistrator) retry(f func() error, shouldRetry func(err error) bool, errorMsg string) (canceled bool) {
	return retry(f, shouldRetry, errorMsg, gr.config.Consumer.BackOffTimeout, gr.stopCh)
}

// retry keeps calling the `f` function until it succeeds. `shouldRetry` is
// used to check the error code returned by `f` to decide whether it should be
// retried. If `shouldRetry` is not specified then any non `nil` error will
// result in retry.
func retry(f func() error, shouldRetry func(err error) bool, errorMsg string,
	delay time.Duration, cancelCh <-chan none.T,
) (canceled bool) {
	err := f()
	if shouldRetry == nil {
		shouldRetry = func(err error) bool { return err != nil }
	}
	for shouldRetry(err) {
		log.Errorf("%s: err=(%s), retryIn=%v", errorMsg, err, delay)
		select {
		case <-time.After(delay):
		case <-cancelCh:
			return true
		}
		err = f()
	}
	return false
}
