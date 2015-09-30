package pixy

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/go-zookeeper/zk"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/wvanbergen/kazoo-go"
)

// consumerGroupRegistry maintains proper consumer group member registration
// in ZooKeeper based on the number of topics the respective consumer group
// member consumes at the moment. The list of consumed topics is managed via
// `Topics()` channel. It also watches for other group members that join/leave
// the group. Updated group member subscriptions are sent down to the
// `MembershipChanges()` channel.
type consumerGroupRegistry struct {
	baseCID             *sarama.ContextID
	group               string
	config              *Config
	groupZNode          *kazoo.Consumergroup
	groupMemberZNode    *kazoo.ConsumergroupInstance
	topicsCh            chan []string
	membershipChangesCh chan map[string][]string
	stoppingCh          chan none
	wg                  sync.WaitGroup
}

func spawnConsumerGroupRegister(group, memberID string, config *Config, kazooConn *kazoo.Kazoo) *consumerGroupRegistry {
	groupZNode := kazooConn.Consumergroup(group)
	groupMemberZNode := groupZNode.Instance(memberID)
	cgr := &consumerGroupRegistry{
		baseCID:             sarama.RootCID.NewChild(fmt.Sprintf("registry:%s", group)),
		group:               group,
		config:              config,
		groupZNode:          groupZNode,
		groupMemberZNode:    groupMemberZNode,
		topicsCh:            make(chan []string),
		membershipChangesCh: make(chan map[string][]string),
		stoppingCh:          make(chan none),
	}
	spawn(&cgr.wg, cgr.watcher)
	spawn(&cgr.wg, cgr.register)
	return cgr
}

func (cgr *consumerGroupRegistry) topics() chan<- []string {
	return cgr.topicsCh
}

func (cgr *consumerGroupRegistry) membershipChanges() <-chan map[string][]string {
	return cgr.membershipChangesCh
}

func (cgr *consumerGroupRegistry) claimPartition(cid *sarama.ContextID, topic string, partition int32, cancelCh <-chan none) func() {
	if !retry(func() error { return cgr.groupMemberZNode.ClaimPartition(topic, partition) }, nil,
		fmt.Sprintf("<%s> failed to claim partition", cid), cgr.config.Consumer.BackOffTimeout, cancelCh,
	) {
		log.Infof("<%s> partition claimed", cid)
	}
	return func() {
		if !retry(func() error { return cgr.groupMemberZNode.ReleasePartition(topic, partition) },
			func(err error) bool { return err != nil && err != kazoo.ErrPartitionNotClaimed },
			fmt.Sprintf("<%s> failed to release partition", cid), cgr.config.Consumer.BackOffTimeout, cancelCh,
		) {
			log.Infof("<%s> partition released", cid)
		}
	}
}

func (cgr *consumerGroupRegistry) stop() {
	close(cgr.stoppingCh)
	cgr.wg.Wait()
}

// partitionOwner returns the id of the consumer group member that has claimed
// the specified topic/partition.
func (cgr *consumerGroupRegistry) partitionOwner(topic string, partition int32) (string, error) {
	owner, err := cgr.groupZNode.PartitionOwner(topic, partition)
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
// changes introduced by the `register` goroutine of this `consumerGroupRegistry`
// instance are also detected, and that is by design.
func (cgr *consumerGroupRegistry) watcher() {
	cid := cgr.baseCID.NewChild("watcher")
	defer cid.LogScope()()
	defer close(cgr.membershipChangesCh)

	if cgr.retry(cgr.groupZNode.Create, nil, fmt.Sprintf("<%s> failed to create a group znode", cid)) {
		return
	}
watchLoop:
	for {
		var members []*kazoo.ConsumergroupInstance
		var membershipChangedCh <-chan zk.Event
		if cgr.retry(
			func() error {
				var err error
				members, membershipChangedCh, err = cgr.groupZNode.WatchInstances()
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
		case <-cgr.stoppingCh:
			return
		case <-time.After(cgr.config.Consumer.RebalanceDelay):
		}

		cgr.sendMembershipUpdate(cid, members)

		// Wait for another membership change or a signal to quit.
		select {
		case <-membershipChangedCh:
			continue watchLoop
		case <-cgr.stoppingCh:
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
func (cgr *consumerGroupRegistry) sendMembershipUpdate(cid *sarama.ContextID, members []*kazoo.ConsumergroupInstance) {
	log.Infof("<%s> fetching group subscriptions...", cid)
	subscriptions := make(map[string][]string, len(members))
	for _, member := range members {
		var registration *kazoo.Registration
		if cgr.retry(
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
	case cgr.membershipChangesCh <- subscriptions:
	case <-cgr.stoppingCh:
		return
	}
}

// register listens for topic subscription updates on the `topicsCh` channel
// and updates the member registration in ZooKeeper accordingly.
func (cgr *consumerGroupRegistry) register() {
	cid := cgr.baseCID.NewChild("register")
	defer cid.LogScope()()
	defer cgr.retry(cgr.groupMemberZNode.Deregister,
		func(err error) bool { return err != nil && err != kazoo.ErrInstanceNotRegistered },
		fmt.Sprintf("<%s> failed to deregister", cid))

	for {
		var topics []string
		select {
		case topics = <-cgr.topicsCh:
		case <-cgr.stoppingCh:
			return
		}
		sort.Sort(sort.StringSlice(topics))

		log.Infof("<%s> registering...: id=%s, topics=%v", cid, cgr.groupMemberZNode.ID, topics)
		if cgr.retry(
			func() error {
				if err := cgr.groupMemberZNode.Deregister(); err != nil && err != kazoo.ErrInstanceNotRegistered {
					return fmt.Errorf("could not deregister: err=(%s)", err)
				}
				return cgr.groupMemberZNode.Register(topics)
			},
			nil, fmt.Sprintf("<%s> failed to register", cid),
		) {
			return
		}
		log.Infof("<%s> registered: id=%s, topics=%v", cid, cgr.groupMemberZNode.ID, topics)
	}
}

func (cgr *consumerGroupRegistry) retry(f func() error, shouldRetry func(err error) bool, errorMsg string) (canceled bool) {
	return retry(f, shouldRetry, errorMsg, cgr.config.Consumer.BackOffTimeout, cgr.stoppingCh)
}
