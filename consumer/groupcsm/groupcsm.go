package groupcsm

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/msgistream"
	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
	"github.com/mailgun/kafka-pixy/consumer/partitioncsm"
	"github.com/mailgun/kafka-pixy/consumer/topiccsm"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
	"github.com/wvanbergen/kazoo-go"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
//
// implements `dispatcher.Factory`.
// implements `dispatcher.Tier`.
type T struct {
	supActorID         *actor.ID
	mgrActorID         *actor.ID
	cfg                *config.Proxy
	group              string
	dispatcher         *dispatcher.T
	kafkaClt           sarama.Client
	kazooClt           *kazoo.Kazoo
	msgIStreamF        msgistream.Factory
	offsetMgrF         offsetmgr.Factory
	groupMember        *groupmember.T
	multiplexers       map[string]*multiplexer.T
	topicCsmLifespanCh chan *topiccsm.T
	stopCh             chan none.T
	wg                 sync.WaitGroup

	// Exist just to be overridden in tests with mocks.
	fetchTopicPartitionsFn func(topic string) ([]int32, error)
}

func New(namespace *actor.ID, group string, cfg *config.Proxy, kafkaClt sarama.Client,
	kazooClt *kazoo.Kazoo, offsetMgrF offsetmgr.Factory,
) *T {
	supervisorActorID := namespace.NewChild(fmt.Sprintf("G:%s", group))
	gc := &T{
		supActorID:         supervisorActorID,
		mgrActorID:         supervisorActorID.NewChild("manager"),
		cfg:                cfg,
		group:              group,
		kafkaClt:           kafkaClt,
		kazooClt:           kazooClt,
		offsetMgrF:         offsetMgrF,
		multiplexers:       make(map[string]*multiplexer.T),
		topicCsmLifespanCh: make(chan *topiccsm.T),
		stopCh:             make(chan none.T),

		fetchTopicPartitionsFn: kafkaClt.Partitions,
	}
	gc.dispatcher = dispatcher.New(gc.supActorID, gc, cfg)
	return gc
}

// implements `dispatcher.Factory`.
func (gc *T) KeyOf(req dispatcher.Request) string {
	return req.Topic
}

// implements `dispatcher.Factory`.
func (gc *T) NewTier(key string) dispatcher.Tier {
	tc := topiccsm.New(gc.supActorID, gc.group, key, gc.cfg, gc.topicCsmLifespanCh)
	return tc
}

// implements `dispatcher.Tier`.
func (gc *T) Key() string {
	return gc.group
}

// implements `dispatcher.Tier`.
func (gc *T) Requests() chan<- dispatcher.Request {
	return gc.dispatcher.Requests()
}

// implements `dispatcher.Tier`.
func (gc *T) Start(stoppedCh chan<- dispatcher.Tier) {
	actor.Spawn(gc.supActorID, &gc.wg, func() {
		defer func() { stoppedCh <- gc }()
		var err error
		gc.msgIStreamF, err = msgistream.SpawnFactory(gc.supActorID, gc.cfg, gc.kafkaClt)
		if err != nil {
			// Must never happen.
			panic(errors.Wrap(err, "failed to create sarama.Consumer"))
		}
		gc.groupMember = groupmember.Spawn(gc.supActorID, gc.group, gc.cfg.ClientID, gc.cfg, gc.kazooClt)
		var manageWg sync.WaitGroup
		actor.Spawn(gc.mgrActorID, &manageWg, gc.runManager)
		gc.dispatcher.Start()
		// Wait for a stop signal and shutdown gracefully when one is received.
		<-gc.stopCh
		gc.dispatcher.Stop()
		gc.groupMember.Stop()
		manageWg.Wait()
		gc.msgIStreamF.Stop()
	})
}

// implements `dispatcher.Tier`.
func (gc *T) Stop() {
	close(gc.stopCh)
	gc.wg.Wait()
}

// String return string ID of this group consumer to be posted in logs.
func (gc *T) String() string {
	return gc.supActorID.String()
}

func (gc *T) runManager() {
	var (
		topicConsumers        = make(map[string]*topiccsm.T)
		topics                []string
		subscriptions         map[string][]string
		ok                    = true
		nilOrRetryCh          <-chan time.Time
		nilOrRegistryTopicsCh chan<- []string
		rebalancingRequired   = false
		rebalancingInProgress = false
		retryScheduled        = false
		stopped               = false
		rebalanceResultCh     = make(chan error, 1)
	)
	for {
		select {
		case tc := <-gc.topicCsmLifespanCh:
			// It is assumed that only one topicConsumer can exist for a
			// particular topic at a time.
			if topicConsumers[tc.Topic()] == tc {
				delete(topicConsumers, tc.Topic())
			} else {
				topicConsumers[tc.Topic()] = tc
			}
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = gc.groupMember.Topics()
			continue
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue
		case subscriptions, ok = <-gc.groupMember.Subscriptions():
			nilOrRetryCh = nil
			if !ok {
				if !rebalancingInProgress {
					goto done
				}
				stopped = true
				continue
			}
			rebalancingRequired = true
		case err := <-rebalanceResultCh:
			rebalancingInProgress = false
			if err != nil {
				log.Errorf("<%s> rebalancing failed: err=(%s)", gc.mgrActorID, err)
				if stopped {
					goto done
				}
				nilOrRetryCh = time.After(gc.cfg.Consumer.RetryBackoff)
				retryScheduled = true
			}
			if stopped {
				goto done
			}

		case <-nilOrRetryCh:
			retryScheduled = false
		}

		if rebalancingRequired && !rebalancingInProgress && !retryScheduled {
			actorID := gc.mgrActorID.NewChild("rebalance")
			// Copy topicConsumers to make sure `rebalance` doesn't see any
			// changes we make while it is running.
			topicConsumersCopy := make(map[string]*topiccsm.T, len(topicConsumers))
			for topic, tc := range topicConsumers {
				topicConsumersCopy[topic] = tc
			}
			subscriptions := subscriptions
			actor.Spawn(actorID, nil, func() {
				gc.runRebalancing(actorID, topicConsumersCopy, subscriptions, rebalanceResultCh)
			})
			rebalancingInProgress = true
			rebalancingRequired = false
		}
	}
done:
	var wg sync.WaitGroup
	for _, mux := range gc.multiplexers {
		wg.Add(1)
		go func(mux *multiplexer.T) {
			defer wg.Done()
			mux.Stop()
		}(mux)
	}
	wg.Wait()
}

func (gc *T) runRebalancing(actorID *actor.ID, topicConsumers map[string]*topiccsm.T,
	subscriptions map[string][]string, rebalanceResultCh chan<- error,
) {
	assignedPartitions, err := gc.resolvePartitions(subscriptions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	log.Infof("<%s> assigned partitions: %v", actorID, assignedPartitions)
	var wg sync.WaitGroup
	// Stop consuming partitions that are no longer assigned to this group
	// and start consuming newly assigned partitions for topics that has been
	// consumed already.
	for topic, tcg := range gc.multiplexers {
		gc.rewireMuxAsync(topic, &wg, tcg, topicConsumers[topic], assignedPartitions[topic])
	}
	// Start consuming partitions for topics that has not been consumed before.
	for topic, assignedTopicPartitions := range assignedPartitions {
		tc := topicConsumers[topic]
		mux := gc.multiplexers[topic]
		if tc == nil || mux != nil {
			continue
		}
		topic := topic
		spawnInFn := func(partition int32) multiplexer.In {
			return partitioncsm.Spawn(gc.supActorID, gc.group, topic, partition,
				gc.cfg, gc.groupMember, gc.msgIStreamF, gc.offsetMgrF)
		}
		mux = multiplexer.New(gc.supActorID, spawnInFn)
		gc.rewireMuxAsync(topic, &wg, mux, tc, assignedTopicPartitions)
		gc.multiplexers[topic] = mux
	}
	wg.Wait()
	// Clean up gears for topics that do not have assigned partitions anymore.
	for topic, mux := range gc.multiplexers {
		if !mux.IsRunning() {
			delete(gc.multiplexers, topic)
		}
	}
	// Notify the caller that rebalancing has completed successfully.
	rebalanceResultCh <- nil
	return
}

// rewireMuxAsync calls muxInputs in another goroutine.
func (gc *T) rewireMuxAsync(topic string, wg *sync.WaitGroup, mux *multiplexer.T, tc *topiccsm.T, assigned []int32) {
	actor.Spawn(gc.supActorID.NewChild("rewire", topic), wg, func() {
		if tc == nil {
			// Parameter output is of interface type, therefore nil should be
			// passed explicitly.
			mux.WireUp(nil, nil)
			return
		}
		mux.WireUp(tc, assigned)
	})
}

// resolvePartitions given topic subscriptions of all consumer group members,
// resolves what topic partitions are assigned to the specified group member.
func (gc *T) resolvePartitions(subscriptions map[string][]string) (
	map[string][]int32, error,
) {
	// Convert members->topics to topic->members map.
	topicsToMembers := make(map[string][]string)
	for groupMemberID, topics := range subscriptions {
		for _, topic := range topics {
			topicsToMembers[topic] = append(topicsToMembers[topic], groupMemberID)
		}
	}
	// Create a set of topics this consumer group member subscribed to.
	subscribedTopics := make(map[string]bool)
	for _, topic := range subscriptions[gc.cfg.ClientID] {
		subscribedTopics[topic] = true
	}
	// Resolve new partition assignments for all subscribed topics.
	assignedPartitions := make(map[string][]int32)
	for topic := range subscribedTopics {
		topicPartitions, err := gc.fetchTopicPartitionsFn(topic)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get partition list, topic=%s", topic)
		}
		subscribersToPartitions := assignTopicPartitions(topicPartitions, topicsToMembers[topic])
		assignedTopicPartitions := subscribersToPartitions[gc.cfg.ClientID]
		if len(assignedTopicPartitions) > 0 {
			assignedPartitions[topic] = assignedTopicPartitions
		}
	}
	return assignedPartitions, nil
}

// assignTopicPartitions divides topic partitions among all consumer group
// members subscribed to the topic. The algorithm used closely resembles the
// one implemented by the standard Java High-Level consumer
// (see http://kafka.apache.org/documentation.html#distributionimpl and scroll
// down to *Consumer registration algorithm*) except it does not take in account
// how partitions are distributed among brokers.
func assignTopicPartitions(partitions []int32, subscribers []string) map[string][]int32 {
	partitionCount := len(partitions)
	subscriberCount := len(subscribers)
	if partitionCount == 0 || subscriberCount == 0 {
		return nil
	}
	sort.Sort(Int32Slice(partitions))
	sort.Sort(sort.StringSlice(subscribers))

	subscribersToPartitions := make(map[string][]int32, subscriberCount)
	partitionsPerSubscriber := partitionCount / subscriberCount
	extra := partitionCount - subscriberCount*partitionsPerSubscriber

	begin := 0
	for _, groupMemberID := range subscribers {
		end := begin + partitionsPerSubscriber
		if extra != 0 {
			end++
			extra--
		}
		assigned := partitions[begin:end]
		if len(assigned) > 0 {
			subscribersToPartitions[groupMemberID] = partitions[begin:end]
		}
		begin = end
	}
	return subscribersToPartitions
}

func listTopics(topicConsumers map[string]*topiccsm.T) []string {
	topics := make([]string, 0, len(topicConsumers))
	for topic := range topicConsumers {
		topics = append(topics, topic)
	}
	return topics
}

type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
