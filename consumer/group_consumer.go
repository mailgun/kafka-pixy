package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer/consumermsg"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/consumer/groupmember"
	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/kafka-pixy/consumer/partitioncsm"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/log"
	"github.com/wvanbergen/kazoo-go"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type groupConsumer struct {
	supervisorActorID       *actor.ID
	managerActorID          *actor.ID
	cfg                     *config.T
	group                   string
	dispatcher              *dispatcher.T
	kafkaClient             sarama.Client
	partitionCsmFactory     partitioncsm.Factory
	offsetMgrFactory        offsetmgr.Factory
	kazooConn               *kazoo.Kazoo
	groupMember             *groupmember.T
	multiplexers            map[string]*multiplexer.T
	topicConsumerLifespanCh chan *topicConsumer
	stoppingCh              chan none.T
	wg                      sync.WaitGroup

	// Exist just to be overridden in tests with mocks.
	fetchTopicPartitionsFn func(topic string) ([]int32, error)
	muxInputsAsyncFn       func()
}

func (sc *T) newConsumerGroup(group string) *groupConsumer {
	supervisorActorID := sc.namespace.NewChild(fmt.Sprintf("G:%s", group))
	gc := &groupConsumer{
		supervisorActorID:       supervisorActorID,
		managerActorID:          supervisorActorID.NewChild("manager"),
		cfg:                     sc.cfg,
		group:                   group,
		kafkaClient:             sc.kafkaClient,
		offsetMgrFactory:        sc.offsetMgrFactory,
		kazooConn:               sc.kazooConn,
		multiplexers:            make(map[string]*multiplexer.T),
		topicConsumerLifespanCh: make(chan *topicConsumer),
		stoppingCh:              make(chan none.T),

		fetchTopicPartitionsFn: sc.kafkaClient.Partitions,
	}
	gc.dispatcher = dispatcher.New(gc.supervisorActorID, gc, sc.cfg)
	return gc
}

func (gc *groupConsumer) String() string {
	return gc.supervisorActorID.String()
}

func (gc *groupConsumer) topicConsumerLifespan() chan<- *topicConsumer {
	return gc.topicConsumerLifespanCh
}

// implements `dispatcher.Tier`.
func (gc *groupConsumer) Key() string {
	return gc.group
}

// implements `dispatcher.Tier`.
func (gc *groupConsumer) Requests() chan<- dispatcher.Request {
	return gc.dispatcher.Requests()
}

// implements `dispatcher.Tier`.
func (gc *groupConsumer) Start(stoppedCh chan<- dispatcher.Tier) {
	actor.Spawn(gc.supervisorActorID, &gc.wg, func() {
		defer func() { stoppedCh <- gc }()
		var err error
		gc.partitionCsmFactory, err = partitioncsm.SpawnFactory(gc.supervisorActorID, gc.kafkaClient)
		if err != nil {
			// Must never happen.
			panic(consumermsg.ErrSetup(fmt.Errorf("failed to create sarama.Consumer: err=(%v)", err)))
		}
		gc.groupMember = groupmember.Spawn(gc.supervisorActorID, gc.group, gc.cfg.ClientID, gc.cfg, gc.kazooConn)
		var manageWg sync.WaitGroup
		actor.Spawn(gc.managerActorID, &manageWg, gc.runManager)
		gc.dispatcher.Start()
		// Wait for a stop signal and shutdown gracefully when one is received.
		<-gc.stoppingCh
		gc.dispatcher.Stop()
		gc.groupMember.Stop()
		manageWg.Wait()
		gc.partitionCsmFactory.Stop()
	})
}

// implements `dispatcher.Tier`.
func (gc *groupConsumer) Stop() {
	close(gc.stoppingCh)
	gc.wg.Wait()
}

// implements `dispatcher.Factory`.
func (gc *groupConsumer) KeyOf(req dispatcher.Request) string {
	return req.Topic
}

// implements `dispatcher.Factory`.
func (gc *groupConsumer) NewTier(key string) dispatcher.Tier {
	tc := gc.newTopicConsumer(key)
	return tc
}

func (gc *groupConsumer) runManager() {
	var (
		topicConsumers                = make(map[string]*topicConsumer)
		topics                        []string
		subscriptions                 map[string][]string
		ok                            = true
		nilOrRetryCh                  <-chan time.Time
		nilOrRegistryTopicsCh         chan<- []string
		shouldRebalance, canRebalance = false, true
		rebalanceResultCh             = make(chan error, 1)
	)
	for {
		select {
		case tc := <-gc.topicConsumerLifespanCh:
			// It is assumed that only one topicConsumer can exist for a
			// particular topic at a time.
			if topicConsumers[tc.topic] == tc {
				delete(topicConsumers, tc.topic)
			} else {
				topicConsumers[tc.topic] = tc
			}
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = gc.groupMember.Topics()
			continue
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue
		case subscriptions, ok = <-gc.groupMember.Subscriptions():
			if !ok {
				goto done
			}
			nilOrRetryCh = nil
			shouldRebalance = true
		case err := <-rebalanceResultCh:
			canRebalance = true
			if err != nil {
				log.Errorf("<%s> rebalance failed: err=(%s)", gc.managerActorID, err)
				nilOrRetryCh = time.After(gc.cfg.Consumer.BackOffTimeout)
				continue
			}
		case <-nilOrRetryCh:
			shouldRebalance = true
		}

		if shouldRebalance && canRebalance {
			// Copy topicConsumers to make sure `rebalance` doesn't see any
			// changes we make while it is running.
			topicConsumerCopy := make(map[string]*topicConsumer, len(topicConsumers))
			for topic, tc := range topicConsumers {
				topicConsumerCopy[topic] = tc
			}
			rebalancerActorID := gc.managerActorID.NewChild("rebalancer")
			subscriptions := subscriptions
			actor.Spawn(rebalancerActorID, nil, func() {
				gc.runRebalancer(rebalancerActorID, topicConsumerCopy, subscriptions, rebalanceResultCh)
			})
			shouldRebalance, canRebalance = false, false
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

func (gc *groupConsumer) runRebalancer(actorID *actor.ID, topicConsumers map[string]*topicConsumer,
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
		gc.rewireMuxAsync(&wg, tcg, topicConsumers[topic], assignedPartitions[topic])
	}
	// Start consuming partitions for topics that has not been consumed before.
	for topic, assignedTopicPartitions := range assignedPartitions {
		tc := topicConsumers[topic]
		mux := gc.multiplexers[topic]
		if tc == nil || mux != nil {
			continue
		}
		topic := topic
		spawnInF := func(partition int32) multiplexer.In {
			return gc.spawnExclusiveConsumer(gc.supervisorActorID, topic, partition)
		}
		mux = multiplexer.New(gc.supervisorActorID, spawnInF)
		gc.rewireMuxAsync(&wg, mux, tc, assignedTopicPartitions)
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

// muxInputsAsync calls muxInputs in another goroutine.
func (gc *groupConsumer) rewireMuxAsync(wg *sync.WaitGroup, mux *multiplexer.T, tc *topicConsumer, assigned []int32) {
	actor.Spawn(gc.supervisorActorID.NewChild("rewire", tc.topic), wg, func() {
		mux.WireUp(tc, assigned)
	})
}

// resolvePartitions given topic subscriptions of all consumer group members,
// resolves what topic partitions are assigned to the specified group member.
func (gc *groupConsumer) resolvePartitions(subscriptions map[string][]string) (
	assignedPartitions map[string][]int32, err error,
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
	assignedPartitions = make(map[string][]int32)
	for topic := range subscribedTopics {
		topicPartitions, err := gc.fetchTopicPartitionsFn(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get partition list: topic=%s, err=(%s)", topic, err)
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

func listTopics(topicConsumers map[string]*topicConsumer) []string {
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
