package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/context"
	"github.com/mailgun/log"
	"github.com/mailgun/sarama"
	"github.com/wvanbergen/kazoo-go"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type groupConsumer struct {
	baseCID                 *context.ID
	cfg                     *config.T
	group                   string
	dispatcher              *dispatcher
	kafkaClient             sarama.Client
	dumbConsumer            sarama.Consumer
	offsetMgr               sarama.OffsetManager
	kazooConn               *kazoo.Kazoo
	registry                *groupRegistrator
	topicConsumerGears      map[string]*topicConsumerGear
	topicConsumerLifespanCh chan *topicConsumer
	stoppingCh              chan none
	wg                      sync.WaitGroup

	// Exist just to be overridden in tests with mocks.
	fetchTopicPartitionsFn func(topic string) ([]int32, error)
	muxInputsAsyncFn       func()
}

func (sc *T) newConsumerGroup(group string) *groupConsumer {
	gc := &groupConsumer{
		baseCID:                 sc.baseCID.NewChild(group),
		cfg:                     sc.cfg,
		group:                   group,
		kafkaClient:             sc.kafkaClient,
		offsetMgr:               sc.offsetMgr,
		kazooConn:               sc.kazooConn,
		topicConsumerGears:      make(map[string]*topicConsumerGear),
		topicConsumerLifespanCh: make(chan *topicConsumer),
		stoppingCh:              make(chan none),

		fetchTopicPartitionsFn: sc.kafkaClient.Partitions,
	}
	gc.dispatcher = newDispatcher(gc.baseCID, gc, sc.cfg)
	return gc
}

func (gc *groupConsumer) String() string {
	return gc.baseCID.String()
}

func (gc *groupConsumer) topicConsumerLifespan() chan<- *topicConsumer {
	return gc.topicConsumerLifespanCh
}

func (gc *groupConsumer) key() string {
	return gc.group
}

func (gc *groupConsumer) start(stoppedCh chan<- dispatchTier) {
	spawn(&gc.wg, func() {
		defer func() { stoppedCh <- gc }()
		var err error
		gc.dumbConsumer, err = sarama.NewConsumerFromClient(gc.kafkaClient)
		if err != nil {
			// Must never happen.
			panic(ErrSetup(fmt.Errorf("failed to create sarama.Consumer: err=(%v)", err)))
		}
		gc.registry = spawnGroupRegistrator(gc.group, gc.cfg.ClientID, gc.cfg, gc.kazooConn)
		var manageWg sync.WaitGroup
		spawn(&manageWg, gc.managePartitions)
		gc.dispatcher.start()
		// Wait for a stop signal and shutdown gracefully when one is received.
		<-gc.stoppingCh
		gc.dispatcher.stop()
		gc.registry.stop()
		manageWg.Wait()
		gc.dumbConsumer.Close()
	})
}

func (gc *groupConsumer) requests() chan<- consumeRequest {
	return gc.dispatcher.requests()
}

func (gc *groupConsumer) stop() {
	close(gc.stoppingCh)
	gc.wg.Wait()
}

func (gc *groupConsumer) dispatchKey(req consumeRequest) string {
	return req.topic
}

func (gc *groupConsumer) newDispatchTier(key string) dispatchTier {
	tc := gc.newTopicConsumer(key)
	return tc
}

func (gc *groupConsumer) managePartitions() {
	cid := gc.baseCID.NewChild("managePartitions")
	defer cid.LogScope()()
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
			nilOrRegistryTopicsCh = gc.registry.topics()
			continue
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue
		case subscriptions, ok = <-gc.registry.membershipChanges():
			if !ok {
				goto done
			}
			nilOrRetryCh = nil
			shouldRebalance = true
		case err := <-rebalanceResultCh:
			canRebalance = true
			if err != nil {
				log.Errorf("<%s> rebalance failed: err=(%s)", cid, err)
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
			go gc.rebalance(topicConsumerCopy, subscriptions, rebalanceResultCh)
			shouldRebalance, canRebalance = false, false
		}
	}
done:
	var wg sync.WaitGroup
	for _, tcg := range gc.topicConsumerGears {
		spawn(&wg, tcg.stop)
	}
	wg.Wait()
}

func (gc *groupConsumer) rebalance(topicConsumers map[string]*topicConsumer,
	subscriptions map[string][]string, rebalanceResultCh chan<- error,
) {
	cid := gc.baseCID.NewChild("rebalance")
	defer cid.LogScope(topicConsumers, subscriptions)()

	assignedPartitions, err := gc.resolvePartitions(subscriptions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	log.Infof("<%s> assigned partitions: %v", cid, assignedPartitions)
	var wg sync.WaitGroup
	// Stop consuming partitions that are no longer assigned to this group
	// and start consuming newly assigned partitions for topics that has been
	// consumed already.
	for topic, tcg := range gc.topicConsumerGears {
		tcg.muxInputsAsync(&wg, topicConsumers[topic], assignedPartitions[topic])
	}
	// Start consuming partitions for topics that has not been consumed before.
	for topic, assignedTopicPartitions := range assignedPartitions {
		tc := topicConsumers[topic]
		tcg := gc.topicConsumerGears[topic]
		if tc == nil || tcg != nil {
			continue
		}
		tcg = newTopicConsumerGear(gc.spawnTopicInput)
		tcg.muxInputsAsync(&wg, tc, assignedTopicPartitions)
		gc.topicConsumerGears[topic] = tcg
	}
	wg.Wait()
	// Clean up gears for topics that do not have assigned partitions anymore.
	for topic, tcg := range gc.topicConsumerGears {
		if tcg.isIdle() {
			delete(gc.topicConsumerGears, topic)
		}
	}
	// Notify the caller that rebalancing has completed successfully.
	rebalanceResultCh <- nil
	return
}

func (gc *groupConsumer) spawnTopicInput(topic string, partition int32) muxInputActor {
	return gc.spawnExclusiveConsumer(topic, partition)
}

// resolvePartitions given topic subscriptions of all consumer group members,
// resolves what topic partitions are assigned to the specified group member.
func (gc *groupConsumer) resolvePartitions(membersToTopics map[string][]string) (
	assignedPartitions map[string][]int32, err error,
) {
	// Convert members->topics to topic->members map.
	topicsToMembers := make(map[string][]string)
	for groupMemberID, topics := range membersToTopics {
		for _, topic := range topics {
			topicsToMembers[topic] = append(topicsToMembers[topic], groupMemberID)
		}
	}
	// Create a set of topics this consumer group member subscribed to.
	subscribedTopics := make(map[string]bool)
	for _, topic := range membersToTopics[gc.cfg.ClientID] {
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
