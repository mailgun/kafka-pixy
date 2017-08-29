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
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
	"github.com/mailgun/kafka-pixy/consumer/partitioncsm"
	"github.com/mailgun/kafka-pixy/consumer/subscriber"
	"github.com/mailgun/kafka-pixy/consumer/topiccsm"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/pkg/errors"
	"github.com/wvanbergen/kazoo-go"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
//
// implements `dispatcher.Factory`.
// implements `dispatcher.Tier`.
type T struct {
	actDesc            *actor.Descriptor
	cfg                *config.Proxy
	group              string
	dispatcher         *dispatcher.T
	kafkaClt           sarama.Client
	kazooClt           *kazoo.Kazoo
	msgFetcherF        msgfetcher.Factory
	offsetMgrF         offsetmgr.Factory
	subscriber         *subscriber.T
	multiplexers       map[string]*multiplexer.T
	topicCsmLifespanCh chan *topiccsm.T
	wg                 sync.WaitGroup

	// Exist just to be overridden in tests with mocks.
	fetchTopicPartitionsFn func(topic string) ([]int32, error)
}

func Spawn(parentActDesc *actor.Descriptor, childSpec dispatcher.ChildSpec,
	cfg *config.Proxy, kafkaClt sarama.Client, kazooClt *kazoo.Kazoo,
	offsetMgrF offsetmgr.Factory,
) *T {
	group := string(childSpec.Key())
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s", group))
	actDesc.AddLogField("kafka.group", group)
	gc := &T{
		actDesc:                actDesc,
		cfg:                    cfg,
		group:                  group,
		kafkaClt:               kafkaClt,
		kazooClt:               kazooClt,
		offsetMgrF:             offsetMgrF,
		multiplexers:           make(map[string]*multiplexer.T),
		topicCsmLifespanCh:     make(chan *topiccsm.T),
		fetchTopicPartitionsFn: kafkaClt.Partitions,
	}

	gc.subscriber = subscriber.Spawn(gc.actDesc, gc.group, gc.cfg, gc.kazooClt)
	gc.msgFetcherF = msgfetcher.SpawnFactory(gc.actDesc, gc.cfg, gc.kafkaClt)
	actor.Spawn(gc.actDesc, &gc.wg, gc.run)

	// Finalizer is called when all downstream topic consumers expire or if
	// the dispatcher is explicitly told to stop by the upstream dispatcher.
	finalizer := func() {
		gc.subscriber.Stop()
		// The run goroutine stops when the subscriber's channel is closed.
		gc.wg.Wait()
		// Only after run is stopped it is safe to shutdown the fetcher factory.
		gc.msgFetcherF.Stop()
	}
	gc.dispatcher = dispatcher.Spawn(gc.actDesc, gc, cfg,
		dispatcher.WithChildSpec(childSpec), dispatcher.WithFinalizer(finalizer))
	return gc
}

// implements `dispatcher.Factory`.
func (gc *T) KeyOf(req dispatcher.Request) dispatcher.Key {
	return dispatcher.Key(req.Topic)
}

// implements `dispatcher.Factory`.
func (gc *T) SpawnChild(childSpec dispatcher.ChildSpec) {
	topiccsm.Spawn(gc.actDesc, gc.group, childSpec, gc.cfg, gc.topicCsmLifespanCh)
}

// String return string ID of this group consumer to be posted in logs.
func (gc *T) String() string {
	return gc.actDesc.String()
}

func (gc *T) run() {
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
			nilOrRegistryTopicsCh = gc.subscriber.Topics()
			continue
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue
		case subscriptions, ok = <-gc.subscriber.Subscriptions():
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
				gc.actDesc.Log().WithError(err).Error("rebalancing failed")
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
			rebalanceActDesc := gc.actDesc.NewChild("rebalance")
			// Copy topicConsumers to make sure `rebalance` doesn't see any
			// changes we make while it is running.
			topicConsumersCopy := make(map[string]*topiccsm.T, len(topicConsumers))
			for topic, tc := range topicConsumers {
				topicConsumersCopy[topic] = tc
			}
			subscriptions := subscriptions
			actor.Spawn(rebalanceActDesc, nil, func() {
				gc.runRebalancing(rebalanceActDesc, topicConsumersCopy, subscriptions, rebalanceResultCh)
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

func (gc *T) runRebalancing(actDesc *actor.Descriptor, topicConsumers map[string]*topiccsm.T,
	subscriptions map[string][]string, rebalanceResultCh chan<- error,
) {
	assignedPartitions, err := gc.resolvePartitions(subscriptions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	actDesc.Log().Infof("assigned partitions: %v", assignedPartitions)
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
			return partitioncsm.Spawn(gc.actDesc, gc.group, topic, partition,
				gc.cfg, gc.subscriber, gc.msgFetcherF, gc.offsetMgrF)
		}
		mux = multiplexer.New(gc.actDesc, spawnInFn)
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
	actor.Spawn(gc.actDesc.NewChild("rewire", topic), wg, func() {
		if tc == nil {
			// Parameter output is an interface type, therefore nil should be
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
	sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
	sort.Strings(subscribers)

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
