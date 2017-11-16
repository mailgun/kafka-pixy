package groupcsm

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
	"github.com/mailgun/kafka-pixy/consumer/msgfetcher"
	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
	"github.com/mailgun/kafka-pixy/consumer/partitioncsm"
	"github.com/mailgun/kafka-pixy/consumer/subscriber"
	"github.com/mailgun/kafka-pixy/consumer/topiccsm"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/mailgun/kazoo-go"
	"github.com/pkg/errors"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
//
// implements `dispatcher.Factory`.
// implements `dispatcher.Tier`.
type T struct {
	actDesc     *actor.Descriptor
	cfg         *config.Proxy
	group       string
	dispatcher  *dispatcher.T
	kafkaClt    sarama.Client
	kazooClt    *kazoo.Kazoo
	msgFetcherF msgfetcher.Factory
	offsetMgrF  offsetmgr.Factory
	subscriber  *subscriber.T
	topicCsmCh  chan *topiccsm.T
	wg          sync.WaitGroup

	multiplexersMu sync.Mutex
	multiplexers   map[string]*multiplexer.T
}

func Spawn(parentActDesc *actor.Descriptor, childSpec dispatcher.ChildSpec,
	cfg *config.Proxy, kafkaClt sarama.Client, kazooClt *kazoo.Kazoo,
	offsetMgrF offsetmgr.Factory,
) *T {
	group := string(childSpec.Key())
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s", group))
	actDesc.AddLogField("kafka.group", group)
	gc := &T{
		actDesc:      actDesc,
		cfg:          cfg,
		group:        group,
		kafkaClt:     kafkaClt,
		kazooClt:     kazooClt,
		offsetMgrF:   offsetMgrF,
		multiplexers: make(map[string]*multiplexer.T),
		topicCsmCh:   make(chan *topiccsm.T, cfg.Consumer.ChannelBufferSize),
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
func (gc *T) KeyOf(req consumer.Request) dispatcher.Key {
	return dispatcher.Key(req.Topic)
}

// implements `dispatcher.Factory`.
func (gc *T) SpawnChild(childSpec dispatcher.ChildSpec) {
	topic := string(childSpec.Key())
	topiccsm.Spawn(gc.actDesc, gc.group, childSpec, gc.cfg, gc.topicCsmCh,
		func() bool { return gc.isSafe2Stop(topic) })
}

// String return string ID of this group consumer to be posted in logs.
func (gc *T) String() string {
	return gc.actDesc.String()
}

func (gc *T) isSafe2Stop(topic string) bool {
	gc.multiplexersMu.Lock()
	mux := gc.multiplexers[topic]
	gc.multiplexersMu.Unlock()
	if mux == nil {
		return true
	}
	return mux.IsSafe2Stop()
}

func (gc *T) run() {
	var (
		topicConsumers          = make(map[string]*topiccsm.T)
		topics                  []string
		subscriptions           map[string][]string
		ok                      = true
		nilOrRetryCh            <-chan time.Time
		nilOrSubscriberTopicsCh chan<- []string
		rebalanceRequired       = false
		rebalancePending        = false
		rebalanceScheduled      = false
		stopped                 = false
		rebalanceResultCh       = make(chan error, 1)
	)
	for {
		select {
		case tc := <-gc.topicCsmCh:
			// It is assumed that only one topicConsumer can exist for a
			// particular topic at a time.
			if topicConsumers[tc.Topic()] == tc {
				delete(topicConsumers, tc.Topic())
			} else {
				topicConsumers[tc.Topic()] = tc
			}
			topics = listTopics(topicConsumers)
			gc.actDesc.Log().Infof("Topics updated: %s", topics)
			nilOrSubscriberTopicsCh = gc.subscriber.Topics()
			continue

		case nilOrSubscriberTopicsCh <- topics:
			nilOrSubscriberTopicsCh = nil
			continue

		case subscriptions, ok = <-gc.subscriber.Subscriptions():
			nilOrRetryCh = nil
			if !ok {
				if !rebalancePending {
					goto done
				}
				stopped = true
				continue
			}
			rebalanceRequired = true

		case err := <-rebalanceResultCh:
			rebalancePending = false
			if err != nil {
				gc.actDesc.Log().WithError(err).Error("rebalancing failed")
				if stopped {
					goto done
				}
				nilOrRetryCh = time.After(gc.cfg.Consumer.RetryBackoff)
				rebalanceScheduled = true
			}
			if stopped {
				goto done
			}
		case <-nilOrRetryCh:
			rebalanceScheduled = false
		}

		if rebalanceRequired && !rebalancePending && !rebalanceScheduled {
			rebalanceActDesc := gc.actDesc.NewChild("rebalance")
			// Copy topicConsumers to make sure `rebalance` doesn't see any
			// changes we make while it is running.
			topicConsumersCopy := make(map[string]*topiccsm.T, len(topicConsumers))
			for topic, tc := range topicConsumers {
				topicConsumersCopy[topic] = tc
			}
			subscriptions := subscriptions
			actor.Spawn(rebalanceActDesc, nil, func() {
				gc.rebalance(rebalanceActDesc, topicConsumersCopy, subscriptions, rebalanceResultCh)
			})
			rebalancePending = true
			rebalanceRequired = false
		}
	}
done:
	var wg sync.WaitGroup
	gc.multiplexersMu.Lock()
	for _, mux := range gc.multiplexers {
		wg.Add(1)
		go func(mux *multiplexer.T) {
			defer wg.Done()
			mux.Stop()
		}(mux)
	}
	gc.multiplexersMu.Unlock()
	wg.Wait()
}

func (gc *T) rebalance(actDesc *actor.Descriptor, topicConsumers map[string]*topiccsm.T,
	subscriptions map[string][]string, rebalanceResultCh chan<- error,
) {
	assignedPartitions, err := gc.resolvePartitions(subscriptions, gc.kafkaClt.Partitions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	actDesc.Log().Infof("assigned partitions: %s", prettyfmt.Val(assignedPartitions))
	var wg sync.WaitGroup
	// Stop consuming partitions that are no longer assigned to this group
	// and start consuming newly assigned partitions for topics that has been
	// consumed already.
	gc.multiplexersMu.Lock()
	defer gc.multiplexersMu.Unlock()
	for topic, mux := range gc.multiplexers {
		gc.rewireMuxAsync(topic, &wg, mux, topicConsumers[topic], assignedPartitions[topic])
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
func (gc *T) resolvePartitions(subscriptions map[string][]string,
	topicPartitionsFn func(string) ([]int32, error)) (map[string][]int32, error,
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
		topicPartitions, err := topicPartitionsFn(topic)
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
