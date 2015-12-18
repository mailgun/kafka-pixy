package consumer

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/wvanbergen/kazoo-go"
	"github.com/mailgun/kafka-pixy/config"
)

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type groupConsumer struct {
	baseCID               *sarama.ContextID
	cfg                   *config.T
	group                 string
	dispatcher            *dispatcher
	kafkaClient           sarama.Client
	dumbConsumer          sarama.Consumer
	offsetMgr             sarama.OffsetManager
	kazooConn             *kazoo.Kazoo
	registry              *groupRegistrator
	topicGears            map[string]*topicGear
	addTopicConsumerCh    chan *topicConsumer
	deleteTopicConsumerCh chan *topicConsumer
	stoppingCh            chan none
	wg                    sync.WaitGroup
}

func (sc *T) newConsumerGroup(group string) *groupConsumer {
	gc := &groupConsumer{
		baseCID:               sc.baseCID.NewChild(group),
		cfg:                   sc.cfg,
		group:                 group,
		kafkaClient:           sc.kafkaClient,
		offsetMgr:             sc.offsetMgr,
		kazooConn:             sc.kazooConn,
		topicGears:            make(map[string]*topicGear),
		addTopicConsumerCh:    make(chan *topicConsumer),
		deleteTopicConsumerCh: make(chan *topicConsumer),
		stoppingCh:            make(chan none),
	}
	gc.dispatcher = newDispatcher(gc.baseCID, gc, sc.cfg)
	return gc
}

func (gc *groupConsumer) String() string {
	return gc.baseCID.String()
}

func (gc *groupConsumer) addTopicConsumer() chan<- *topicConsumer {
	return gc.addTopicConsumerCh
}

func (gc *groupConsumer) deleteTopicConsumer() chan<- *topicConsumer {
	return gc.deleteTopicConsumerCh
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
		memberSubscriptions           map[string][]string
		ok                            = true
		nilOrRetryCh                  <-chan time.Time
		nilOrRegistryTopicsCh         chan<- []string
		shouldRebalance, canRebalance = false, true
		rebalanceResultCh             = make(chan error, 1)
	)
	for {
		select {
		case tc := <-gc.addTopicConsumerCh:
			topicConsumers[tc.topic] = tc
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = gc.registry.topics()
			continue
		case tc := <-gc.deleteTopicConsumerCh:
			delete(topicConsumers, tc.topic)
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = gc.registry.topics()
			continue
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue
		case memberSubscriptions, ok = <-gc.registry.membershipChanges():
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
			go gc.rebalance(topicConsumerCopy, memberSubscriptions, rebalanceResultCh)
			shouldRebalance, canRebalance = false, false
		}
	}
done:
	var wg sync.WaitGroup
	for _, tg := range gc.topicGears {
		tg := tg
		spawn(&wg, func() { gc.rewireMultiplexer(tg, nil) })
	}
	wg.Wait()
}

func (gc *groupConsumer) rebalance(topicConsumers map[string]*topicConsumer,
	memberSubscriptions map[string][]string, rebalanceResultCh chan<- error,
) {
	cid := gc.baseCID.NewChild("rebalance")
	defer cid.LogScope(topicConsumers, memberSubscriptions)()

	assignedPartitions, err := gc.resolvePartitions(memberSubscriptions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	log.Infof("<%s> assigned: %v", cid, assignedPartitions)
	var wg sync.WaitGroup
	// Stop consuming partitions that are no longer assigned to this group
	// and start consuming newly assigned partitions for topics that has been
	// consumed already.
	for topic, tg := range gc.topicGears {
		tg := tg
		assignedTopicPartition := assignedPartitions[topic]
		spawn(&wg, func() { gc.rewireMultiplexer(tg, assignedTopicPartition) })
	}
	// Start consuming partitions for topics that has not been consumed before.
	for topic, assignedTopicPartitions := range assignedPartitions {
		tc := topicConsumers[topic]
		tg := gc.topicGears[topic]
		if tc == nil || tg != nil {
			continue
		}
		tg = &topicGear{
			topicConsumer:      tc,
			exclusiveConsumers: make(map[int32]*exclusiveConsumer, len(assignedTopicPartitions)),
		}
		assignedTopicPartitions := assignedTopicPartitions
		spawn(&wg, func() { gc.rewireMultiplexer(tg, assignedTopicPartitions) })
		gc.topicGears[topic] = tg
	}
	wg.Wait()
	// Clean up gears for topics that are not consumed anymore.
	for topic, tg := range gc.topicGears {
		if tg.multiplexer == nil {
			delete(gc.topicGears, topic)
		}
	}
	// Notify the caller that rebalancing has completed successfully.
	rebalanceResultCh <- nil
	return
}

// rewireMultiplexer ensures that only assigned partitions are multiplexed to
// the topic consumer. It stops exclusive consumers for partitions that are not
// assigned anymore, spins up exclusive consumers for newly assigned partitions,
// and restarts the multiplexer to account for the changes if there is any.
func (gc *groupConsumer) rewireMultiplexer(tg *topicGear, assigned map[int32]bool) {
	var wg sync.WaitGroup
	for partition, ec := range tg.exclusiveConsumers {
		if !assigned[partition] {
			if tg.multiplexer != nil {
				tg.multiplexer.stop()
				tg.multiplexer = nil
			}
			spawn(&wg, ec.stop)
			delete(tg.exclusiveConsumers, partition)
		}
	}
	wg.Wait()
	for partition := range assigned {
		if _, ok := tg.exclusiveConsumers[partition]; !ok {
			if tg.multiplexer != nil {
				tg.multiplexer.stop()
				tg.multiplexer = nil
			}
			ec := gc.spawnExclusiveConsumer(tg.topicConsumer.topic, partition)
			tg.exclusiveConsumers[partition] = ec
		}
	}
	if tg.multiplexer == nil && len(tg.exclusiveConsumers) > 0 {
		muxIns := make([]multiplexerIn, 0, len(tg.exclusiveConsumers))
		for _, ec := range tg.exclusiveConsumers {
			muxIns = append(muxIns, ec)
		}
		tg.multiplexer = spawnMultiplexer(tg.topicConsumer.contextID, tg.topicConsumer, muxIns)
	}
}

// resolvePartitions takes a `consumer group members->topics` map and returns a
// `topic->partitions` map that for every consumed topic tells what partitions
// this consumer group instance is responsible for.
func (gc *groupConsumer) resolvePartitions(membersToTopics map[string][]string) (
	assignedPartitions map[string]map[int32]bool, err error,
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
	// Resolve new partition assignments for the subscribed topics.
	assignedPartitions = make(map[string]map[int32]bool)
	for topic := range subscribedTopics {
		topicPartitions, err := gc.kafkaClient.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get partition list: topic=%s, err=(%s)", topic, err)
		}
		subscribersToPartitions := assignPartitionsToSubscribers(topicPartitions, topicsToMembers[topic])
		assignedTopicPartitions := subscribersToPartitions[gc.cfg.ClientID]
		assignedPartitions[topic] = assignedTopicPartitions
	}
	return assignedPartitions, nil
}

// assignPartitionsToSubscribers divides topic partitions among all consumer
// group members subscribed to the topic. The algorithm used closely resembles
// the one implemented by the standard Java High-Level consumer
// (see http://kafka.apache.org/documentation.html#distributionimpl and scroll
// down to *Consumer registration algorithm*) except it does not take in account
// how partitions are distributed among brokers.
func assignPartitionsToSubscribers(partitions []int32, subscribers []string) map[string]map[int32]bool {
	partitionCount := len(partitions)
	subscriberCount := len(subscribers)
	if partitionCount == 0 || subscriberCount == 0 {
		return nil
	}
	sort.Sort(Int32Slice(partitions))
	sort.Sort(sort.StringSlice(subscribers))

	subscribersToPartitions := make(map[string]map[int32]bool, subscriberCount)
	partitionsPerSubscriber := partitionCount / subscriberCount
	extra := partitionCount - subscriberCount*partitionsPerSubscriber

	begin := 0
	for _, groupMemberID := range subscribers {
		end := begin + partitionsPerSubscriber
		if extra != 0 {
			end++
			extra--
		}
		for _, partition := range partitions[begin:end] {
			topicAssignments := subscribersToPartitions[groupMemberID]
			if topicAssignments == nil {
				topicAssignments = make(map[int32]bool, partitionCount)
				subscribersToPartitions[groupMemberID] = topicAssignments
			}
			subscribersToPartitions[groupMemberID][partition] = true
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

// topicGear represents a set of actors that a consumer group maintains for
// each consumed topic.
type topicGear struct {
	topicConsumer      *topicConsumer
	multiplexer        *multiplexer
	exclusiveConsumers map[int32]*exclusiveConsumer
}

type Int32Slice []int32

func (p Int32Slice) Len() int           { return len(p) }
func (p Int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p Int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
