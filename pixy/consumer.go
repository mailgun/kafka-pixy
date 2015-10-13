package pixy

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/wvanbergen/kazoo-go"
)

type (
	ErrConsumerSetup          error
	ErrConsumerBufferOverflow error
	ErrConsumerRequestTimeout error
)

// SmartConsumer is a Kafka consumer implementation that automatically
// maintains consumer groups registrations and topic subscriptions. Whenever a
// a message from a particular topic is consumed by a particular consumer group
// SmartConsumer checks if it has registered with the consumer group, and
// registers otherwise. Then it checks if it has subscribed for the topic, and
// subscribes otherwise. Later if a particular topic has not been consumed for
// the `Config.Consumer.RegistrationTimeout` period of time, then the consumer
// unsubscribes from the topic, likewise if a consumer group has not seen any
// requests for that period then the consumer deregisters from the group.
type SmartConsumer struct {
	baseCID     *sarama.ContextID
	config      *Config
	dispatcher  *dispatcher
	kafkaClient sarama.Client
	offsetMgr   sarama.OffsetManager
	kazooConn   *kazoo.Kazoo
}

// SpawnSmartConsumer creates a SmartConsumer instance with the specified
// configuration and starts all its goroutines.
func SpawnSmartConsumer(config *Config) (*SmartConsumer, error) {
	kafkaClient, err := sarama.NewClient(config.Kafka.SeedPeers, config.saramaConfig())
	if err != nil {
		return nil, ErrConsumerSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	offsetMgr, err := sarama.NewOffsetManagerFromClient(kafkaClient)
	if err != nil {
		return nil, ErrConsumerSetup(fmt.Errorf("failed to create sarama.OffsetManager: err=(%v)", err))
	}
	kazooConn, err := kazoo.NewKazoo(config.ZooKeeper.SeedPeers, config.kazooConfig())
	if err != nil {
		return nil, ErrConsumerSetup(fmt.Errorf("failed to create kazoo.Kazoo: err=(%v)", err))
	}
	sc := &SmartConsumer{
		baseCID:     sarama.RootCID.NewChild("smartConsumer"),
		config:      config,
		kafkaClient: kafkaClient,
		offsetMgr:   offsetMgr,
		kazooConn:   kazooConn,
	}
	sc.dispatcher = newDispatcher(sc.baseCID, sc, sc.config)
	sc.dispatcher.start()
	return sc, nil
}

// Stop sends the shutdown signal to all internal goroutines and blocks until
// all of them are stopped. It is guaranteed that all last consumed offsets of
// all consumer groups/topics are committed to Kafka before SmartConsumer stops.
func (sc *SmartConsumer) Stop() {
	sc.dispatcher.stop()
}

// Consume consumes a message from the specified topic on behalf of the
// specified consumer group. If there are no more new messages in the topic
// at the time of the request then it will block for
// `Config.Consumer.LongPollingTimeout`. If no new message is produced during
// that time, then `ErrConsumerRequestTimeout` is returned.
//
// Note that during state transitions topic subscribe<->unsubscribe and
// consumer group register<->deregister the method may return either
// `ErrConsumerBufferOverflow` or `ErrConsumerRequestTimeout` even when there
// are messages available for consumption. In that case the user should back
// off a bit and then repeat the request.
func (sc *SmartConsumer) Consume(group, topic string) (*sarama.ConsumerMessage, error) {
	replyCh := make(chan consumeResult, 1)
	sc.dispatcher.requests() <- consumeRequest{time.Now().UTC(), group, topic, replyCh}
	result := <-replyCh
	return result.Msg, result.Err
}

type consumeRequest struct {
	timestamp time.Time
	group     string
	topic     string
	replyCh   chan<- consumeResult
}

type consumeResult struct {
	Msg *sarama.ConsumerMessage
	Err error
}

func (sc *SmartConsumer) String() string {
	return sc.baseCID.String()
}

func (sc *SmartConsumer) dispatchKey(req consumeRequest) string {
	return req.group
}

func (sc *SmartConsumer) newDispatchTier(key string) dispatchTier {
	return sc.newConsumerGroup(key)
}

// groupConsumer manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type groupConsumer struct {
	baseCID               *sarama.ContextID
	config                *Config
	group                 string
	sc                    *SmartConsumer
	dispatcher            *dispatcher
	kafkaClient           sarama.Client
	dumbConsumer          sarama.Consumer
	offsetMgr             sarama.OffsetManager
	kazooConn             *kazoo.Kazoo
	registry              *consumerGroupRegistry
	exclusiveConsumers    map[string]map[int32]*exclusiveConsumer
	addTopicConsumerCh    chan *topicConsumer
	deleteTopicConsumerCh chan *topicConsumer
	stoppingCh            chan none
	wg                    sync.WaitGroup
}

func (sc *SmartConsumer) newConsumerGroup(group string) *groupConsumer {
	gc := &groupConsumer{
		baseCID:               sc.baseCID.NewChild(group),
		config:                sc.config,
		group:                 group,
		sc:                    sc,
		kafkaClient:           sc.kafkaClient,
		offsetMgr:             sc.offsetMgr,
		kazooConn:             sc.kazooConn,
		exclusiveConsumers:    make(map[string]map[int32]*exclusiveConsumer),
		addTopicConsumerCh:    make(chan *topicConsumer),
		deleteTopicConsumerCh: make(chan *topicConsumer),
		stoppingCh:            make(chan none),
	}
	gc.dispatcher = newDispatcher(gc.baseCID, gc, sc.config)
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
			panic(ErrConsumerSetup(fmt.Errorf("failed to create sarama.Consumer: err=(%v)", err)))
		}
		gc.registry = spawnConsumerGroupRegister(gc.group, gc.config.ClientID, gc.config, gc.kazooConn)
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
				nilOrRetryCh = time.After(gc.config.Consumer.BackOffTimeout)
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
	for _, topicExclusiveConsumers := range gc.exclusiveConsumers {
		for _, ec := range topicExclusiveConsumers {
			spawn(&wg, ec.stop)
		}
	}
	wg.Wait()
}

func (gc *groupConsumer) rebalance(topicConsumers map[string]*topicConsumer,
	memberSubscriptions map[string][]string, rebalanceResultCh chan<- error) {

	cid := gc.baseCID.NewChild("rebalance")
	defer cid.LogScope(topicConsumers, memberSubscriptions)()

	assignedPartitions, err := gc.resolvePartitions(memberSubscriptions)
	if err != nil {
		rebalanceResultCh <- err
		return
	}
	// Stop partition consumers that are no longer assigned to this group member.
	var wg sync.WaitGroup
	for topic, topicExclusiveConsumers := range gc.exclusiveConsumers {
		assignedTopicPartitions := assignedPartitions[topic]
		for partition, ec := range topicExclusiveConsumers {
			if !assignedTopicPartitions[partition] {
				delete(topicExclusiveConsumers, partition)
				spawn(&wg, ec.stop)
			}
		}
	}
	wg.Wait()
	// Spawn consumers for just assigned partitions.
	for topic, assignedTopicPartitions := range assignedPartitions {
		log.Infof("<%s> assigned: topic=%s, partitions=%v", cid, topic, assignedTopicPartitions)
		tc := topicConsumers[topic]
		if tc == nil {
			continue
		}
		topicExclusiveConsumers := gc.exclusiveConsumers[topic]
		if topicExclusiveConsumers == nil {
			topicExclusiveConsumers = make(map[int32]*exclusiveConsumer, len(assignedTopicPartitions))
			gc.exclusiveConsumers[topic] = topicExclusiveConsumers
		}
		for partition := range assignedTopicPartitions {
			if _, ok := topicExclusiveConsumers[partition]; !ok {
				ec := gc.spawnExclusiveConsumer(tc, partition)
				topicExclusiveConsumers[partition] = ec
			}
		}
	}
	// Notify the caller that rebalancing has completed successfully.
	rebalanceResultCh <- nil
	return
}

// resolvePartitions takes a `subscriber->topics` map and returns a
// `topic->partitions` map that for every consumed topic tells what partitions
// this consumer group instance is responsible for.
func (gc *groupConsumer) resolvePartitions(subscribersToTopics map[string][]string) (
	assignedPartitions map[string]map[int32]bool, err error) {

	// Convert subscribers->topics to topic->subscribers map.
	topicsToSubscribers := make(map[string][]string)
	for subscriberID, topics := range subscribersToTopics {
		for _, topic := range topics {
			topicsToSubscribers[topic] = append(topicsToSubscribers[topic], subscriberID)
		}
	}
	// Create a set of topics this consumer group member subscribed to.
	subscribedTopics := make(map[string]bool)
	for _, topic := range subscribersToTopics[gc.config.ClientID] {
		subscribedTopics[topic] = true
	}
	// Resolve new partition assignments for the subscribed topics.
	assignedPartitions = make(map[string]map[int32]bool)
	for topic := range subscribedTopics {
		topicPartitions, err := gc.kafkaClient.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to get partition list: topic=%s, err=(%s)", topic, err)
		}
		partitionsToSubscribers := assignPartitionsToSubscribers(topicPartitions, topicsToSubscribers[topic])
		assignedTopicPartitions := partitionsToSubscribers[gc.config.ClientID]
		assignedPartitions[topic] = assignedTopicPartitions
	}
	return assignedPartitions, nil
}

// assignPartitionsToSubscribers does what the name says. The algorithm used
// closely resembles the algorithm implemented by the standard Java High-Level
// consumer (see http://kafka.apache.org/documentation.html#distributionimpl
// and scroll down to *Consumer registration algorithm*) except it does not take
// in account how partitions are distributed among brokers.
func assignPartitionsToSubscribers(partitions []int32, subscribers []string) map[string]map[int32]bool {
	partitionCount := len(partitions)
	subscriberCount := len(subscribers)
	if partitionCount == 0 || subscriberCount == 0 {
		return nil
	}
	sort.Sort(Int32Slice(partitions))
	sort.Sort(sort.StringSlice(subscribers))

	partitionsToSubscribers := make(map[string]map[int32]bool, subscriberCount)
	partitionsPerSubscriber := partitionCount / subscriberCount
	extra := partitionCount - subscriberCount*partitionsPerSubscriber

	begin := 0
	for _, subscriberID := range subscribers {
		end := begin + partitionsPerSubscriber
		if extra != 0 {
			end++
			extra--
		}
		for _, partition := range partitions[begin:end] {
			topicAssignments := partitionsToSubscribers[subscriberID]
			if topicAssignments == nil {
				topicAssignments = make(map[int32]bool, partitionCount)
				partitionsToSubscribers[subscriberID] = topicAssignments
			}
			partitionsToSubscribers[subscriberID][partition] = true
		}
		begin = end
	}
	return partitionsToSubscribers
}

func listTopics(topicConsumers map[string]*topicConsumer) []string {
	topics := make([]string, 0, len(topicConsumers))
	for topic := range topicConsumers {
		topics = append(topics, topic)
	}
	return topics
}

// topicConsumer implements a consumer request dispatch tier responsible for
// consumption of a particular topic by a consumer group. It receives requests
// on the `requests()` channel and replies with messages supplied by exclusive
// partition consumers via `messages()` channel. If there has been no message
// received for `Config.Consumer.LongPollingTimeout` then a timeout error is
// sent to the requests' reply channel.
type topicConsumer struct {
	contextID     *sarama.ContextID
	config        *Config
	gc            *groupConsumer
	group         string
	topic         string
	assignmentsCh chan []int32
	requestsCh    chan consumeRequest
	messagesCh    chan *sarama.ConsumerMessage
	wg            sync.WaitGroup
}

func (gc *groupConsumer) newTopicConsumer(topic string) *topicConsumer {
	return &topicConsumer{
		contextID:     gc.baseCID.NewChild(topic),
		config:        gc.config,
		gc:            gc,
		group:         gc.group,
		topic:         topic,
		assignmentsCh: make(chan []int32),
		requestsCh:    make(chan consumeRequest, gc.config.ChannelBufferSize),
		messagesCh:    make(chan *sarama.ConsumerMessage),
	}
}

func (tc *topicConsumer) messages() chan<- *sarama.ConsumerMessage {
	return tc.messagesCh
}

func (tc *topicConsumer) key() string {
	return tc.topic
}

func (tc *topicConsumer) requests() chan<- consumeRequest {
	return tc.requestsCh
}

func (tc *topicConsumer) start(stoppedCh chan<- dispatchTier) {
	spawn(&tc.wg, func() {
		defer func() { stoppedCh <- tc }()
		tc.run()
	})
}

func (tc *topicConsumer) stop() {
	close(tc.requestsCh)
	tc.wg.Wait()
}

func (tc *topicConsumer) run() {
	defer tc.contextID.LogScope()()
	tc.gc.addTopicConsumer() <- tc
	defer func() {
		tc.gc.deleteTopicConsumer() <- tc
	}()

	timeoutErr := ErrConsumerRequestTimeout(fmt.Errorf("long polling timeout"))
	timeoutResult := consumeResult{Err: timeoutErr}
	for consumeReq := range tc.requestsCh {
		requestAge := time.Now().UTC().Sub(consumeReq.timestamp)
		ttl := tc.config.Consumer.LongPollingTimeout - requestAge
		// The request has been waiting in the buffer for too long. If we
		// reply with a fetched message, then there is a good chance that the
		// client won't receive it due to the client HTTP timeout. Therefore
		// we reject the request to avoid message loss.
		if ttl <= 0 {
			consumeReq.replyCh <- timeoutResult
			continue
		}

		select {
		case msg := <-tc.messagesCh:
			consumeReq.replyCh <- consumeResult{Msg: msg}
		case <-time.After(ttl):
			consumeReq.replyCh <- timeoutResult
		}
	}
}

func (tc *topicConsumer) String() string {
	return tc.contextID.String()
}

// exclusiveConsumer ensures exclusive consumption of message from a particular
// `consumer group+topic+partition`. It achieves that by claiming the partition
// via the consumer group registry and only after the claim is succeeded starts
// consuming messages.
type exclusiveConsumer struct {
	contextID    *sarama.ContextID
	config       *Config
	tc           *topicConsumer
	group        string
	topic        string
	partition    int32
	dumbConsumer sarama.Consumer
	registry     *consumerGroupRegistry
	offsetMgr    sarama.OffsetManager
	stoppingCh   chan none
	wg           sync.WaitGroup
}

func (gc *groupConsumer) spawnExclusiveConsumer(tc *topicConsumer, partition int32) *exclusiveConsumer {
	ec := &exclusiveConsumer{
		contextID:    gc.baseCID.NewChild(fmt.Sprintf("%s:%d", tc.topic, partition)),
		config:       gc.config,
		tc:           tc,
		group:        gc.group,
		topic:        tc.topic,
		partition:    partition,
		dumbConsumer: gc.dumbConsumer,
		registry:     gc.registry,
		offsetMgr:    gc.offsetMgr,
		stoppingCh:   make(chan none),
	}
	spawn(&ec.wg, ec.run)
	return ec
}

func (ec *exclusiveConsumer) run() {
	defer ec.contextID.LogScope()()
	defer ec.registry.claimPartition(ec.contextID, ec.topic, ec.partition, ec.stoppingCh)()

	pom, err := ec.offsetMgr.ManagePartition(ec.group, ec.topic, ec.partition)
	if err != nil {
		log.Errorf("<%s> failed to spawn partition manager: err=(%s)", ec.contextID, err)
		return
	}
	defer pom.Close()

	// Wait for the initial offset to be retrieved.
	var initialOffset sarama.DecoratedOffset
	select {
	case initialOffset = <-pom.InitialOffset():
	case <-ec.stoppingCh:
		return
	}

	pc, err := ec.dumbConsumer.ConsumePartition(ec.topic, ec.partition, initialOffset.Offset)
	if err != nil {
		log.Errorf("<%s> failed to start partition consumer: err=(%s)", ec.contextID, err)
		return
	}
	defer pc.Close()

	log.Infof("<%s> initialized: offset=%d", ec.contextID, initialOffset.Offset)
	firstMessageFetched := false
	var lastSubmittedOffset, lastCommittedOffset int64
	for {
		select {
		case consumerMessage := <-pc.Messages():
			// Notify tests when the very first message is fetched.
			if !firstMessageFetched && ec.config.testing.firstMessageFetchedCh != nil {
				firstMessageFetched = true
				ec.config.testing.firstMessageFetchedCh <- ec
			}
			select {
			case ec.tc.messages() <- consumerMessage:
				lastSubmittedOffset = consumerMessage.Offset + 1
				pom.SubmitOffset(lastSubmittedOffset, "")
			case <-ec.stoppingCh:
				goto done
			}
		case committedOffset := <-pom.CommittedOffsets():
			lastCommittedOffset = committedOffset.Offset
		case <-ec.stoppingCh:
			goto done
		}
	}
done:
	if lastCommittedOffset == lastSubmittedOffset {
		return
	}
	// It is necessary to wait for the offset of the last consumed message to
	// be committed to Kafka before releasing ownership over the partition,
	// otherwise the message can be consumed by the new partition owner again.
	log.Infof("<%s> waiting for the last offset to be committed: submitted=%d, committed=%d",
		ec.contextID, lastSubmittedOffset, lastCommittedOffset)
	for committedOffset := range pom.CommittedOffsets() {
		if committedOffset.Offset == lastSubmittedOffset {
			return
		}
		log.Infof("<%s> waiting for the last offset to be committed: submitted=%d, committed=%d",
			ec.contextID, lastSubmittedOffset, committedOffset.Offset)
	}
}

func (ec *exclusiveConsumer) stop() {
	close(ec.stoppingCh)
	ec.wg.Wait()
}
