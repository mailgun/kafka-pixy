package pixy

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/kazoo-go"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

type ErrConsumerSetup error
type ErrConsumerBufferOverflow error
type ErrConsumerRequestTimeout error

// SmartConsumer is a Kafka consumer implementation that automatically
// maintains consumer groups registrations and topic subscriptions. Whenever a
// a message from a particular topic is consumed by a particular consumer group
// SmartConsumer checks if it has registered with the consumer group, and
// registers otherwise. Then it checks if it has subscribed for the topic, and
// subscribes otherwise. Later if a particular topic has not been consumed for
// the `Config.Consumer.RegistrationTimeout` period of time, then the consumer
// unsubscribes from the topic, likewise if a consumer group has not seen any
// requests for that period then the consumer deregisters from the group.
//
// Note that during state transitions subscribe<->unsubscribe and
// register<->deregister the consumer may return either `ErrConsumerRequestTimeout`
// or `ErrConsumerBufferOverflow` error. In that case the user should back off
// a bit and then repeat the request.
type SmartConsumer struct {
	baseCID      *sarama.ContextID
	config       *Config
	dispatcher   *dispatcher
	kafkaClient  sarama.Client
	dumbConsumer sarama.Consumer
	offsetMgr    sarama.OffsetManager
	kazooConn    *kazoo.Kazoo
}

func SpawnSmartConsumer(config *Config) (*SmartConsumer, error) {
	kafkaClient, err := sarama.NewClient(config.Kafka.SeedPeers, config.saramaConfig())
	if err != nil {
		return nil, ErrConsumerSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	dumbConsumer, err := sarama.NewConsumer(config.Kafka.SeedPeers, config.saramaConfig())
	if err != nil {
		return nil, ErrConsumerSetup(fmt.Errorf("failed to create sarama.Concumer: err=(%v)", err))
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
		baseCID:      sarama.RootCID.NewChild("smartConsumer"),
		config:       config,
		kafkaClient:  kafkaClient,
		dumbConsumer: dumbConsumer,
		offsetMgr:    offsetMgr,
		kazooConn:    kazooConn,
	}
	sc.dispatcher = newDispatcher(sc.baseCID, sc, sc.config)
	sc.dispatcher.start()
	return sc, nil
}

func (sc *SmartConsumer) Stop() {
	sc.dispatcher.stop()
}

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

// consumerGroup manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type consumerGroup struct {
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

func (sc *SmartConsumer) newConsumerGroup(group string) *consumerGroup {
	cg := &consumerGroup{
		baseCID:               sc.baseCID.NewChild(group),
		config:                sc.config,
		group:                 group,
		sc:                    sc,
		kafkaClient:           sc.kafkaClient,
		dumbConsumer:          sc.dumbConsumer,
		offsetMgr:             sc.offsetMgr,
		kazooConn:             sc.kazooConn,
		exclusiveConsumers:    make(map[string]map[int32]*exclusiveConsumer),
		addTopicConsumerCh:    make(chan *topicConsumer),
		deleteTopicConsumerCh: make(chan *topicConsumer),
		stoppingCh:            make(chan none),
	}
	cg.dispatcher = newDispatcher(cg.baseCID, cg, sc.config)
	return cg
}

func (cg *consumerGroup) String() string {
	return cg.baseCID.String()
}

func (cg *consumerGroup) addTopicConsumer() chan<- *topicConsumer {
	return cg.addTopicConsumerCh
}

func (cg *consumerGroup) deleteTopicConsumer() chan<- *topicConsumer {
	return cg.deleteTopicConsumerCh
}

func (cg *consumerGroup) key() string {
	return cg.group
}

func (cg *consumerGroup) start(stoppedCh chan<- dispatchTier) {
	spawn(&cg.wg, func() {
		defer func() { stoppedCh <- cg }()
		cg.registry = spawnConsumerGroupRegister(cg.group, cg.config.ClientID, cg.config, cg.kazooConn)
		var manageWg sync.WaitGroup
		spawn(&manageWg, cg.managePartitions)
		cg.dispatcher.start()
		<-cg.stoppingCh
		cg.dispatcher.stop()
		cg.registry.stop()
		manageWg.Wait()
	})
}

func (cg *consumerGroup) requests() chan<- consumeRequest {
	return cg.dispatcher.requests()
}

func (cg *consumerGroup) stop() {
	close(cg.stoppingCh)
	cg.wg.Wait()
}

func (cg *consumerGroup) dispatchKey(req consumeRequest) string {
	return req.topic
}

func (cg *consumerGroup) newDispatchTier(key string) dispatchTier {
	tc := cg.newTopicConsumer(key)
	return tc
}

func (cg *consumerGroup) managePartitions() {
	cid := cg.baseCID.NewChild("managePartitions")
	defer cid.LogScope()()

	topicConsumers := make(map[string]*topicConsumer)
	var topics []string
	var memberSubscriptions map[string][]string
	ok := true
	var nilOrRetryCh <-chan time.Time
	var nilOrRegistryTopicsCh chan<- []string
	shouldRebalance, canRebalance := false, true
	rebalanceResultCh := make(chan error, 1)
rebalancingLoop:
	for {
		select {
		case tc := <-cg.addTopicConsumerCh:
			topicConsumers[tc.topic] = tc
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = cg.registry.topics()
			continue rebalancingLoop
		case tc := <-cg.deleteTopicConsumerCh:
			delete(topicConsumers, tc.topic)
			topics = listTopics(topicConsumers)
			nilOrRegistryTopicsCh = cg.registry.topics()
			continue rebalancingLoop
		case nilOrRegistryTopicsCh <- topics:
			nilOrRegistryTopicsCh = nil
			continue rebalancingLoop
		case memberSubscriptions, ok = <-cg.registry.membershipChanges():
			if !ok {
				goto done
			}
			nilOrRetryCh = nil
			shouldRebalance = true
		case err := <-rebalanceResultCh:
			canRebalance = true
			if err != nil {
				log.Errorf("<%s> rebalance failed: err=(%s)", cid, err)
				nilOrRetryCh = time.After(cg.config.Consumer.BackOffTimeout)
				continue
			}
		case <-nilOrRetryCh:
			shouldRebalance = true
		}

		if shouldRebalance && canRebalance {
			topicConsumerCopy := make(map[string]*topicConsumer, len(topicConsumers))
			for topic, tc := range topicConsumers {
				topicConsumerCopy[topic] = tc
			}
			go cg.rebalance(topicConsumerCopy, memberSubscriptions, rebalanceResultCh)
			shouldRebalance, canRebalance = false, false
		}
	}
done:
	var allExclusiveConsumers []*exclusiveConsumer
	for _, topicExclusiveConsumers := range cg.exclusiveConsumers {
		for _, ec := range topicExclusiveConsumers {
			allExclusiveConsumers = append(allExclusiveConsumers, ec)
		}
	}
	stopExclusiveConsumers(allExclusiveConsumers)
}

func (cg *consumerGroup) rebalance(topicConsumers map[string]*topicConsumer,
	memberSubscriptions map[string][]string, rebalanceResultCh chan<- error) {

	cid := cg.baseCID.NewChild("rebalance")
	defer cid.LogScope(topicConsumers, memberSubscriptions)()

	// Convert subscriber->topics to topic->subscribers map.
	topicSubscribers := make(map[string][]string)
	for memberID, topics := range memberSubscriptions {
		for _, topic := range topics {
			topicSubscribers[topic] = append(topicSubscribers[topic], memberID)
		}
	}
	// Create a set of topics this consumer group member subscribed to.
	subscribedTopics := make(map[string]none)
	for _, topic := range memberSubscriptions[cg.config.ClientID] {
		subscribedTopics[topic] = nothing
	}
	// Resolve new partition assignments for the subscribed topics.
	assignments := make(map[string]map[int32]none)
	for topic := range subscribedTopics {
		topicPartitions, err := cg.kafkaClient.Partitions(topic)
		if err != nil {
			log.Errorf("<%s> failed to get partition list: topic=%s, err=(%s)", cid, topic, err)
			rebalanceResultCh <- err
			return
		}
		groupAssignments := resolveAssignments(topicPartitions, topicSubscribers[topic])
		topicAssignments := groupAssignments[cg.config.ClientID]
		log.Infof("<%s> partitions assigned: topic=%s, my=%v, all=%v", cid, topic, topicAssignments, groupAssignments)
		assignments[topic] = topicAssignments
	}
	// Stop partition consumers that are no more assigned to this group member.
	var unassignedPartitions []*exclusiveConsumer
	for topic, topicExclusiveConsumers := range cg.exclusiveConsumers {
		topicAssignments := assignments[topic]
		for partition, ec := range topicExclusiveConsumers {
			if _, ok := topicAssignments[partition]; !ok {
				delete(topicExclusiveConsumers, partition)
				unassignedPartitions = append(unassignedPartitions, ec)
			}
		}
	}
	stopExclusiveConsumers(unassignedPartitions)
	// Spawn consumers for just assigned partitions.
	for topic, topicAssignments := range assignments {
		tc := topicConsumers[topic]
		if tc == nil {
			continue
		}
		topicExclusiveConsumers := cg.exclusiveConsumers[topic]
		if topicExclusiveConsumers == nil {
			topicExclusiveConsumers = make(map[int32]*exclusiveConsumer, len(topicAssignments))
			cg.exclusiveConsumers[topic] = topicExclusiveConsumers
		}
		for partition := range topicAssignments {
			if _, ok := topicExclusiveConsumers[partition]; !ok {
				ec := cg.spawnExclusiveConsumer(tc, partition)
				topicExclusiveConsumers[partition] = ec
			}
		}
	}
	// Notify the caller that rebalancing has completed successfully.
	rebalanceResultCh <- nil
	return
}

func stopExclusiveConsumers(exclusiveConsumers []*exclusiveConsumer) {
	wg := sync.WaitGroup{}
	for _, ec := range exclusiveConsumers {
		spawn(&wg, ec.stop)
	}
	wg.Wait()
}

func listTopics(topicConsumers map[string]*topicConsumer) []string {
	topics := make([]string, 0, len(topicConsumers))
	for topic := range topicConsumers {
		topics = append(topics, topic)
	}
	return topics
}

func resolveAssignments(partitions []int32, members []string) map[string]map[int32]none {
	partitionCount := len(partitions)
	memberCount := len(members)
	if partitionCount == 0 || memberCount == 0 {
		return nil
	}
	sort.Sort(Int32Slice(partitions))
	sort.Sort(sort.StringSlice(members))

	assignments := make(map[string]map[int32]none, memberCount)
	partitionsPerMember := partitionCount / memberCount
	extra := partitionCount - memberCount*partitionsPerMember

	begin := 0
	for _, memberID := range members {
		end := begin + partitionsPerMember
		if extra != 0 {
			end++
			extra--
		}
		for _, partition := range partitions[begin:end] {
			topicAssignments := assignments[memberID]
			if topicAssignments == nil {
				topicAssignments = make(map[int32]none, partitionCount)
				assignments[memberID] = topicAssignments
			}
			assignments[memberID][partition] = nothing
		}
		begin = end
	}
	return assignments
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
	cg            *consumerGroup
	group         string
	topic         string
	assignmentsCh chan []int32
	requestsCh    chan consumeRequest
	messagesCh    chan *sarama.ConsumerMessage
	wg            sync.WaitGroup
}

func (cg *consumerGroup) newTopicConsumer(topic string) *topicConsumer {
	return &topicConsumer{
		contextID:     cg.baseCID.NewChild(topic),
		config:        cg.config,
		cg:            cg,
		group:         cg.group,
		topic:         topic,
		assignmentsCh: make(chan []int32),
		requestsCh:    make(chan consumeRequest, cg.config.ChannelBufferSize),
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
	tc.cg.addTopicConsumer() <- tc
	defer func() {
		tc.cg.deleteTopicConsumer() <- tc
	}()

	timeoutErr := ErrConsumerRequestTimeout(fmt.Errorf("<%s> timeout", tc.contextID))
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

func (cg *consumerGroup) spawnExclusiveConsumer(tc *topicConsumer, partition int32) *exclusiveConsumer {
	ec := &exclusiveConsumer{
		contextID:    cg.baseCID.NewChild(fmt.Sprintf("%s:%d", tc.topic, partition)),
		tc:           tc,
		group:        cg.group,
		topic:        tc.topic,
		partition:    partition,
		dumbConsumer: cg.dumbConsumer,
		registry:     cg.registry,
		offsetMgr:    cg.offsetMgr,
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

	var initialOffset sarama.FetchedOffset
	for {
		select {
		case initialOffset = <-pom.InitialOffset():
			goto spawnPartitionConsumer
		case <-ec.stoppingCh:
			return
		}
	}
spawnPartitionConsumer:
	pc, err := ec.dumbConsumer.ConsumePartition(ec.topic, ec.partition, initialOffset.Offset)
	if err != nil {
		log.Errorf("<%s> failed to start partition consumer: err=(%s)", ec.contextID, err)
		return
	}
	defer pc.Close()

	log.Infof("<%s> initialized: offset=%d", ec.contextID, initialOffset.Offset)
	for {
		select {
		case consumerMessage := <-pc.Messages():
			select {
			case ec.tc.messages() <- consumerMessage:
				pom.CommitOffset(consumerMessage.Offset+1, "")
			case <-ec.stoppingCh:
				return
			}
		case <-ec.stoppingCh:
			return
		}
	}
}

func (ec *exclusiveConsumer) stop() {
	close(ec.stoppingCh)
	ec.wg.Wait()
}
