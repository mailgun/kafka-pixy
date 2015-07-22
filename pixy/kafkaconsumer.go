package pixy

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/kazoo-go"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

var (
	ErrBufferOverflow = errors.New("Request buffer is full")
	ErrRequestTimeout = errors.New("Reqeust timeout has elapsed")
)

// SmartConsumer is a Kafka consumer implementation that automatically
// maintains consumer groups registrations and topic subscriptions. Whenever a
// a message from a particular topic is consumed by a particular consumer group
// SmartConsumer checks if it has registered with the consumer group, and
// registers otherwise. Then it checks if it has subscribed for the topic, and
// subscribes otherwise. Later if a particular topic has not been consumed for
// the `KafkaClientCfg.Consumer.DiscardAfter` period of time, then the consumer
// unsubscribes from the topic, likewise if a consumer group has not seen any
// requests for that period then the consumer deregisters from the group.
//
// Note that during state transitions subscribe<->unsubscribe and
// register<->deregister the consumer may return error. In that case the user
// should back off a bit and then repeat the request.
type SmartConsumer struct {
	config         *KafkaClientCfg
	saramaClient   sarama.Client
	saramaConsumer sarama.Consumer
	kazooConn      *kazoo.Kazoo
	requestsCh     chan *consumeRequest
	wg             sync.WaitGroup
}

type consumeRequest struct {
	timestamp time.Time
	group     string
	topic     string
	replyCh   chan<- *ConsumeResult
}

type ConsumeResult struct {
	Msg *sarama.ConsumerMessage
	Err error
}

func SpawnSmartConsumer(config *KafkaClientCfg, saramaClient sarama.Client, kazooConn *kazoo.Kazoo) (*SmartConsumer, error) {
	saramaConsumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}
	sc := &SmartConsumer{
		config:         config,
		saramaClient:   saramaClient,
		saramaConsumer: saramaConsumer,
		kazooConn:      kazooConn,
		requestsCh:     make(chan *consumeRequest, config.ChannelBufferSize),
	}

	goGo(&sc.wg, func() { sc.dispatch(sc) })

	return sc, nil
}

func (sc *SmartConsumer) Stop() {
	close(sc.requestsCh)
	sc.wg.Wait()
}

func (sc *SmartConsumer) Consume(group, topic string) *ConsumeResult {
	replyCh := make(chan *ConsumeResult, 1)
	sc.requestsCh <- &consumeRequest{time.Now().UTC(), group, topic, replyCh}
	return <-replyCh
}

// dispatchTier interface represents an entity that along with the
// `SmartConsumer.dispatch` method allows construction of hierarchical request
// processing structures.
type dispatchTier interface {
	// key returns a unique identifier of the key. It must equal to the value
	// returned by `childKey` method of the parent tier.
	key() string
	// requests is a channel that the tier is reading requests from.
	requests() chan *consumeRequest
	// start spins up the tier's goroutines.
	start()
	// stop makes all tier goroutines stop and releases all resources.
	stop()
}

// dispatchTierParent interface represents an entity that along with the
// `SmartConsumer.dispatch` method allows construction of hierarchical request
// processing structures.
type dispatchTierParent interface {
	// requests is a channel that the tier is reading requests from.
	requests() chan *consumeRequest
	// childKey returns a key that a child created for the specified `req`
	// should have.
	childKey(req *consumeRequest) string
	// newChild creates a new dispatch tier.
	newChild(key string) dispatchTier
}

// expiringDispatchTier represents a dispatch tier that expires if not used.
type expiringDispatchTier struct {
	instance  dispatchTier
	successor dispatchTier
	timer     *time.Timer
	expired   bool
}

// dispatch forwards consume requests received from the `parent` tier requests
// channel to a child requests channel. The destination child is determined by
// `parent.childKey()`. If there is no child with the specified key, then it is
// created.
//
// If a child tier has not been used for a period of time given by the
// `Config.Consumer.RegistrationTimeout` parameter, then it is closed.
func (sc *SmartConsumer) dispatch(parent dispatchTierParent) {
	defer logScope(fmt.Sprintf("%v/dispatch", parent))

	children := make(map[string]*expiringDispatchTier)
	expiredChildrenCh := make(chan dispatchTier, sc.config.ChannelBufferSize)
	stoppedChildrenCh := make(chan dispatchTier, sc.config.ChannelBufferSize)
dispatchLoop:
	for {
		select {
		case req, ok := <-parent.requests():
			if !ok {
				goto done
			}
			childKey := parent.childKey(req)
			var child dispatchTier
			expiringChild := children[childKey]
			if expiringChild == nil {
				child = parent.newChild(childKey)
				child.start()
				expiringChild = &expiringDispatchTier{
					instance: child,
					timer: time.AfterFunc(
						sc.config.Consumer.RegistrationTimeout, func() { expiredChildrenCh <- child }),
				}
				children[childKey] = expiringChild
			} else {
				child = expiringChild.instance
				if expiringChild.expired || !expiringChild.timer.Reset(sc.config.Consumer.RegistrationTimeout) {
					child = expiringChild.successor
					if child == nil {
						child = parent.newChild(childKey)
						expiringChild.successor = child
					}
				}
			}
			// Forward the request to the destination child tier, but drop it
			// if request channel buffer of the child tier is full.
			select {
			case child.requests() <- req:
			default:
				req.replyCh <- &ConsumeResult{Err: ErrBufferOverflow}
			}

		case child := <-expiredChildrenCh:
			expiringChild := children[child.key()]
			if expiringChild != nil && expiringChild.instance == child {
				expiringChild.expired = true
				go func() {
					expiringChild.instance.stop()
					stoppedChildrenCh <- expiringChild.instance
				}()
			}

		case child := <-stoppedChildrenCh:
			expiringChild := children[child.key()]
			if expiringChild.successor == nil {
				delete(children, child.key())
				continue dispatchLoop
			}
			expiringChild.expired = false
			expiringChild.instance = expiringChild.successor
			expiringChild.successor = nil
			expiringChild.instance.start()
		}
	}
done:
	wg := sync.WaitGroup{}
	for _, expiringChild := range children {
		if expiringChild.expired {
			if expiringChild.successor != nil {
				goGo(&wg, expiringChild.successor.stop)
				for req := range expiringChild.successor.requests() {
					req.replyCh <- &ConsumeResult{Err: ErrBufferOverflow}
				}
			}
			continue
		}
		goGo(&wg, expiringChild.instance.stop)
	}
	wg.Wait()
}

func (sc *SmartConsumer) String() string {
	return "smartConsumer"
}

func (sc *SmartConsumer) requests() chan *consumeRequest {
	return sc.requestsCh
}

func (sc *SmartConsumer) asyncClose() {
	close(sc.requestsCh)
}

func (sc *SmartConsumer) childKey(req *consumeRequest) string {
	return req.group
}

func (sc *SmartConsumer) newChild(key string) dispatchTier {
	return sc.newConsumerGroup(key)
}

// consumerGroup manages a fleet of topic consumers and disposes of those that
// have been inactive for the `Config.Consumer.DisposeAfter` period of time.
type consumerGroup struct {
	sc                    *SmartConsumer
	group                 string
	memberID              string
	registry              *ConsumerGroupRegistry
	addTopicConsumerCh    chan *topicConsumer
	deleteTopicConsumerCh chan *topicConsumer
	requestsCh            chan *consumeRequest
	wg                    sync.WaitGroup
}

func (sc *SmartConsumer) newConsumerGroup(group string) *consumerGroup {
	cg := &consumerGroup{
		sc:                 sc,
		group:              group,
		memberID:           generateMemberID(),
		addTopicConsumerCh: make(chan *topicConsumer),
		requestsCh:         make(chan *consumeRequest, sc.config.ChannelBufferSize),
	}
	return cg
}

func (cg *consumerGroup) String() string {
	return fmt.Sprintf("%s/%s", cg.sc.String(), cg.group)
}

func (cg *consumerGroup) key() string {
	return cg.group
}

func (cg *consumerGroup) start() {
	go func() {
		defer logScope(fmt.Sprintf("%v/supervisor", cg))()
		dispatchWg := &sync.WaitGroup{}
		goGo(dispatchWg, func() { cg.sc.dispatch(cg) })
		cg.registry = SpawnConsumerGroupRegister(
			cg.group, cg.memberID, cg.sc.config, cg.sc.kazooConn)
		goGo(&cg.wg, cg.managePartitions)
		// Calling `stop` triggers the `dispatch` goroutine shutdown.
		dispatchWg.Wait()
		cg.registry.Stop()
	}()
}

func (cg *consumerGroup) requests() chan *consumeRequest {
	return cg.requestsCh
}

func (cg *consumerGroup) stop() {
	close(cg.requestsCh)
	cg.wg.Wait()
}

func (cg *consumerGroup) childKey(req *consumeRequest) string {
	return req.topic
}

func (cg *consumerGroup) newChild(key string) dispatchTier {
	tc := cg.newTopicConsumer(key)
	return tc
}

func (cg *consumerGroup) managePartitions() {
	scope := fmt.Sprintf("%v/managePartitions", cg)
	defer logScope(scope)()

	topicConsumers := make(map[string]*topicConsumer)
	var topics []string
	var subscriptions []GroupMemberSubscription
	ok := true
	var exclusiveConsumers []*exclusiveConsumer
	var nilOrRetryCh <-chan time.Time
	var nilOrTopicsCh chan<- []string
rebalancingLoop:
	for {
		select {
		case tc := <-cg.addTopicConsumerCh:
			topicConsumers[tc.topic] = tc
			topics = cg.listTopics(topicConsumers)
			nilOrTopicsCh = cg.registry.Topics()
			continue rebalancingLoop
		case tc := <-cg.deleteTopicConsumerCh:
			delete(topicConsumers, tc.topic)
			topics = cg.listTopics(topicConsumers)
			nilOrTopicsCh = cg.registry.Topics()
			continue rebalancingLoop
		case nilOrTopicsCh <- topics:
			nilOrTopicsCh = nil
			continue rebalancingLoop
		case subscriptions, ok = <-cg.registry.MembershipChanges():
			if !ok {
				goto done
			}
			nilOrRetryCh = nil
			cg.stopExclusiveConsumers(exclusiveConsumers)
		case <-nilOrRetryCh:
		}

		log.Infof("<%s> Rebalancing: topics=%v", topics)
		topicPartitions := make(map[string][]int32)
		for topic := range topicConsumers {
			partitions, err := cg.sc.saramaClient.Partitions(topic)
			if err != nil {
				log.Errorf("<%s> Failed to get partition list: topic=%s, err=%v", scope, topic, err)
				nilOrRetryCh = time.After(cg.sc.config.Consumer.BackOffTimeout)
			}
			topicPartitions[topic] = partitions
		}
		exclusiveConsumers = cg.rebalance(topicConsumers, topicPartitions, subscriptions)
	}
done:
	cg.stopExclusiveConsumers(exclusiveConsumers)
}

func (cg *consumerGroup) listTopics(topicConsumers map[string]*topicConsumer) []string {
	topics := make([]string, 0, len(topicConsumers))
	for topic := range topicConsumers {
		topics = append(topics, topic)
	}
	return topics
}

func (cg *consumerGroup) stopExclusiveConsumers(exclusiveConsumers []*exclusiveConsumer) {
	wg := sync.WaitGroup{}
	for _, ec := range exclusiveConsumers {
		goGo(&wg, ec.stop)
	}
	wg.Wait()
}

func (cg *consumerGroup) rebalance(topicConsumers map[string]*topicConsumer, topicPartitions map[string][]int32,
	subscriptions []GroupMemberSubscription) []*exclusiveConsumer {

	topicSubscribers := make(map[string][]string)
	var subscribedForTopics []string
	for _, subscription := range subscriptions {
		if subscription.memberID == cg.memberID {
			subscribedForTopics = subscription.topics
		}
		for _, topic := range subscription.topics {
			topicSubscribers[topic] = append(topicSubscribers[topic], subscription.memberID)
		}
	}

	var exclusiveConsumers []*exclusiveConsumer
	for _, topic := range subscribedForTopics {
		tc := topicConsumers[topic]
		assignments := assignPartitions(topicPartitions[topic], topicSubscribers[topic])
		for _, partition := range assignments[topic] {
			ec := tc.spawnExclusiveConsumer(partition)
			exclusiveConsumers = append(exclusiveConsumers, ec)
		}
	}
	return exclusiveConsumers
}

func generateMemberID() string {
	hostname, err := os.Hostname()
	if err != nil {
		ip, err := getIP()
		if err != nil {
			buffer := make([]byte, 8)
			_, _ = rand.Read(buffer)
			hostname = fmt.Sprintf("%X", buffer)

		} else {
			hostname = ip.String()
		}
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	return fmt.Sprintf("pixy_%s_%d_%s", hostname, os.Getpid(), timestamp)
}

func assignPartitions(partitions []int32, members []string) map[string][]int32 {
	plen := len(partitions)
	clen := len(members)
	sort.Sort(sort.StringSlice(members))

	assignments := make(map[string][]int32)
	n := plen / clen
	if plen%clen > 0 {
		n++
	}
	for i, memberID := range members {
		first := i * n
		if first > plen {
			first = plen
		}

		last := (i + 1) * n
		if last > plen {
			last = plen
		}

		for _, p := range partitions[first:last] {
			assignments[memberID] = append(assignments[memberID], p)
		}
	}
	return assignments
}

// topicConsumer receives consume requests on `messageCh` channel and replies
// with messages received via messageCh.
type topicConsumer struct {
	cg            *consumerGroup
	topic         string
	assignmentsCh chan []int32
	requestsCh    chan *consumeRequest
	messagesCh    chan *sarama.ConsumerMessage
	wg            sync.WaitGroup
}

func (cg *consumerGroup) newTopicConsumer(topic string) *topicConsumer {
	return &topicConsumer{
		cg:            cg,
		topic:         topic,
		assignmentsCh: make(chan []int32),
		requestsCh:    make(chan *consumeRequest, cg.sc.config.ChannelBufferSize),
		messagesCh:    make(chan *sarama.ConsumerMessage, 1),
	}
}

func (tc *topicConsumer) key() string {
	return tc.topic
}

func (tc *topicConsumer) requests() chan *consumeRequest {
	return tc.requestsCh
}

func (tc *topicConsumer) start() {
	goGo(&tc.wg, tc.processRequests)
}

func (tc *topicConsumer) stop() {
	close(tc.requestsCh)
	tc.wg.Wait()
}

// processRequests receives `consumerRequest`s from the `requestsCh` channel
// and sends messages retrieved from Kafka partitions assigned by the parent
// consumer group in response. If there is no message available then it blocks
// for `Config.Consumer.RequestTimeout` waiting for it.
func (tc *topicConsumer) processRequests() {
	defer logScope(fmt.Sprintf("%s/%s/processRequests", tc.cg.String(), tc.topic))()
	tc.cg.addTopicConsumerCh <- tc
	defer func() {
		tc.cg.deleteTopicConsumerCh <- tc
	}()

	for consumeReq := range tc.requestsCh {
		requestAge := time.Now().UTC().Sub(consumeReq.timestamp)
		ttl := tc.cg.sc.config.Consumer.RequestTimeout - requestAge
		// The request has been waiting in the buffer for too long. If we
		// reply with a fetched message, then there is a good chance that the
		// client won't receive it due to the client HTTP timeout. Therefore
		// we reject the request to avoid message loss.
		if ttl <= 0 {
			consumeReq.replyCh <- &ConsumeResult{Err: ErrRequestTimeout}
			continue
		}

		select {
		case msg := <-tc.messagesCh:
			consumeReq.replyCh <- &ConsumeResult{Msg: msg}
			// TODO commit offset to the offset manager
		case <-time.After(ttl):
			consumeReq.replyCh <- &ConsumeResult{Err: ErrRequestTimeout}
		}
	}
}

type exclusiveConsumer struct {
	tc         *topicConsumer
	partition  int32
	stoppingCh chan none
	wg         sync.WaitGroup
}

func (tc *topicConsumer) spawnExclusiveConsumer(partition int32) *exclusiveConsumer {
	ec := &exclusiveConsumer{
		tc:         tc,
		partition:  partition,
		stoppingCh: make(chan none),
	}
	return ec
}

func (ec *exclusiveConsumer) consumePartition() {
	scope := fmt.Sprintf("%v/%d/consumePartition", ec.tc, ec.partition)
	defer logScope(scope)()
	defer ec.tc.cg.registry.ClaimPartition(scope, ec.tc.topic, ec.partition, ec.stoppingCh)()

	// TODO retrieve the initial offset from the offset manager.
	//	offset := ec.tc.cg.sc.offsetManager.getOffset(ec.tc.topic, ec.partition)
	pc, err := ec.tc.cg.sc.saramaConsumer.ConsumePartition(ec.tc.topic, ec.partition, sarama.OffsetOldest)
	if err != nil {
		log.Errorf("<%s> Failed to start partition consumer: err=%v", scope, err)
	}

	for {
		select {
		case consumerMessage := <-pc.Messages():
			select {
			case ec.tc.messagesCh <- consumerMessage:
			case <-ec.stoppingCh:
				goto done
			}
		case <-ec.stoppingCh:
			goto done
		}
	}
done:
	pc.Close()
}

func (ec *exclusiveConsumer) stop() {
	close(ec.stoppingCh)
	ec.wg.Wait()
}
