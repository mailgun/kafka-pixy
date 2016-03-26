package consumer

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/context"
	"github.com/mailgun/log"
	"github.com/mailgun/sarama"
	"github.com/wvanbergen/kazoo-go"
)

type (
	ErrSetup          error
	ErrBufferOverflow error
	ErrRequestTimeout error
)

var (
	// If this channel is not `nil` then exclusive consumers will use it to
	// notify when they fetch the very first message.
	firstMessageFetchedCh chan *exclusiveConsumer
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
type T struct {
	baseCID     *context.ID
	cfg         *config.T
	dispatcher  *dispatcher
	kafkaClient sarama.Client
	offsetMgr   sarama.OffsetManager
	kazooConn   *kazoo.Kazoo
}

// Spawn creates a consumer instance with the specified configuration and
// starts all its goroutines.
func Spawn(cfg *config.T) (*T, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.ClientID = cfg.ClientID
	saramaCfg.ChannelBufferSize = cfg.Consumer.ChannelBufferSize
	saramaCfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	saramaCfg.Consumer.Retry.Backoff = cfg.Consumer.BackOffTimeout
	saramaCfg.Consumer.Fetch.Default = 1024 * 1024

	kafkaClient, err := sarama.NewClient(cfg.Kafka.SeedPeers, saramaCfg)
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	offsetMgr, err := sarama.NewOffsetManagerFromClient(kafkaClient)
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create sarama.OffsetManager: err=(%v)", err))
	}

	kazooCfg := kazoo.NewConfig()
	kazooCfg.Chroot = cfg.ZooKeeper.Chroot
	// ZooKeeper documentation says following about the session timeout: "The
	// current (ZooKeeper) implementation requires that the timeout be a
	// minimum of 2 times the tickTime (as set in the server configuration) and
	// a maximum of 20 times the tickTime". The default tickTime is 2 seconds.
	// See http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkSessions
	kazooCfg.Timeout = 15 * time.Second

	kazooConn, err := kazoo.NewKazoo(cfg.ZooKeeper.SeedPeers, kazooCfg)
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create kazoo.Kazoo: err=(%v)", err))
	}

	sc := &T{
		baseCID:     context.RootID.NewChild("smartConsumer"),
		cfg:         cfg,
		kafkaClient: kafkaClient,
		offsetMgr:   offsetMgr,
		kazooConn:   kazooConn,
	}
	sc.dispatcher = newDispatcher(sc.baseCID, sc, sc.cfg)
	sc.dispatcher.start()
	return sc, nil
}

// Stop sends the shutdown signal to all internal goroutines and blocks until
// all of them are stopped. It is guaranteed that all last consumed offsets of
// all consumer groups/topics are committed to Kafka before SmartConsumer stops.
func (sc *T) Stop() {
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
func (sc *T) Consume(group, topic string) (*sarama.ConsumerMessage, error) {
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

func (sc *T) String() string {
	return sc.baseCID.String()
}

func (sc *T) dispatchKey(req consumeRequest) string {
	return req.group
}

func (sc *T) newDispatchTier(key string) dispatchTier {
	return sc.newConsumerGroup(key)
}

// topicConsumer implements a consumer request dispatch tier responsible for
// consumption of a particular topic by a consumer group. It receives requests
// on the `requests()` channel and replies with messages selected by the
// respective multiplexer. If there has been no message received for
// `Config.Consumer.LongPollingTimeout` then a timeout error is sent to the
// requests' reply channel.
type topicConsumer struct {
	contextID     *context.ID
	cfg           *config.T
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
		cfg:           gc.cfg,
		gc:            gc,
		group:         gc.group,
		topic:         topic,
		assignmentsCh: make(chan []int32),
		requestsCh:    make(chan consumeRequest, gc.cfg.Consumer.ChannelBufferSize),
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
	tc.gc.topicConsumerLifespan() <- tc
	defer func() {
		tc.gc.topicConsumerLifespan() <- tc
	}()

	timeoutErr := ErrRequestTimeout(fmt.Errorf("long polling timeout"))
	timeoutResult := consumeResult{Err: timeoutErr}
	for consumeReq := range tc.requestsCh {
		requestAge := time.Now().UTC().Sub(consumeReq.timestamp)
		ttl := tc.cfg.Consumer.LongPollingTimeout - requestAge
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

// exclusiveConsumer ensures exclusive consumption of messages from a topic
// partition within a particular group. It ensures that a partition is consumed
// exclusively by first claiming the partition in ZooKeeper. When a fetched
// message is pulled from the `messages()` channel, it is considered to be
// consumed and its offset is committed.
type exclusiveConsumer struct {
	contextID    *context.ID
	cfg          *config.T
	group        string
	topic        string
	partition    int32
	dumbConsumer sarama.Consumer
	registry     *groupRegistrator
	offsetMgr    sarama.OffsetManager
	messagesCh   chan *sarama.ConsumerMessage
	acksCh       chan *sarama.ConsumerMessage
	stoppingCh   chan none
	wg           sync.WaitGroup
}

func (gc *groupConsumer) spawnExclusiveConsumer(topic string, partition int32) *exclusiveConsumer {
	ec := &exclusiveConsumer{
		contextID:    gc.baseCID.NewChild(fmt.Sprintf("%s:%d", topic, partition)),
		cfg:          gc.cfg,
		group:        gc.group,
		topic:        topic,
		partition:    partition,
		dumbConsumer: gc.dumbConsumer,
		registry:     gc.registry,
		offsetMgr:    gc.offsetMgr,
		messagesCh:   make(chan *sarama.ConsumerMessage),
		acksCh:       make(chan *sarama.ConsumerMessage),
		stoppingCh:   make(chan none),
	}
	spawn(&ec.wg, ec.run)
	return ec
}

func (ec *exclusiveConsumer) messages() <-chan *sarama.ConsumerMessage {
	return ec.messagesCh
}

func (ec *exclusiveConsumer) acks() chan<- *sarama.ConsumerMessage {
	return ec.acksCh
}

func (ec *exclusiveConsumer) run() {
	defer ec.contextID.LogScope()()
	defer ec.registry.claimPartition(ec.contextID, ec.topic, ec.partition, ec.stoppingCh)()

	pom, err := ec.offsetMgr.ManagePartition(ec.group, ec.topic, ec.partition)
	if err != nil {
		panic(fmt.Errorf("<%s> failed to spawn partition manager: err=(%s)", ec.contextID, err))
	}
	defer pom.Close()

	// Wait for the initial offset to be retrieved.
	var initialOffset sarama.DecoratedOffset
	select {
	case initialOffset = <-pom.InitialOffset():
	case <-ec.stoppingCh:
		return
	}

	pc, concreteOffset, err := ec.dumbConsumer.ConsumePartition(ec.topic, ec.partition, initialOffset.Offset)
	if err != nil {
		panic(fmt.Errorf("<%s> failed to start partition consumer: err=(%s)", ec.contextID, err))
	}
	defer pc.Close()
	log.Infof("<%s> initialized: initialOffset=%d, concreteOffset=%d",
		ec.contextID, initialOffset.Offset, concreteOffset)

	var lastSubmittedOffset, lastCommittedOffset int64

	// Initialize the Kafka offset storage for a group on first consumption.
	if initialOffset.Offset == sarama.OffsetNewest {
		pom.SubmitOffset(concreteOffset, "")
		lastSubmittedOffset = concreteOffset
	}

	firstMessageFetched := false
	for {
		var msg *sarama.ConsumerMessage
		// Wait for a fetched message to to provided by the controlled
		// partition consumer.
		for {
			select {
			case msg = <-pc.Messages():
				// Notify tests when the very first message is fetched.
				if !firstMessageFetched && firstMessageFetchedCh != nil {
					firstMessageFetched = true
					firstMessageFetchedCh <- ec
				}
				goto offerAndAck
			case committedOffset := <-pom.CommittedOffsets():
				lastCommittedOffset = committedOffset.Offset
				continue
			case <-ec.stoppingCh:
				goto done
			}
		}
	offerAndAck:
		// Offer the fetched message to the upstream consumer and wait for it
		// to be acknowledged.
		for {
			select {
			case ec.messagesCh <- msg:
				// Keep offering the same message until it is acknowledged.
			case <-ec.acksCh:
				lastSubmittedOffset = msg.Offset + 1
				pom.SubmitOffset(lastSubmittedOffset, "")
				break offerAndAck
			case committedOffset := <-pom.CommittedOffsets():
				lastCommittedOffset = committedOffset.Offset
				continue
			case <-ec.stoppingCh:
				goto done
			}
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

type none struct{}

// spawn starts function `f` as a goroutine making it a member of the `wg`
// wait group.
func spawn(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}
