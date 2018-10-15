package subscriber

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer/subscriber/kazoo"
	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/samuel/go-zookeeper/zk"
)

const (
	// It is ok for an attempt to claim a partition to fail, for it might take
	// some time for the current partition owner to release it. So we won't report
	// first several failures to claim a partition as an error.
	safeClaimRetriesCount = 10
)

// T is a subscriber implementation based on ZooKeeper. It maintains consumer
// group membership and topic subscriptions, watches for other members to join,
// leave and update their subscriptions, and generates notifications of such
// changes. It also provides an API to for a partition consumer to claim and
// release a group-topic-partition.
//
// FIXME: It is assumed that all members of the group are registered with the
// FIXME: `static` pattern. If a member that pattern is either `white_list` or
// FIXME: `black_list` joins the group the result will be unpredictable.
type T struct {
	actDesc         *actor.Descriptor
	cfg             *config.Proxy
	group           string
	kazooModel      kazoo.Model
	registered      bool
	topicsCh        chan []string
	subscriptionsCh chan map[string][]string
	stopCh          chan none.T
	claimErrorsCh   chan none.T
	wg              sync.WaitGroup
}

// Spawn creates a subscriber instance and starts its goroutine.
func Spawn(parentActDesc *actor.Descriptor, group string, cfg *config.Proxy, zkConn *zk.Conn) *T {
	actDesc := parentActDesc.NewChild("member")
	actDesc.AddLogField("kafka.group", group)
	kazooModel := kazoo.NewModel(
		zkConn,
		cfg.ZooKeeper.Chroot,
		group,
		cfg.ClientID,
		actDesc.Log())
	ss := &T{
		actDesc:         actDesc,
		cfg:             cfg,
		group:           group,
		kazooModel:      kazooModel,
		topicsCh:        make(chan []string),
		subscriptionsCh: make(chan map[string][]string),
		stopCh:          make(chan none.T),
		claimErrorsCh:   make(chan none.T, 1),
	}
	actor.Spawn(ss.actDesc, &ss.wg, ss.run)
	return ss
}

// Topics returns a channel to receive a list of topics the member should
// subscribe to. To make the member unsubscribe from all topics either nil or
// an empty topic list can be sent.
func (s *T) Topics() chan<- []string {
	return s.topicsCh
}

// Subscriptions returns a channel that subscriptions will be sent whenever a
// member joins or leaves the group or when an existing member updates its
// subscription.
func (s *T) Subscriptions() <-chan map[string][]string {
	return s.subscriptionsCh
}

// ClaimPartition claims a topic/partition to be consumed by this member of the
// consumer group. It blocks until either succeeds or canceled by the caller. It
// returns a function that should be called to release the claim.
func (s *T) ClaimPartition(claimerActDesc *actor.Descriptor, topic string, partition int32, cancelCh <-chan none.T) func() {
	pc := partitionClaimer{
		subscriber: s,
		actDesc:    claimerActDesc,
		topic:      topic,
		partition:  partition,
		cancelCh:   cancelCh,
	}
	return pc.claim()
}

// Stop signals the consumer group member to stop and blocks until its
// goroutines are over.
func (s *T) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *T) run() {
	defer close(s.subscriptionsCh)

	var err error
	// Ensure that the member leaves the group in ZooKeeper on stop. We retry
	// indefinitely here until ZooKeeper confirms that there is no registration.
	defer func() {
		for {
			err := s.kazooModel.EnsureMemberSubscription(nil)
			if err == nil {
				return
			}
			s.actDesc.Log().WithError(err).Error("Failed to unregister")
			<-time.After(s.cfg.Consumer.RetryBackoff)
		}
	}()

	var (
		nilOrSubscriptionsCh     chan<- map[string][]string
		nilOrWatchCh             <-chan none.T
		nilOrTimeoutCh           <-chan time.Time
		cancelWatch              context.CancelFunc
		shouldSubmitTopics       = false
		shouldFetchSubscriptions = false
		topics                   []string
		subscriptions            map[string][]string
		submittedAt              = time.Now()
	)
	for {
		select {
		case topics = <-s.topicsCh:
			sort.Strings(topics)
			shouldSubmitTopics = true

		case nilOrSubscriptionsCh <- subscriptions:
			nilOrSubscriptionsCh = nil

		case <-nilOrWatchCh:
			nilOrWatchCh = nil
			cancelWatch()
			shouldFetchSubscriptions = true

		case <-s.claimErrorsCh:
			sinceLastSubmit := time.Now().Sub(submittedAt)
			if sinceLastSubmit > s.cfg.Consumer.RetryBackoff {
				s.actDesc.Log().Infof("Resubmit triggered by claim failure: since=%v", sinceLastSubmit)
				shouldSubmitTopics = true
			}
		case <-nilOrTimeoutCh:
			nilOrTimeoutCh = nil

		case <-s.stopCh:
			if cancelWatch != nil {
				cancelWatch()
			}
			return
		}

		if shouldSubmitTopics {
			if err = s.kazooModel.EnsureMemberSubscription(topics); err != nil {
				s.actDesc.Log().WithError(err).Error("Failed to submit topics")
				nilOrTimeoutCh = time.After(s.cfg.Consumer.RetryBackoff)
				continue
			}
			submittedAt = time.Now()
			s.actDesc.Log().Infof("Submitted: topics=%v", topics)
			shouldSubmitTopics = false
			if cancelWatch != nil {
				cancelWatch()
			}
			shouldFetchSubscriptions = true
		}

		if shouldFetchSubscriptions {
			subscriptions, nilOrWatchCh, cancelWatch, err = s.kazooModel.FetchGroupSubscriptions()
			if err != nil {
				s.actDesc.Log().WithError(err).Error("Failed to fetch subscriptions")
				nilOrTimeoutCh = time.After(s.cfg.Consumer.RetryBackoff)
				continue
			}
			shouldFetchSubscriptions = false
			s.actDesc.Log().Infof("Fetched subscriptions: %s", prettyfmt.Val(subscriptions))
			nilOrSubscriptionsCh = s.subscriptionsCh

			// If fetched topics are not the same as the current subscription
			// then initiate topic submission.
			fetchedTopics := subscriptions[s.cfg.ClientID]
			if reflect.DeepEqual(topics, fetchedTopics) {
				continue
			}
			s.actDesc.Log().Errorf("Outdated subscription: want=%v, got=%v", topics, fetchedTopics)
			shouldSubmitTopics = true
		}
	}
}

type partitionClaimer struct {
	subscriber *T
	actDesc    *actor.Descriptor
	topic      string
	partition  int32
	cancelCh   <-chan none.T
}

func (pc *partitionClaimer) claim() func() {
	beginAt := time.Now()
	retries := 0
	for {
		err := pc.subscriber.kazooModel.CreatePartitionOwner(pc.topic, pc.partition)
		if err == nil {
			break
		}
		logEntry := pc.actDesc.Log().WithError(err)
		logFailureFn := logEntry.Infof
		if retries++; retries > safeClaimRetriesCount {
			logFailureFn = logEntry.Errorf
		}
		logFailureFn("Failed to claim partition: via=%s, retries=%d, took=%s",
			pc.subscriber.actDesc, retries, time.Since(beginAt))

		// Let the subscriber actor know that a claim attempt failed.
		select {
		case pc.subscriber.claimErrorsCh <- none.V:
		default:
		}
		// Wait until either the retry timeout expires or the claim is canceled.
		select {
		case <-time.After(pc.subscriber.cfg.Consumer.RetryBackoff):
		case <-pc.cancelCh:
			return func() {}
		}
	}
	pc.actDesc.Log().Infof("Partition claimed: via=%s, retries=%d, took=%s",
		pc.subscriber.actDesc, retries, time.Since(beginAt))
	return pc.release
}

func (pc *partitionClaimer) release() {
	beginAt := time.Now()
	retries := 0
	for {
		err := pc.subscriber.kazooModel.DeletePartitionOwner(pc.topic, pc.partition)
		if err == nil {
			break
		}
		logEntry := pc.actDesc.Log().WithError(err)
		logFailureFn := logEntry.Infof
		if retries++; retries > safeClaimRetriesCount {
			logFailureFn = logEntry.Errorf
		}
		logFailureFn("Failed to release partition: via=%s, retries=%d, took=%s",
			pc.subscriber.actDesc, retries, time.Since(beginAt))
		<-time.After(pc.subscriber.cfg.Consumer.RetryBackoff)
	}
	pc.actDesc.Log().Infof("Partition released: via=%s, retries=%d, took=%s",
		pc.subscriber.actDesc, retries, time.Since(beginAt))
}
