package topiccsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/holster/clock"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
)

var (
	requestTimeoutRs         = consumer.Response{Err: consumer.ErrRequestTimeout}
	safe2StopPollingInterval = 100 * time.Millisecond
)

// T serves consume requests received from childSpec.Requests() channel with
// messages received from Messages() channel. The topic consumer expires and
// shuts itself down when either of the following happens:
// * there has been no requests for Consumer.SubscriptionTimeout and
//   isSafe2StopFn returns true;
// * there has been no requests for max value of Consumer.SubscriptionTimeout
//   and Consumer.AckTimeout
//
// implements `multiplexer.Out`.
type T struct {
	actDesc       *actor.Descriptor
	childSpec     dispatcher.ChildSpec
	cfg           *config.Proxy
	group         string
	topic         string
	lifespanCh    chan<- *T
	isSafe2StopFn func() bool
	messagesCh    chan consumer.Message
	wg            sync.WaitGroup
}

// Spawn creates and starts a topic consumer instance.
func Spawn(parentActDesc *actor.Descriptor, group string, childSpec dispatcher.ChildSpec,
	cfg *config.Proxy, lifespanCh chan<- *T, isSafe2StopFn func() bool,
) *T {
	topic := string(childSpec.Key())
	actDesc := parentActDesc.NewChild(fmt.Sprintf("%s", topic))
	actDesc.AddLogField("kafka.group", group)
	actDesc.AddLogField("kafka.topic", topic)
	tc := T{
		actDesc:       actDesc,
		childSpec:     childSpec,
		cfg:           cfg,
		group:         group,
		topic:         topic,
		lifespanCh:    lifespanCh,
		isSafe2StopFn: isSafe2StopFn,

		// Messages channel must be non-buffered. Otherwise we might end up
		// buffering a message from a partition that no longer belongs to this
		// consumer group member.
		messagesCh: make(chan consumer.Message),
	}
	actor.Spawn(tc.actDesc, &tc.wg, tc.run)
	return &tc
}

// Topic returns the topic name this topic consumer is responsible for.
func (tc *T) Topic() string {
	return tc.topic
}

// implements `multiplexer.Out`
func (tc *T) Messages() chan<- consumer.Message {
	return tc.messagesCh
}

func (tc *T) run() {
	defer tc.childSpec.Dispose()
	tc.lifespanCh <- tc
	defer func() {
		tc.lifespanCh <- tc
	}()

	latestRqTime := clock.Now().UTC()
	expireTimer := clock.NewTimer(tc.cfg.Consumer.SubscriptionTimeout)
	defer expireTimer.Stop()
	for {
		// Serve requests, and jump to the safe stop when they cease coming.
	serveRequests:
		for {
			select {
			case consumeRq, ok := <-tc.childSpec.Requests():
				if !ok {
					tc.actDesc.Log().Info("Shutting down")
					return
				}
				latestRqTime = tc.serveRequest(consumeRq)
			case <-expireTimer.C():
				sinceLatestRq := clock.Now().UTC().Sub(latestRqTime)
				subscriptionTTL := tc.cfg.Consumer.SubscriptionTimeout - sinceLatestRq
				if subscriptionTTL <= 0 {
					tc.actDesc.Log().Info("Topic subscription expired")
					goto wait4SafeStop
				}
				expireTimer.Reset(subscriptionTTL)
			}
		}
		// Keep polling isSafe2StopFn until it returns true or the ack timeout
		// expires and then terminate. If new request arrives while waiting,
		// then jump back to the request serving loop.
	wait4SafeStop:
		for {
			if tc.isSafe2StopFn() {
				return
			}
			sinceLatestRq := clock.Now().UTC().Sub(latestRqTime)
			if sinceLatestRq >= tc.cfg.Consumer.AckTimeout {
				tc.actDesc.Log().Error("Stopping unsafely, some messages can be consumed more than once")
				return
			}
			select {
			case consumeRq, ok := <-tc.childSpec.Requests():
				if !ok {
					tc.actDesc.Log().Info("Signaled to shutdown")
					return
				}
				tc.actDesc.Log().Info("Resume request handling")
				latestRqTime = tc.serveRequest(consumeRq)
				subscriptionTTL := tc.cfg.Consumer.SubscriptionTimeout - sinceLatestRq
				expireTimer.Reset(subscriptionTTL)
				goto serveRequests
			case <-clock.After(safe2StopPollingInterval):
			}
		}
	}
}

func (tc *T) String() string {
	return tc.actDesc.String()
}

func (tc *T) serveRequest(consumeRq consumer.Request) time.Time {
	latestRqTime := clock.Now().UTC()
	requestAge := latestRqTime.Sub(consumeRq.Timestamp)
	requestTTL := tc.cfg.Consumer.LongPollingTimeout - requestAge
	// The request has been waiting in the buffer for too long. If we
	// reply with a fetched message, then there is a good chance that the
	// client won't receive it due to the client HTTP timeout. Therefore
	// we reject the request to avoid message loss.
	if requestTTL <= 0 {
		consumeRq.ResponseCh <- requestTimeoutRs
		return latestRqTime
	}
	select {
	case msg := <-tc.messagesCh:
		msg.EventsCh <- consumer.Event{T: consumer.EvOffered, Offset: msg.Offset}
		consumeRq.ResponseCh <- consumer.Response{Msg: msg}
	case <-clock.After(requestTTL):
		consumeRq.ResponseCh <- requestTimeoutRs
	}
	return latestRqTime
}
