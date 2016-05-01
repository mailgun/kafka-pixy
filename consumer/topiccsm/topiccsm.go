package topiccsm

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/dispatcher"
)

// T implements a consumer request dispatch tier responsible for a particular
// topic. It receives requests on the `Requests()` channel and replies with
// messages received on `Messages()` channel. If there has been no message
// received for `Config.Consumer.LongPollingTimeout` then a timeout error is
// sent to the requests' reply channel.
//
// implements `dispatcher.Tier`.
// implements `multiplexer.Out`.
type T struct {
	actorID       *actor.ID
	cfg           *config.T
	group         string
	topic         string
	lifespanCh    chan<- *T
	assignmentsCh chan []int32
	requestsCh    chan dispatcher.Request
	messagesCh    chan *consumer.Message
	wg            sync.WaitGroup
}

// Creates a topic consumer instance. It should be explicitly started in
// accordance with the `dispatcher.Tier` contract.
func New(namespace *actor.ID, group, topic string, cfg *config.T, lifespanCh chan<- *T) *T {
	return &T{
		actorID:       namespace.NewChild(fmt.Sprintf("T:%s", topic)),
		cfg:           cfg,
		group:         group,
		topic:         topic,
		lifespanCh:    lifespanCh,
		assignmentsCh: make(chan []int32),
		requestsCh:    make(chan dispatcher.Request, cfg.Consumer.ChannelBufferSize),
		messagesCh:    make(chan *consumer.Message),
	}
}

// Topic returns the topic name this topic consumer is responsible for.
func (tc *T) Topic() string {
	return tc.topic
}

// implements `multiplexer.Out`
func (tc *T) Messages() chan<- *consumer.Message {
	return tc.messagesCh
}

// implements `dispatcher.Tier`.
func (tc *T) Key() string {
	return tc.topic
}

// implements `dispatcher.Tier`.
func (tc *T) Requests() chan<- dispatcher.Request {
	return tc.requestsCh
}

// implements `dispatcher.Tier`.
func (tc *T) Start(stoppedCh chan<- dispatcher.Tier) {
	actor.Spawn(tc.actorID, &tc.wg, func() {
		defer func() { stoppedCh <- tc }()
		tc.run()
	})
}

// implements `dispatcher.Tier`.
func (tc *T) Stop() {
	close(tc.requestsCh)
	tc.wg.Wait()
}

func (tc *T) run() {
	tc.lifespanCh <- tc
	defer func() {
		tc.lifespanCh <- tc
	}()

	timeoutErr := consumer.ErrRequestTimeout(fmt.Errorf("long polling timeout"))
	timeoutResult := dispatcher.Response{Err: timeoutErr}
	for consumeReq := range tc.requestsCh {
		requestAge := time.Now().UTC().Sub(consumeReq.Timestamp)
		ttl := tc.cfg.Consumer.LongPollingTimeout - requestAge
		// The request has been waiting in the buffer for too long. If we
		// reply with a fetched message, then there is a good chance that the
		// client won't receive it due to the client HTTP timeout. Therefore
		// we reject the request to avoid message loss.
		if ttl <= 0 {
			consumeReq.ResponseCh <- timeoutResult
			continue
		}

		select {
		case msg := <-tc.messagesCh:
			consumeReq.ResponseCh <- dispatcher.Response{Msg: msg}
		case <-time.After(ttl):
			consumeReq.ResponseCh <- timeoutResult
		}
	}
}

func (tc *T) String() string {
	return tc.actorID.String()
}
