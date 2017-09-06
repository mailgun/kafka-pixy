package consumer

import (
	"time"

	"github.com/pkg/errors"
)

const (
	// An event of this type should be sent to the message events channel
	// when the message is offered to a client.
	EvOffered eventType = iota

	// An event of this type should be sent to the message events channel
	// when the message is acknowledged by a client.
	EvAcked
)

var (
	ErrRequestTimeout  = errors.New("long polling timeout")
	ErrUnavailable     = errors.New("service is shutting down")
	ErrTooManyRequests = errors.New("Too many requests. Consider increasing `consumer.channel_buffer_size` (https://github.com/mailgun/kafka-pixy/blob/master/default.yaml#L43)")
)

type T interface {
	// Consume consumes a message from the specified topic on behalf of the
	// specified consumer group. If there are no more new messages in the topic
	// at the time of the request then it will block for
	// `Config.Consumer.LongPollingTimeout`. If no new message is produced during
	// that time, then `ErrRequestTimeout` is returned.
	//
	// Note that during state transitions topic subscribe<->unsubscribe and
	// consumer group register<->deregister the method may return either
	// `ErrBufferOverflow` or `ErrRequestTimeout` even when there are messages
	// available for consumption. In that case the user should back off a bit
	// and then repeat the request.
	Consume(group, topic string) (Message, error)

	// AsyncConsume is an asynchronous counterpart of Consume function. It
	// sends a response down to a buffered channel of the consumer machinery
	// and returns a channel that a response should be expected from.
	AsyncConsume(group, topic string) <-chan Response

	// Stop sends a shutdown signal to all internal goroutines and blocks until
	// they are stopped. It is guaranteed that all last consumed offsets of all
	// consumer groups/topics are committed to Kafka before Consumer stops.
	Stop()
}

// Request
type Request struct {
	Timestamp  time.Time
	Group      string
	Topic      string
	ResponseCh chan Response
}

// Response defines responses returned upstream by the children.
type Response struct {
	Msg Message
	Err error
}

// Message encapsulates a Kafka message returned by the consumer.
type Message struct {
	Key, Value    []byte
	Topic         string
	Partition     int32
	Offset        int64
	Timestamp     time.Time // only set if Kafka is version 0.10+
	HighWaterMark int64
	EventsCh      chan<- Event
}

func NewRequest(group, topic string) Request {
	return Request{
		Timestamp:  time.Now().UTC(),
		Group:      group,
		Topic:      topic,
		ResponseCh: make(chan Response, 1),
	}
}

func Ack(offset int64) Event {
	return Event{EvAcked, offset}
}

type Event struct {
	T      eventType
	Offset int64
}

type eventType int
