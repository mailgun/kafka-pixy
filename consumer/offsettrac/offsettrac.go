package offsettrac

import (
	"time"

	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
)

// T represents an entity that tracks offered and acknowledged messages and
// maintains offset data for the current state.
type T struct {
	offerTimeout time.Duration
	offset       offsetmgr.Offset
}

// New creates a new offset tracker instance.
func New(offset offsetmgr.Offset, offerTimeout time.Duration) *T {
	return &T{
		offerTimeout: offerTimeout,
		offset:       offset,
	}
}

// OnOffered should be called when a message has been offered to a consumer. It
// returns an offset to be submitted and a total number of pending messages.
func (ot *T) OnOffered(msg *consumer.Message) (offsetmgr.Offset, int) {
	return ot.offset, 1
}

// OnAcked should be called when a message has been acknowledged by a consumer.
// It returns an offset to be submitted and a total number of pending messages.
func (ot *T) OnAcked(msg *consumer.Message) (offsetmgr.Offset, int) {
	ot.offset = offsetmgr.Offset{Val: msg.Offset + 1, Meta: ""}
	return ot.offset, 0
}

// NextRetry returns a next message to be retried along with the retry attempt
// number. If there is no message to be retried then nil is returned.
func (ot *T) NextRetry() (*consumer.Message, int) {
	return nil, 0
}

// ShouldWait4Ack tells whether there are messages that acknowledgments are
// worth waiting for, and if so returns a timeout for that wait.
func (ot *T) ShouldWait4Ack() (bool, time.Duration) {
	return false, -1
}
