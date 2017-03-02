package offsettrac

import (
	"time"

	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

const (
	base64EncodeMap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	maxDelta        = 0xFFF
)

var (
	base64DecodeMap [256]byte
)

func init() {
	for i := range base64DecodeMap {
		base64DecodeMap[i] = 0xFF
	}
	for i := range base64EncodeMap {
		base64DecodeMap[base64EncodeMap[i]] = byte(i)
	}
}

// T represents an entity that tracks offered and acknowledged messages and
// maintains offset data for the current state.
type T struct {
	offerTimeout time.Duration
	offset       offsetmgr.Offset
	ackRanges    []ackRange
	msgEntries   []msgEntry
}

// New creates a new offset tracker instance.
func New(offset offsetmgr.Offset, offerTimeout time.Duration) *T {
	ot := T{
		offerTimeout: offerTimeout,
		offset:       offset,
	}
	var err error
	ot.ackRanges, err = decodeAckRanges(offset.Val, offset.Meta)
	if err != nil {
		ot.ackRanges = nil
		ot.offset.Meta = ""
		log.Errorf("failed to decode ack ranges: %v, err=%+v", offset, err)
	}
	return &ot
}

// OnOffered should be called when a message has been offered to a consumer. It
// returns an offset to be submitted and a total number of pending messages.
func (ot *T) OnOffered(msg *consumer.Message) (offsetmgr.Offset, int) {
	return ot.offset, 1
}

// OnAcked should be called when a message has been acknowledged by a consumer.
// It returns an offset to be submitted and a total number of pending messages.
func (ot *T) OnAcked(msg *consumer.Message) (offsetmgr.Offset, int) {
	msgCount := len(ot.msgEntries)
	ot.updateAckRanges(msg.Offset)
	var err error
	ot.offset.Meta, err = encodeAckRanges(ot.offset.Val, ot.ackRanges)
	if err != nil {
		log.Errorf("failed to encode ack ranges: err=%+v", err)
	}
	return ot.offset, msgCount
}

// IsAcked tells if a message has already been acknowledged.
func (ot *T) IsAcked(msg *consumer.Message) bool {
	if msg.Offset < ot.offset.Val {
		return true
	}
	for _, ar := range ot.ackRanges {
		if msg.Offset < ar.from {
			return false
		}
		if msg.Offset < ar.to {
			return true
		}
	}
	return false
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

func (ot *T) updateAckRanges(ackedOffset int64) {
	ackRangesCount := len(ot.ackRanges)
	if ackedOffset < ot.offset.Val {
		log.Errorf("ack before committed: %d", ackedOffset)
		return
	}
	if ackedOffset == ot.offset.Val {
		if ackRangesCount > 0 && ackedOffset == ot.ackRanges[0].from-1 {
			ot.offset.Val = ot.ackRanges[0].to
			ot.ackRanges = ot.ackRanges[1:]
			return
		}
		ot.offset.Val += 1
		return
	}
	for i := range ot.ackRanges {
		if ackedOffset < ot.ackRanges[i].from {
			if ackedOffset == ot.ackRanges[i].from-1 {
				ot.ackRanges[i].from -= 1
				return
			}
			ot.ackRanges = append(ot.ackRanges, ackRange{})
			copy(ot.ackRanges[i+1:], ot.ackRanges[i:ackRangesCount])
			ot.ackRanges[i] = newAckRange(ackedOffset)
			return
		}
		if ackedOffset < ot.ackRanges[i].to {
			log.Errorf("duplicate ack: %d", ackedOffset)
			return
		}
		if ackedOffset == ot.ackRanges[i].to {
			if ackRangesCount > i+1 && ackedOffset == ot.ackRanges[i+1].from-1 {
				ot.ackRanges[i+1].from = ot.ackRanges[i].from
				ackRangesCount -= 1
				copy(ot.ackRanges[i:ackRangesCount], ot.ackRanges[i+1:])
				ot.ackRanges = ot.ackRanges[:ackRangesCount]
				return
			}
			ot.ackRanges[i].to += 1
			return
		}
	}
	ot.ackRanges = append(ot.ackRanges, newAckRange(ackedOffset))
	return
}

func encodeAckRanges(base int64, ackRanges []ackRange) (string, error) {
	ackRangesCount := len(ackRanges)
	if ackRangesCount == 0 {
		return "", nil
	}
	buf := make([]byte, ackRangesCount*4)
	for i, ar := range ackRanges {
		if err := ar.encode(base, buf[i*4:]); err != nil {
			return "", errors.Wrapf(err, "unsupported range: %+v", ar)
		}
		base = ar.to
	}
	return string(buf), nil
}

func decodeAckRanges(base int64, encoded string) ([]ackRange, error) {
	if encoded == "" {
		return nil, nil
	}
	if len(encoded)&0x3 != 0 {
		return nil, errors.Errorf("too few chars: %d", len(encoded))
	}
	ackRanges := make([]ackRange, len(encoded)/4)
	buf := []byte(encoded)
	for i := range ackRanges {
		if err := ackRanges[i].decode(base, buf[i*4:]); err != nil {
			return nil, errors.Wrapf(err, "bad encoding: %s", encoded)
		}
		base = ackRanges[i].to
	}
	return ackRanges, nil
}

func newAckRange(offset int64) ackRange {
	return ackRange{offset, offset + 1}
}

func (ar *ackRange) encode(base int64, b []byte) error {
	fromDlt := ar.from - base
	if fromDlt > maxDelta {
		return errors.Errorf("range `from` delta too big: %d", fromDlt)
	}
	toDlt := ar.to - ar.from
	if toDlt > maxDelta {
		return errors.Errorf("range `to` delta too big: %d", toDlt)
	}

	val := fromDlt<<12 | toDlt
	b[0] = base64EncodeMap[val>>18&0x3F]
	b[1] = base64EncodeMap[val>>12&0x3F]
	b[2] = base64EncodeMap[val>>6&0x3F]
	b[3] = base64EncodeMap[val&0x3F]
	return nil
}

func (ar *ackRange) decode(base int64, b []byte) error {
	if len(b) < 4 {
		return errors.Errorf("too few chars: %d", len(b))
	}
	var val int64
	for i := 0; i < 4; i++ {
		chr := base64DecodeMap[b[i]]
		if chr == 0xFF {
			return errors.Errorf("invalid char: %c", b[i])
		}
		val = val<<6 | int64(chr)
	}
	ar.from = base + val>>12
	ar.to = ar.from + val&0xFFF
	if ar.from >= ar.to || ar.from <= 0 {
		return errors.Errorf("invalid range: %v", *ar)
	}
	return nil
}

type msgEntry struct {
	msg     *consumer.Message
	retryNo int
	retryAt time.Time
}

type ackRange struct {
	from, to int64
}
