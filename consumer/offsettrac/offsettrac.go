package offsettrac

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
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
	actorID      *actor.ID
	offerTimeout time.Duration
	offset       offsetmgr.Offset
	ackRanges    []ackRange
	offers       []offer
}

// SparseAcks2Str returns human readable representation of sparsely committed
// ranges encoded in the specified offset metadata.
func SparseAcks2Str(offset offsetmgr.Offset) string {
	var buf bytes.Buffer
	ackRanges, _ := decodeAckRanges(offset.Val, offset.Meta)
	for i, ar := range ackRanges {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(ar.from-offset.Val, 10))
		buf.WriteString("-")
		buf.WriteString(strconv.FormatInt(ar.to-offset.Val, 10))
	}
	return buf.String()
}

// New creates a new offset tracker instance.
func New(actorID *actor.ID, offset offsetmgr.Offset, offerTimeout time.Duration) *T {
	ot := T{
		actorID:      actorID,
		offerTimeout: offerTimeout,
		offset:       offset,
	}
	var err error
	ot.ackRanges, err = decodeAckRanges(offset.Val, offset.Meta)
	if err != nil {
		ot.ackRanges = nil
		ot.offset.Meta = ""
		log.Errorf("<%v> failed to decode ack ranges: %v, err=%+v", ot.actorID, offset, err)
	}
	return &ot
}

// OnOffered should be called when a message has been offered to a consumer. It
// returns the total number of offered messages. It is callers responsibility
// to ensure that the number of offered message does not grow too large.
func (ot *T) OnOffered(msg consumer.Message) int {
	offersCount := len(ot.offers)
	// Ignore messages that has already been acknowledged
	if ot.IsAcked(msg) {
		return offersCount
	}
	// Even though the logic of this function allows offering in any order,
	// this special case exists to expedite the most common real life usage
	// pattern where messages are offered in their offset order.
	if offersCount == 0 || msg.Offset > ot.offers[offersCount-1].offset {
		ot.offers = append(ot.offers, ot.newOffer(msg))
		return offersCount + 1
	}
	// Find the spot where the message should be inserted to keep the offer
	// list sorted by offsets.
	i := sort.Search(offersCount, func(i int) bool {
		return ot.offers[i].msg.Offset >= msg.Offset
	})
	// Ignore if the message is already in the list. It is either a retry, then
	// the offer has already been updated in a NextRetry call, or a mistake of
	// the caller, then we just do not care.
	if ot.offers[i].msg.Offset == msg.Offset {
		return offersCount
	}
	// Insert the message to the offer list keeping it sorted by offsets.
	ot.offers = append(ot.offers, offer{})
	copy(ot.offers[i+1:], ot.offers[i:offersCount])
	ot.offers[i] = ot.newOffer(msg)
	return offersCount + 1
}

// OnAcked should be called when a message has been acknowledged by a consumer.
// It returns an offset to be submitted and a total number of offered messages.
func (ot *T) OnAcked(offset int64) (offsetmgr.Offset, int) {
	ot.removeOffer(offset)
	ot.updateAckRanges(offset)
	var err error
	ot.offset.Meta, err = encodeAckRanges(ot.offset.Val, ot.ackRanges)
	if err != nil {
		log.Errorf("<%s> failed to encode ack ranges: err=%+v", ot.actorID, err)
	}
	return ot.offset, len(ot.offers)
}

func (ot *T) removeOffer(offset int64) {
	offersCount := len(ot.offers)
	i := sort.Search(offersCount, func(i int) bool {
		return ot.offers[i].msg.Offset >= offset
	})
	if i >= offersCount || ot.offers[i].msg.Offset != offset {
		log.Errorf("<%s> unknown message acked: offset=%d", ot.actorID, offset)
		return
	}
	offersCount -= 1
	copy(ot.offers[i:offersCount], ot.offers[i+1:])
	ot.offers[offersCount].msg = consumer.Message{} // Makes it subject for garbage collection.
	ot.offers = ot.offers[:offersCount]
}

// IsAcked tells if a message has already been acknowledged.
func (ot *T) IsAcked(msg consumer.Message) bool {
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
// number. If there are no messages to be retried then nil is returned.
func (ot *T) NextRetry() (consumer.Message, int, bool) {
	now := time.Now()
	return ot.nextRetry(now)
}
func (ot *T) nextRetry(now time.Time) (consumer.Message, int, bool) {
	for i := range ot.offers {
		o := &ot.offers[i]
		if o.deadline.Before(now) {
			o.deadline = now.Add(ot.offerTimeout)
			o.retryNo += 1
			return o.msg, o.retryNo, true
		}
		// When we reach the first never retried offer with a deadline set in
		// the future it is guaranteed that all further offers in the list have
		// not expired yet. BUT it is only true if messages are offered in the
		// order of their offsets. Which is indeed how partition consumer is
		// doing it. However the offset tracker API allows any order. So the
		// following logic is not valid in general case.
		if o.retryNo == 0 {
			return consumer.Message{}, -1, false
		}
	}
	return consumer.Message{}, -1, false
}

// ShouldWait4Ack tells whether there are messages that acknowledgments are
// worth waiting for, and if so returns a timeout for that wait.
func (ot *T) ShouldWait4Ack() (bool, time.Duration) {
	now := time.Now()
	return ot.shouldWait4Ack(now)
}
func (ot *T) shouldWait4Ack(now time.Time) (bool, time.Duration) {
	for _, o := range ot.offers {
		if o.deadline.After(now) {
			return true, o.deadline.Sub(now)
		}
	}
	// Complain about all not acknowledged messages before giving up.
	for _, o := range ot.offers {
		log.Errorf("<%s> not acknowledged: offset=%d", ot.actorID, o.msg.Offset)
	}
	return false, 0
}

func (ot *T) updateAckRanges(offset int64) {
	ackRangesCount := len(ot.ackRanges)
	if offset < ot.offset.Val {
		log.Errorf("<%s> ack before committed: offset=%d", ot.actorID, offset)
		return
	}
	if offset == ot.offset.Val {
		if ackRangesCount > 0 && offset == ot.ackRanges[0].from-1 {
			ot.offset.Val = ot.ackRanges[0].to
			ot.ackRanges = ot.ackRanges[1:]
			return
		}
		ot.offset.Val += 1
		return
	}
	for i := range ot.ackRanges {
		if offset < ot.ackRanges[i].from {
			if offset == ot.ackRanges[i].from-1 {
				ot.ackRanges[i].from -= 1
				return
			}
			ot.ackRanges = append(ot.ackRanges, ackRange{})
			copy(ot.ackRanges[i+1:], ot.ackRanges[i:ackRangesCount])
			ot.ackRanges[i] = newAckRange(offset)
			return
		}
		if offset < ot.ackRanges[i].to {
			log.Errorf("<%s> duplicate ack: offset=%d", ot.actorID, offset)
			return
		}
		if offset == ot.ackRanges[i].to {
			if ackRangesCount > i+1 && offset == ot.ackRanges[i+1].from-1 {
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
	ot.ackRanges = append(ot.ackRanges, newAckRange(offset))
	return
}

func (ot *T) newOffer(msg consumer.Message) offer {
	return offer{msg, msg.Offset, 0, time.Now().Add(ot.offerTimeout)}
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
	b[0] = base64EncodeMap[fromDlt>>6&0x3F]
	b[1] = base64EncodeMap[fromDlt&0x3F]

	toDlt := ar.to - ar.from
	if toDlt > maxDelta {
		return errors.Errorf("range `to` delta too big: %d", toDlt)
	}
	b[2] = base64EncodeMap[toDlt>>6&0x3F]
	b[3] = base64EncodeMap[toDlt&0x3F]
	return nil
}

func (ar *ackRange) decode(base int64, b []byte) error {
	if len(b) < 4 {
		return errors.Errorf("too few chars: %d", len(b))
	}
	var err error
	if ar.from, err = decodeDelta(base, b); err != nil {
		return errors.Wrap(err, "bad `from` boundary")
	}
	if ar.to, err = decodeDelta(ar.from, b[2:]); err != nil {
		return errors.Wrap(err, "bad `to` boundary")
	}
	if ar.from >= ar.to || ar.from <= 0 {
		return errors.Errorf("invalid range: %v", *ar)
	}
	return nil
}

func decodeDelta(base int64, b []byte) (int64, error) {
	var chr0, chr1 byte
	if chr0 = base64DecodeMap[b[0]]; chr0 == 0xFF {
		return -1, errors.Errorf("invalid char: %c", b[0])
	}
	if chr1 = base64DecodeMap[b[1]]; chr1 == 0xFF {
		return -1, errors.Errorf("invalid char: %c", b[1])
	}
	return base + (int64(chr0)<<6 | int64(chr1)), nil
}

type offer struct {
	msg      consumer.Message
	offset   int64
	retryNo  int
	deadline time.Time
}

type ackRange struct {
	from, to int64
}
