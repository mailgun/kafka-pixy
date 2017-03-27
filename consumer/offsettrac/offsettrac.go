package offsettrac

import (
	"bytes"
	"sort"
	"strconv"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

const (
	base64EncodeMap = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
)

var (
	base64DecodeMap [256]byte
	decodeShifts    [64/5 + 1]uint
)

func init() {
	for i := range base64DecodeMap {
		base64DecodeMap[i] = 0xFF
	}
	for i := range base64EncodeMap {
		base64DecodeMap[base64EncodeMap[i]] = byte(i)
	}
	for i := range decodeShifts {
		decodeShifts[i] = uint(5 * i)
	}
}

// T represents an entity that tracks offered and acknowledged messages and
// maintains offset data for the current state.
type T struct {
	actorID      *actor.ID
	offerTimeout time.Duration
	offset       offsetmgr.Offset
	ackedRanges  []offsetRange
	offers       []offer
}

// SparseAcks2Str returns human readable representation of sparsely committed
// ranges encoded in the specified offset metadata.
func SparseAcks2Str(offset offsetmgr.Offset) string {
	var buf bytes.Buffer
	ackedRanges, _ := decodeAckedRanges(offset.Val, offset.Meta)
	for i, ar := range ackedRanges {
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
	ot.ackedRanges, err = decodeAckedRanges(offset.Val, offset.Meta)
	if err != nil {
		ot.ackedRanges = nil
		ot.offset.Meta = ""
		log.Errorf("<%v> bad sparse acks: %v, err=%+v", ot.actorID, offset, err)
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
	offerMissing := !ot.removeOffer(offset)
	duplicateAck := !ot.updateAckedRanges(offset)
	if offerMissing || duplicateAck {
		log.Errorf("<%s> bad ack: offerMissing=%t, duplicateAck=%t",
			ot.actorID, offerMissing, duplicateAck)
	}
	if !duplicateAck {
		ot.offset.Meta = encodeAckedRanges(ot.offset.Val, ot.ackedRanges)
	}
	return ot.offset, len(ot.offers)
}

func (ot *T) removeOffer(offset int64) bool {
	offersCount := len(ot.offers)
	i := sort.Search(offersCount, func(i int) bool {
		return ot.offers[i].msg.Offset >= offset
	})
	if i >= offersCount || ot.offers[i].msg.Offset != offset {
		return false
	}
	offersCount -= 1
	copy(ot.offers[i:offersCount], ot.offers[i+1:])
	ot.offers[offersCount].msg = consumer.Message{} // Makes it subject for garbage collection.
	ot.offers = ot.offers[:offersCount]
	return true
}

// IsAcked tells if a message has already been acknowledged.
func (ot *T) IsAcked(msg consumer.Message) bool {
	if msg.Offset < ot.offset.Val {
		return true
	}
	for _, ar := range ot.ackedRanges {
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
	return ot.nextRetry(time.Now())
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
	return ot.shouldWait4Ack(time.Now())
}
func (ot *T) shouldWait4Ack(now time.Time) (bool, time.Duration) {
	for _, o := range ot.offers {
		if o.deadline.After(now) {
			timeout := o.deadline.Sub(now)
			log.Infof("<%s> waiting for acks: count=%d, offset=%d, timeout=%v",
				ot.actorID, len(ot.offers), o.offset, timeout)
			return true, timeout
		}
	}
	// Complain about all not acknowledged messages before giving up.
	for _, o := range ot.offers {
		log.Errorf("<%s> not acked: offset=%d", ot.actorID, o.msg.Offset)
	}
	return false, 0
}

func (ot *T) updateAckedRanges(offset int64) bool {
	ackedRangesCount := len(ot.ackedRanges)
	if offset < ot.offset.Val {
		return false
	}
	if offset == ot.offset.Val {
		if ackedRangesCount > 0 && offset == ot.ackedRanges[0].from-1 {
			ot.offset.Val = ot.ackedRanges[0].to
			ot.ackedRanges = ot.ackedRanges[1:]
			return true
		}
		ot.offset.Val += 1
		return true
	}
	for i := range ot.ackedRanges {
		if offset < ot.ackedRanges[i].from {
			if offset == ot.ackedRanges[i].from-1 {
				ot.ackedRanges[i].from -= 1
				return true
			}
			ot.ackedRanges = append(ot.ackedRanges, offsetRange{})
			copy(ot.ackedRanges[i+1:], ot.ackedRanges[i:ackedRangesCount])
			ot.ackedRanges[i] = newOffsetRange(offset)
			return true
		}
		if offset < ot.ackedRanges[i].to {
			return false
		}
		if offset == ot.ackedRanges[i].to {
			if ackedRangesCount > i+1 && offset == ot.ackedRanges[i+1].from-1 {
				ot.ackedRanges[i+1].from = ot.ackedRanges[i].from
				ackedRangesCount -= 1
				copy(ot.ackedRanges[i:ackedRangesCount], ot.ackedRanges[i+1:])
				ot.ackedRanges = ot.ackedRanges[:ackedRangesCount]
				return true
			}
			ot.ackedRanges[i].to += 1
			return true
		}
	}
	ot.ackedRanges = append(ot.ackedRanges, newOffsetRange(offset))
	return true
}

func (ot *T) newOffer(msg consumer.Message) offer {
	return offer{msg, msg.Offset, 0, time.Now().Add(ot.offerTimeout)}
}

func encodeAckedRanges(base int64, ackedRanges []offsetRange) string {
	ackedRangesCount := len(ackedRanges)
	if ackedRangesCount == 0 {
		return ""
	}
	buf := make([]byte, 0, ackedRangesCount*4)
	for _, ar := range ackedRanges {
		buf = ar.encode(base, buf)
		base = ar.to
	}
	return string(buf)
}

func decodeAckedRanges(base int64, encoded string) ([]offsetRange, error) {
	if encoded == "" {
		return nil, nil
	}
	ackedRanges := make([]offsetRange, 0, len(encoded)/2)
	var err error
	buf := []byte(encoded)
	for i := 0; len(buf) > 0; i++ {
		var ar offsetRange
		if buf, err = ar.decode(base, buf); err != nil {
			return nil, errors.Wrapf(err, "bad encoding: %s", encoded)
		}
		base = ar.to
		ackedRanges = append(ackedRanges, ar)
	}
	return ackedRanges, nil
}

type offsetRange struct {
	from, to int64
}

func newOffsetRange(offset int64) offsetRange {
	return offsetRange{offset, offset + 1}
}

func (or *offsetRange) encode(base int64, buf []byte) []byte {
	buf = encodeInt64(or.from-base, buf)
	buf = encodeInt64(or.to-or.from, buf)
	return buf
}

func (or *offsetRange) decode(base int64, buf []byte) ([]byte, error) {
	delta, buf, err := decodeInt64(buf)
	if err != nil {
		return nil, errors.Wrap(err, "bad `from` boundary")
	}
	or.from = base + delta

	delta, buf, err = decodeInt64(buf)
	if err != nil {
		return nil, errors.Wrap(err, "bad `to` boundary")
	}
	or.to = or.from + delta

	if or.from >= or.to || or.from <= 0 {
		return nil, errors.Errorf("bad range: %v", *or)
	}
	return buf, nil
}

func encodeInt64(n int64, buf []byte) []byte {
	var ci int64
	for ci, n = n&0x1F, n>>5; n > 0; ci, n = n&0x1F, n>>5 {
		buf = append(buf, base64EncodeMap[ci|0x20])
	}
	return append(buf, base64EncodeMap[ci])
}

func decodeInt64(buf []byte) (int64, []byte, error) {
	var n int64
	i := 0
	for ; i < len(buf) && i < len(decodeShifts); i++ {
		ci := base64DecodeMap[buf[i]]
		if ci == 0xFF {
			return -1, nil, errors.Errorf("bad char: %c", buf[i])
		}
		n |= int64(ci) & 0x1F << decodeShifts[i]
		if ci&0x20 == 0 {
			return n, buf[i+1:], nil
		}
	}
	return -1, nil, errors.Errorf("bad sequence: %s", string(buf[:i]))
}

type offer struct {
	msg      consumer.Message
	offset   int64
	retryNo  int
	deadline time.Time
}
