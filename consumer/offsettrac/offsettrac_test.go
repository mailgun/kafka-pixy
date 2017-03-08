package offsettrac

import (
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/pkg/errors"
	. "gopkg.in/check.v1"
)

type action int

const (
	doOff action = iota
	doAck
)

var _ = Suite(&OffsetTrackerSuite{})

type OffsetTrackerSuite struct {
	ns *actor.ID
}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *OffsetTrackerSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

// Acknowledged offsets are properly reflected in ackRanges.
func (s *OffsetTrackerSuite) TestOnAckedRanges(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	for i, tc := range []struct {
		offset            int64
		committed         int64
		ranges            string
		skipSymmetryCheck bool
	}{
		/*  0 */ {offset: 299, committed: 300, ranges: ""},
		/*  1 */ {offset: 300, committed: 301, ranges: ""},
		/*  2 */ {offset: 302, committed: 301, ranges: "1-2"},
		/*  3 */ {offset: 310, committed: 301, ranges: "1-2,9-10"},
		/*  4 */ {offset: 311, committed: 301, ranges: "1-2,9-11"},
		/*  5 */ {offset: 307, committed: 301, ranges: "1-2,6-7,9-11"},
		/*  6 */ {offset: 305, committed: 301, ranges: "1-2,4-5,6-7,9-11"},
		/*  7 */ {offset: 304, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		//        Acking the an acked offset makes no difference
		/*  8 */ {offset: 305, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		/*  9 */ {offset: 308, committed: 301, ranges: "1-2,3-5,6-8,9-11"},
		/* 10 */ {offset: 303, committed: 301, ranges: "1-5,6-8,9-11"},
		/* 11 */ {offset: 309, committed: 301, ranges: "1-5,6-11"},
		/* 12 */ {offset: 306, committed: 301, ranges: "1-11"},
		/* 13 */ {offset: 301, committed: 312, ranges: ""},
		/* 14 */ {offset: 398, committed: 312, ranges: "86-87"},
		/* 15 */ {offset: 399, committed: 312, ranges: "86-88"},
		//        delta is greater then 4095, so large deltas cannot be
		//        represented in the selected encoding algorithm. That must
		//        never happen in production, but to be thorough we need to
		//        handle this case somehow, so we just drop all range info.
		/* 16 */ {offset: 4496, committed: 312, ranges: "", skipSymmetryCheck: true},
		/* 17 */ {offset: 4495, committed: 312, ranges: "86-88,4183-4185"},
	} {
		// When
		offset, _ := ot.OnAcked(tc.offset)
		ot2 := New(s.ns, offset, -1)

		// Then
		c.Assert(offset.Val, Equals, tc.committed, Commentf("case: %d", i))
		c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case: %d", i))
		if tc.skipSymmetryCheck {
			continue
		}
		// Both nil and empty slice ack ranges become nil when going through
		// the encode/decode circle.
		if ot.ackRanges == nil || len(ot.ackRanges) == 0 {
			c.Assert(ot2.ackRanges, IsNil, Commentf("case: %d", i))
		} else {
			c.Assert(ot2.ackRanges, DeepEquals, ot.ackRanges, Commentf("case: %d", i))
		}
	}
}

func (s *OffsetTrackerSuite) TestAckRangeEncodeDecode(c *C) {
	encoded := make([]byte, 4)
	for i, tc := range []struct {
		base int64
		ar   ackRange
	}{
		/* 0 */ {0, ackRange{1, 2}},
		/* 1 */ {100, ackRange{101, 102}},
		/* 2 */ {10000, ackRange{14095, 14100}},
		/* 3 */ {10000, ackRange{10001, 14096}},
	} {
		var decoded ackRange

		// When
		err := tc.ar.encode(tc.base, encoded)
		c.Assert(err, IsNil, Commentf("case: %d", i))
		decoded.decode(tc.base, encoded)

		// Then
		c.Assert(decoded, Equals, tc.ar, Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestAckRangeEncodeError(c *C) {
	encoded := make([]byte, 4)
	for i, tc := range []struct {
		base int64
		ar   ackRange
		err  error
	}{
		/* 0 */ {10000, ackRange{14096, 14097},
			errors.New("range `from` delta too big: 4096")},
		/* 1 */ {10000, ackRange{10001, 14097},
			errors.New("range `to` delta too big: 4096")},
	} {
		// When
		err := tc.ar.encode(tc.base, encoded)

		// Then
		c.Assert(err.Error(), Equals, tc.err.Error(), Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestAckRangeDecodeError(c *C) {
	var ar ackRange
	for i, tc := range []struct {
		encoded string
		err     error
	}{
		/* 0 */ {"abc", errors.New("too few chars: 3")},
		/* 1 */ {"ab@d", errors.New("bad `to` boundary: invalid char: @")},
		/* 2 */ {"ABAA", errors.New("invalid range: {10001 10001}")},
	} {
		// When
		err := ar.decode(10000, []byte(tc.encoded))

		// Then
		c.Assert(err.Error(), Equals, tc.err.Error(), Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestNewOffsetAdjusted(c *C) {
	for i, tc := range []struct {
		initial  offsetmgr.Offset
		adjusted offsetmgr.Offset
	}{
		/* 0 */ {
			offsetmgr.Offset{1000, ""},
			offsetmgr.Offset{1000, ""},
		},
		/* 1 */ {
			offsetmgr.Offset{1000, "abra1234+/PS"},
			offsetmgr.Offset{1000, "abra1234+/PS"},
		},
		/* 2 */ {
			offsetmgr.Offset{1000, "abra1234+/P"},
			offsetmgr.Offset{1000, ""},
		},
		/* 2 */ {
			offsetmgr.Offset{1000, "abra1234+/@S"},
			offsetmgr.Offset{1000, ""},
		},
	} {
		// When
		ot := New(s.ns, tc.initial, -1)

		// Then
		c.Assert(ot.offset, Equals, tc.adjusted, Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestIsAcked(c *C) {
	meta, _ := encodeAckRanges(301, []ackRange{
		{302, 305}, {307, 309}, {310, 313}})
	offset := offsetmgr.Offset{301, meta}
	ot := New(s.ns, offset, -1)
	for i, tc := range []struct {
		offset  int64
		isAcked bool
	}{
		/*  0 */ {offset: 299, isAcked: true},
		/*  1 */ {offset: 300, isAcked: true},
		/*  2 */ {offset: 301, isAcked: false},
		/*  3 */ {offset: 302, isAcked: true},
		/*  4 */ {offset: 303, isAcked: true},
		/*  5 */ {offset: 304, isAcked: true},
		/*  6 */ {offset: 305, isAcked: false},
		/*  7 */ {offset: 306, isAcked: false},
		/*  8 */ {offset: 307, isAcked: true},
		/*  9 */ {offset: 308, isAcked: true},
		/* 10 */ {offset: 309, isAcked: false},
		/* 11 */ {offset: 310, isAcked: true},
		/* 12 */ {offset: 311, isAcked: true},
		/* 13 */ {offset: 312, isAcked: true},
		/* 14 */ {offset: 313, isAcked: false},
		/* 15 */ {offset: 314, isAcked: false},
	} {
		// When/Then
		c.Assert(ot.IsAcked(consumer.Message{Offset: tc.offset}),
			Equals, tc.isAcked, Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestOfferAckLoop(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	for i, tc := range []struct {
		act       action
		offset    int64
		count     int
		committed int64
		ranges    string
	}{
		/*  0 */ {act: doOff, offset: 298, count: 0},
		/*  1 */ {act: doOff, offset: 299, count: 0},
		//        Test offering in arbitrary order, even though it never
		//        happens in real life.
		/*  2 */ {act: doOff, offset: 301, count: 1},
		/*  3 */ {act: doOff, offset: 300, count: 2},
		/*  4 */ {act: doOff, offset: 304, count: 3},
		/*  5 */ {act: doOff, offset: 302, count: 4},
		/*  6 */ {act: doOff, offset: 303, count: 5},
		//        And a bit of regular sequential offering
		/*  7 */ {act: doOff, offset: 305, count: 6},
		/*  8 */ {act: doOff, offset: 306, count: 7},
		//        Ack some messages
		/*  9 */ {act: doAck, offset: 307, count: 7, committed: 300, ranges: "7-8"},
		/* 10 */ {act: doAck, offset: 299, count: 7, committed: 300, ranges: "7-8"},
		/* 11 */ {act: doAck, offset: 303, count: 6, committed: 300, ranges: "3-4,7-8"},
		/* 12 */ {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		//        Offer acked again
		/* 13 */ {act: doOff, offset: 298, count: 5},
		/* 14 */ {act: doOff, offset: 305, count: 5},
		//        Offer offered (ignored)
		/* 15 */ {act: doOff, offset: 302, count: 5},
		/* 16 */ {act: doOff, offset: 305, count: 5},
		/* 17 */ {act: doOff, offset: 306, count: 5},
		//        Ack acked (ignored)
		/* 18 */ {act: doAck, offset: 299, count: 5, committed: 301, ranges: "2-3,6-7"},
		/* 19 */ {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		//        Offer/Ack some more
		/* 20 */ {act: doOff, offset: 309, count: 6},
		/* 21 */ {act: doAck, offset: 302, count: 5, committed: 301, ranges: "1-3,6-7"},
		/* 22 */ {act: doAck, offset: 301, count: 4, committed: 304, ranges: "3-4"},
		/* 23 */ {act: doOff, offset: 308, count: 5},
		/* 24 */ {act: doAck, offset: 306, count: 4, committed: 304, ranges: "2-4"},
	} {
		var count int
		// When
		switch tc.act {
		case doOff:
			count = ot.OnOffered(consumer.Message{Offset: tc.offset})
		case doAck:
			var offset offsetmgr.Offset
			offset, count = ot.OnAcked(tc.offset)
			c.Assert(offset.Val, Equals, tc.committed, Commentf("case: %d", i))
			c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case: %d", i))
		}
		// Then
		c.Assert(count, Equals, tc.count, Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestNextRetry(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, 5*time.Second)
	msgs := []consumer.Message{
		{Offset: 301},
		{Offset: 302},
		{Offset: 303},
	}
	begin := time.Now()
	for _, msg := range msgs {
		ot.OnOffered(msg)
	}
	ot.offers[0].deadline = begin.Add(5 * time.Second)
	ot.offers[1].deadline = begin.Add(7 * time.Second)
	ot.offers[2].deadline = begin.Add(9 * time.Second)

	for i, tc := range []struct {
		millis     int
		offset     int64
		retryCount int
	}{
		/*  0 */ {millis: 0},
		/*  1 */ {millis: 1000},
		/*  2 */ {millis: 5000},
		/*  3 */ {millis: 5001, offset: 301, retryCount: 1},
		/*  4 */ {millis: 7000},
		//        The next attempt deadline is counted from the time when
		//        NextRetry returns the message.
		/*  5 */ {millis: 9001, offset: 302, retryCount: 1},
		/*  6 */ {millis: 9003, offset: 303, retryCount: 1},
		/*  8 */ {millis: 10000},
		/*  9 */ {millis: 20000, offset: 301, retryCount: 2},
		/* 10 */ {millis: 20002, offset: 302, retryCount: 2},
		/* 11 */ {millis: 20004, offset: 303, retryCount: 2},
		/* 12 */ {millis: 25000},
		/* 13 */ {millis: 25001, offset: 301, retryCount: 3},
		/* 14 */ {millis: 25002},
		/* 15 */ {millis: 25003, offset: 302, retryCount: 3},
		/* 16 */ {millis: 25004},
		/* 17 */ {millis: 25005, offset: 303, retryCount: 3},
		/* 18 */ {millis: 25006},
	} {
		// When
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		msg, retryCount, ok := ot.nextRetry(now)

		// Then
		if ok {
			c.Assert(msg.Offset, Equals, tc.offset, Commentf("case: %d", i))
			c.Assert(retryCount, Equals, tc.retryCount, Commentf("case: %d", i))
		} else {
			c.Assert(tc.offset, Equals, int64(0), Commentf("case: %d", i))
			c.Assert(retryCount, Equals, -1, Commentf("case: %d", i))
		}
	}
}

func (s *OffsetTrackerSuite) TestShouldWait4Ack(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	msgs := []consumer.Message{
		{Offset: 301},
		{Offset: 302},
		{Offset: 303},
	}
	begin := time.Now()
	for _, msg := range msgs {
		ot.OnOffered(msg)
	}
	ot.offers[0].deadline = begin.Add(3 * time.Second)
	ot.offers[1].deadline = begin.Add(4 * time.Second)
	ot.offers[2].deadline = begin.Add(9 * time.Second)

	for i, tc := range []struct {
		millis  int
		wait    bool
		timeout int
	}{
		/*  0 */ {millis: 0, wait: true, timeout: 3000},
		/*  1 */ {millis: 2998, wait: true, timeout: 2},
		/*  2 */ {millis: 2999, wait: true, timeout: 1},
		/*  3 */ {millis: 3000, wait: true, timeout: 1000},
		/*  4 */ {millis: 3001, wait: true, timeout: 999},
		/*  5 */ {millis: 3999, wait: true, timeout: 1},
		/*  6 */ {millis: 4000, wait: true, timeout: 5000},
		/*  7 */ {millis: 8950, wait: true, timeout: 50},
		/*  8 */ {millis: 8999, wait: true, timeout: 1},
		/*  9 */ {millis: 9000, wait: false, timeout: 0},
		/* 10 */ {millis: 9001, wait: false, timeout: 0},
		/* 11 */ {millis: 9999, wait: false, timeout: 0},
	} {
		// When/Then
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		wait, timeout := ot.shouldWait4Ack(now)
		c.Assert(wait, Equals, tc.wait, Commentf("case: %d", i))
		c.Assert(timeout, Equals, time.Duration(tc.timeout)*time.Millisecond, Commentf("case: %d", i))
	}
}
