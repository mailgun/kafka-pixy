package offsettrac

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/pkg/errors"
	. "gopkg.in/check.v1"
)

var _ = Suite(&OffsetTrackerSuite{})

type OffsetTrackerSuite struct {
}

func Test(t *testing.T) {
	TestingT(t)
}

// Acknowledged offsets are properly reflected in ackRanges.
func (s *OffsetTrackerSuite) TestOnAckedRanges(c *C) {
	ot := New(offsetmgr.Offset{Val: 300}, -1)
	for i, tc := range []struct {
		offset            int64
		ranges            string
		skipSymmetryCheck bool
	}{
		/*  0 */ {offset: 299, ranges: "300:"},
		/*  1 */ {offset: 300, ranges: "301:"},
		/*  2 */ {offset: 302, ranges: "301:302-303"},
		/*  3 */ {offset: 310, ranges: "301:302-303,310-311"},
		/*  4 */ {offset: 311, ranges: "301:302-303,310-312"},
		/*  5 */ {offset: 307, ranges: "301:302-303,307-308,310-312"},
		/*  6 */ {offset: 305, ranges: "301:302-303,305-306,307-308,310-312"},
		/*  7 */ {offset: 304, ranges: "301:302-303,304-306,307-308,310-312"},
		/*  8 */ {offset: 305, ranges: "301:302-303,304-306,307-308,310-312"},
		/*  9 */ {offset: 308, ranges: "301:302-303,304-306,307-309,310-312"},
		/* 10 */ {offset: 303, ranges: "301:302-306,307-309,310-312"},
		/* 11 */ {offset: 309, ranges: "301:302-306,307-312"},
		/* 12 */ {offset: 306, ranges: "301:302-312"},
		/* 13 */ {offset: 301, ranges: "312:"},
		/* 14 */ {offset: 398, ranges: "312:398-399"},
		/* 15 */ {offset: 399, ranges: "312:398-400"},
		//        delta is greater then 4095, so large deltas cannot be
		//        represented in the selected encoding algorithm. They must
		//        never happen in production, but to be thorough we need to
		//        handle this case somehow, so we just drop all range info.
		/* 16 */ {offset: 4496, ranges: "312:", skipSymmetryCheck: true},
		/* 17 */ {offset: 4495, ranges: "312:398-400,4495-4497"},
	} {
		// When
		offset, _ := ot.OnAcked(&consumer.Message{Offset: tc.offset})
		ot2 := New(offset, -1)

		// Then
		c.Assert(offsetToStr(offset), Equals, tc.ranges, Commentf("case: %d", i))
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
		/* 1 */ {"ab@d", errors.New("invalid char: @")},
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
		ot := New(tc.initial, -1)

		// Then
		c.Assert(ot.offset, Equals, tc.adjusted, Commentf("case: %d", i))
	}
}

func (s *OffsetTrackerSuite) TestIsAcked(c *C) {
	meta, _ := encodeAckRanges(301, []ackRange{
		{302, 305}, {307, 309}, {310, 313}})
	offset := offsetmgr.Offset{301, meta}
	ot := New(offset, -1)
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
		c.Assert(ot.IsAcked(&consumer.Message{Offset: tc.offset}),
			Equals, tc.isAcked, Commentf("case: %d", i))
	}
}

func offsetToStr(offset offsetmgr.Offset) string {
	var buf bytes.Buffer
	buf.WriteString(strconv.FormatInt(offset.Val, 10))
	buf.WriteString(":")
	ackRanges, _ := decodeAckRanges(offset.Val, offset.Meta)
	for i, ar := range ackRanges {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString(strconv.FormatInt(ar.from, 10))
		buf.WriteString("-")
		buf.WriteString(strconv.FormatInt(ar.to, 10))
	}
	return buf.String()
}
