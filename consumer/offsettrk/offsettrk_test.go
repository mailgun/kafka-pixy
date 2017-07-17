package offsettrk

import (
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

type action int

const (
	doOfr action = iota
	doAck
)

var _ = Suite(&OffsetTrkSuite{})

type OffsetTrkSuite struct {
	ns *actor.ID
}

func Test(t *testing.T) {
	TestingT(t)
}

func (s *OffsetTrkSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *OffsetTrkSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

// Acknowledged offsets are properly reflected in ackedRanges.
func (s *OffsetTrkSuite) TestOnAckedRanges(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	for i, tc := range []struct {
		acked     int64
		committed int64
		ranges    string
	}{
		0: {acked: 299, committed: 300, ranges: ""},
		1: {acked: 300, committed: 301, ranges: ""},
		2: {acked: 302, committed: 301, ranges: "1-2"},
		3: {acked: 310, committed: 301, ranges: "1-2,9-10"},
		4: {acked: 311, committed: 301, ranges: "1-2,9-11"},
		5: {acked: 307, committed: 301, ranges: "1-2,6-7,9-11"},
		6: {acked: 305, committed: 301, ranges: "1-2,4-5,6-7,9-11"},
		7: {acked: 304, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		// Acking an acked offset makes no difference.
		8:  {acked: 305, committed: 301, ranges: "1-2,3-5,6-7,9-11"},
		9:  {acked: 308, committed: 301, ranges: "1-2,3-5,6-8,9-11"},
		10: {acked: 303, committed: 301, ranges: "1-5,6-8,9-11"},
		11: {acked: 309, committed: 301, ranges: "1-5,6-11"},
		12: {acked: 306, committed: 301, ranges: "1-11"},
		13: {acked: 301, committed: 312, ranges: ""},
		14: {acked: 398, committed: 312, ranges: "86-87"},
		15: {acked: 399, committed: 312, ranges: "86-88"},
		16: {acked: 4496, committed: 312, ranges: "86-88,4184-4185"},
		17: {acked: 0x7FFFFFFFFFFFFFFE, committed: 312, ranges: "86-88,4184-4185,9223372036854775494-9223372036854775495"},
	} {
		// When
		offset, _ := ot.OnAcked(tc.acked)
		ot2 := New(s.ns, offset, -1)

		// Then
		c.Assert(offset.Val, Equals, tc.committed, Commentf("case #%d", i))
		c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case #%d", i))
		// Both nil and empty slice ack ranges become nil when going through
		// the encode/decode circle.
		if ot.ackedRanges == nil || len(ot.ackedRanges) == 0 {
			c.Assert(ot2.ackedRanges, IsNil, Commentf("case #%d", i))
		} else {
			c.Assert(ot2.ackedRanges, DeepEquals, ot.ackedRanges, Commentf("case #%d", i))
		}
	}
}

// Acknowledged offsets are properly reflected in ackedRanges.
func (s *OffsetTrkSuite) TestAdjust(c *C) {
	initialOffset := int64(300)
	for i, tc := range []struct {
		offset    int64
		ranges    string
		hasOffers bool
		nextOffer int64
	}{
		0:  {offset: 300, ranges: "2-4,5-9,12-13,14-16", hasOffers: true, nextOffer: 300},
		1:  {offset: 301, ranges: "1-3,4-8,11-12,13-15", hasOffers: true, nextOffer: 301},
		2:  {offset: 304, ranges: "1-5,8-9,10-12", hasOffers: true, nextOffer: 304},
		3:  {offset: 304, ranges: "1-5,8-9,10-12", hasOffers: true, nextOffer: 304},
		4:  {offset: 304, ranges: "1-5,8-9,10-12", hasOffers: true, nextOffer: 304},
		5:  {offset: 309, ranges: "3-4,5-7", hasOffers: true, nextOffer: 309},
		6:  {offset: 309, ranges: "3-4,5-7", hasOffers: true, nextOffer: 309},
		7:  {offset: 309, ranges: "3-4,5-7", hasOffers: true, nextOffer: 309},
		8:  {offset: 309, ranges: "3-4,5-7", hasOffers: true, nextOffer: 309},
		9:  {offset: 309, ranges: "3-4,5-7", hasOffers: true, nextOffer: 309},
		10: {offset: 310, ranges: "2-3,4-6", hasOffers: true, nextOffer: 310},
		11: {offset: 311, ranges: "1-2,3-5", hasOffers: true, nextOffer: 311},
		12: {offset: 313, ranges: "1-3", hasOffers: true, nextOffer: 313},
		13: {offset: 313, ranges: "1-3", hasOffers: true, nextOffer: 313},
		14: {offset: 316, ranges: "", hasOffers: true, nextOffer: 316},
		15: {offset: 316, ranges: "", hasOffers: true, nextOffer: 316},
		16: {offset: 316, ranges: "", hasOffers: true, nextOffer: 316},
		17: {offset: 317, ranges: "", hasOffers: false},
	} {
		correction := initialOffset + int64(i)
		ot := New(s.ns, offsetmgr.Offset{Val: initialOffset}, -1)
		for j, acked := range []int{0, 0, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0, 1, 0, 1, 1, 0} {
			offset := initialOffset + int64(j)
			ot.OnOffered(consumer.Message{Offset: offset})
			if acked == 1 {
				ot.OnAcked(offset)
			}
		}
		c.Assert(SparseAcks2Str(ot.offset), Equals, "2-4,5-9,12-13,14-16")

		// When
		adjustedOffset := ot.Adjust(correction)

		// Then
		c.Assert(adjustedOffset.Val, Equals, tc.offset, Commentf("case #%d", i))
		c.Assert(SparseAcks2Str(ot.offset), Equals, tc.ranges, Commentf("case #%d", i))

		msg, _, ok := ot.nextRetry(time.Now())
		c.Assert(ok, Equals, tc.hasOffers)
		if tc.hasOffers {
			c.Assert(msg.Offset, Equals, tc.nextOffer, Commentf("case #%d", i))
		}

	}
}

func (s *OffsetTrkSuite) TestRangeEncodeDecode(c *C) {
	for i, tc := range []struct {
		base int64
		ar   offsetRange
	}{
		0: {0, offsetRange{1, 2}},
		1: {100, offsetRange{101, 102}},
		2: {0, offsetRange{0x7FFFFFFFFFFFFFFE, 0x7FFFFFFFFFFFFFFF}},
	} {
		var encoded []byte
		var decoded offsetRange

		// When
		encoded = tc.ar.encode(tc.base, encoded)
		decoded.decode(tc.base, encoded)

		// Then
		c.Assert(decoded, Equals, tc.ar, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestRangeDecodeError(c *C) {
	var ar offsetRange
	for i, tc := range []struct {
		encoded string
		error   string
	}{
		0: {"AA", "bad range: {10000 10000}"},
		1: {"0123456789012a", "bad `from` boundary: bad sequence: 0123456789012"},
		2: {"a0123456789012b", "bad `to` boundary: bad sequence: 0123456789012"},
		3: {"1@b", "bad `from` boundary: bad char: @"},
		4: {"a@b", "bad `to` boundary: bad char: @"},
	} {
		// When
		buf, err := ar.decode(10000, []byte(tc.encoded))

		// Then
		c.Assert(err.Error(), Equals, tc.error, Commentf("case #%d", i))
		c.Assert(buf, IsNil, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestNew(c *C) {
	for i, tc := range []struct {
		given  offsetmgr.Offset
		actual offsetmgr.Offset
	}{
		0: {
			offsetmgr.Offset{1000, ""},
			offsetmgr.Offset{1000, ""},
		},
		1: {
			offsetmgr.Offset{1000, "abra1234+/P"},
			offsetmgr.Offset{1000, "abra1234+/P"},
		},
		2: {
			offsetmgr.Offset{1000, "abra1234+/PS"},
			offsetmgr.Offset{1000, ""},
		},
		3: {
			offsetmgr.Offset{1000, "a@b"},
			offsetmgr.Offset{1000, ""},
		},
	} {
		// When
		ot := New(s.ns, tc.given, -1)

		// Then
		c.Assert(ot.offset, Equals, tc.actual, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestIsAcked(c *C) {
	meta := encodeAckedRanges(301, []offsetRange{
		{302, 305}, {307, 309}, {310, 313}})
	offset := offsetmgr.Offset{301, meta}
	ot := New(s.ns, offset, -1)
	for i, tc := range []struct {
		offset       int64
		isAcked      bool
		nextNotAcked int64
	}{
		0:  {offset: 299, isAcked: true, nextNotAcked: 301},
		1:  {offset: 300, isAcked: true, nextNotAcked: 301},
		2:  {offset: 301, isAcked: false, nextNotAcked: 305},
		3:  {offset: 302, isAcked: true, nextNotAcked: 305},
		4:  {offset: 303, isAcked: true, nextNotAcked: 305},
		5:  {offset: 304, isAcked: true, nextNotAcked: 305},
		6:  {offset: 305, isAcked: false, nextNotAcked: 306},
		7:  {offset: 306, isAcked: false, nextNotAcked: 309},
		8:  {offset: 307, isAcked: true, nextNotAcked: 309},
		9:  {offset: 308, isAcked: true, nextNotAcked: 309},
		10: {offset: 309, isAcked: false, nextNotAcked: 313},
		11: {offset: 310, isAcked: true, nextNotAcked: 313},
		12: {offset: 311, isAcked: true, nextNotAcked: 313},
		13: {offset: 312, isAcked: true, nextNotAcked: 313},
		14: {offset: 313, isAcked: false, nextNotAcked: 314},
		15: {offset: 314, isAcked: false, nextNotAcked: 315},
	} {
		// When
		isAcked, nextNotAcked := ot.IsAcked(tc.offset)
		// /Then
		c.Assert(isAcked, Equals, tc.isAcked, Commentf("case #%d", i))
		c.Assert(nextNotAcked, Equals, tc.nextNotAcked, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestOfferAckLoop(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	for i, tc := range []struct {
		act       action
		offset    int64
		count     int
		committed int64
		ranges    string
	}{
		0: {act: doOfr, offset: 298, count: 0},
		1: {act: doOfr, offset: 299, count: 0},
		// Test offering in arbitrary order, even though it never happens in
		// real life.
		2: {act: doOfr, offset: 301, count: 1},
		3: {act: doOfr, offset: 300, count: 2},
		4: {act: doOfr, offset: 304, count: 3},
		5: {act: doOfr, offset: 302, count: 4},
		6: {act: doOfr, offset: 303, count: 5},
		// And a bit of regular sequential offering
		7: {act: doOfr, offset: 305, count: 6},
		8: {act: doOfr, offset: 306, count: 7},
		// Ack some messages
		9:  {act: doAck, offset: 307, count: 7, committed: 300, ranges: "7-8"},
		10: {act: doAck, offset: 299, count: 7, committed: 300, ranges: "7-8"},
		11: {act: doAck, offset: 303, count: 6, committed: 300, ranges: "3-4,7-8"},
		12: {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		// Offer acked again
		13: {act: doOfr, offset: 298, count: 5},
		14: {act: doOfr, offset: 305, count: 5},
		// Offer offered (ignored)
		15: {act: doOfr, offset: 302, count: 5},
		16: {act: doOfr, offset: 305, count: 5},
		17: {act: doOfr, offset: 306, count: 5},
		//        Ack acked (ignored)
		18: {act: doAck, offset: 299, count: 5, committed: 301, ranges: "2-3,6-7"},
		19: {act: doAck, offset: 300, count: 5, committed: 301, ranges: "2-3,6-7"},
		//        Offer/Ack some more
		20: {act: doOfr, offset: 309, count: 6},
		21: {act: doAck, offset: 302, count: 5, committed: 301, ranges: "1-3,6-7"},
		22: {act: doAck, offset: 301, count: 4, committed: 304, ranges: "3-4"},
		23: {act: doOfr, offset: 308, count: 5},
		24: {act: doAck, offset: 306, count: 4, committed: 304, ranges: "2-4"},
	} {
		var count int
		// When
		switch tc.act {
		case doOfr:
			count = ot.OnOffered(consumer.Message{Offset: tc.offset})
		case doAck:
			var offset offsetmgr.Offset
			offset, count = ot.OnAcked(tc.offset)
			c.Assert(offset.Val, Equals, tc.committed, Commentf("case #%d", i))
			c.Assert(SparseAcks2Str(offset), Equals, tc.ranges, Commentf("case #%d", i))
		}
		// Then
		c.Assert(count, Equals, tc.count, Commentf("case #%d", i))
	}
}

func (s *OffsetTrkSuite) TestNextRetry(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, 5*time.Second)
	msgs := []consumer.Message{
		{Offset: 300},
		{Offset: 301},
		{Offset: 302},
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
		0: {millis: 0},
		1: {millis: 1000},
		2: {millis: 5000},
		3: {millis: 5001, offset: 300, retryCount: 1},
		4: {millis: 7000},
		// The next attempt deadline is counted from the time when NextRetry
		// returns the message.
		5:  {millis: 9001, offset: 301, retryCount: 1},
		6:  {millis: 9003, offset: 302, retryCount: 1},
		7:  {millis: 10000},
		8:  {millis: 20000, offset: 300, retryCount: 2},
		9:  {millis: 20002, offset: 301, retryCount: 2},
		10: {millis: 20004, offset: 302, retryCount: 2},
		11: {millis: 25000},
		12: {millis: 25001, offset: 300, retryCount: 3},
		13: {millis: 25002},
		14: {millis: 25003, offset: 301, retryCount: 3},
		15: {millis: 25004},
		16: {millis: 25005, offset: 302, retryCount: 3},
		17: {millis: 25006},
	} {
		// When
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		msg, retryCount, ok := ot.nextRetry(now)

		// Then
		if ok {
			c.Assert(msg.Offset, Equals, tc.offset, Commentf("case #%d", i))
			c.Assert(retryCount, Equals, tc.retryCount, Commentf("case #%d", i))
		} else {
			c.Assert(tc.offset, Equals, int64(0), Commentf("case #%d", i))
			c.Assert(retryCount, Equals, -1, Commentf("case #%d", i))
		}
	}
}

func (s *OffsetTrkSuite) TestShouldWait4Ack(c *C) {
	ot := New(s.ns, offsetmgr.Offset{Val: 300}, -1)
	msgs := []consumer.Message{
		{Offset: 300},
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
		0:  {millis: 0, wait: true, timeout: 3000},
		1:  {millis: 2998, wait: true, timeout: 2},
		2:  {millis: 2999, wait: true, timeout: 1},
		3:  {millis: 3000, wait: true, timeout: 1000},
		4:  {millis: 3001, wait: true, timeout: 999},
		5:  {millis: 3999, wait: true, timeout: 1},
		6:  {millis: 4000, wait: true, timeout: 5000},
		7:  {millis: 8950, wait: true, timeout: 50},
		8:  {millis: 8999, wait: true, timeout: 1},
		9:  {millis: 9000, wait: false, timeout: 0},
		10: {millis: 9001, wait: false, timeout: 0},
		11: {millis: 9999, wait: false, timeout: 0},
	} {
		// When/Then
		now := begin.Add(time.Duration(tc.millis) * time.Millisecond)
		wait, timeout := ot.shouldWait4Ack(now)
		c.Assert(wait, Equals, tc.wait, Commentf("case #%d", i))
		c.Assert(timeout, Equals, time.Duration(tc.timeout)*time.Millisecond, Commentf("case #%d", i))
	}
}
