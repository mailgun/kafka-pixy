package consumer

import (
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/consumermsg"
	. "gopkg.in/check.v1"
)

type MultiplexerSuite struct {
	cid *actor.ID
}

var _ = Suite(&MultiplexerSuite{})

func (s *MultiplexerSuite) SetUpSuite(c *C) {
	s.cid = actor.RootID.NewChild("testMux")
}

func (s *MultiplexerSuite) TestFindMaxLag(c *C) {
	max, idx, cnt := findMaxLag([]*consumermsg.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)})
	c.Assert(max, Equals, int64(13))
	c.Assert(idx, Equals, 2)
	c.Assert(cnt, Equals, 1)

	max, idx, cnt = findMaxLag([]*consumermsg.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)})
	c.Assert(max, Equals, int64(11))
	c.Assert(idx, Equals, 1)
	c.Assert(cnt, Equals, 3)
}

func (s *MultiplexerSuite) TestSelectInputOne(c *C) {
	c.Assert(2, Equals, selectInput(-1, []*consumermsg.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)}))
	c.Assert(2, Equals, selectInput(2, []*consumermsg.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)}))
}

func (s *MultiplexerSuite) TestSelectInputMany(c *C) {
	c.Assert(1, Equals, selectInput(-1, []*consumermsg.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(2, Equals, selectInput(1, []*consumermsg.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(4, Equals, selectInput(2, []*consumermsg.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(1, Equals, selectInput(4, []*consumermsg.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
}

// If there is just one input then it is forwarded to the output.
func (s *MultiplexerSuite) TestOneInput(c *C) {
	acksCh := make(chan *consumermsg.ConsumerMessage, 100)
	in1 := newMockMuxIn(acksCh,
		msg(1001, 1),
		msg(1002, 1),
		msg(1003, 1),
	)
	out := newMockMuxOut(100)
	m := spawnMultiplexer(s.cid, out, []muxInput{in1})

	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1003, 1))

	m.stop()
}

// If there are several inputs with the same max lag then messages from them
// are multiplexed in the round robin fashion. Note that messages are acknowledged
func (s *MultiplexerSuite) TestSameLag(c *C) {
	acksCh := make(chan *consumermsg.ConsumerMessage, 100)
	in1 := newMockMuxIn(acksCh,
		msg(1001, 1),
		msg(1002, 1),
		msg(1003, 1),
		msg(1004, 1),
		msg(1005, 1),
	)
	in2 := newMockMuxIn(acksCh,
		msg(2001, 1),
		msg(2002, 1),
		msg(2003, 1),
	)
	in3 := newMockMuxIn(acksCh,
		msg(3001, 1),
		msg(3002, 1),
	)
	in4 := newMockMuxIn(acksCh,
		msg(4001, 1),
	)
	out := newMockMuxOut(100)
	m := spawnMultiplexer(s.cid, out, []muxInput{in1, in2, in3, in4})

	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))

	checkMsg(c, out.messagesCh, acksCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(2002, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(3002, 1))

	checkMsg(c, out.messagesCh, acksCh, msg(1003, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(2003, 1))

	checkMsg(c, out.messagesCh, acksCh, msg(1004, 1))

	checkMsg(c, out.messagesCh, acksCh, msg(1005, 1))
	m.stop()
}

// Messages with the largest lag among current channel heads is selected.
func (s *MultiplexerSuite) TestLargeLagPreferred(c *C) {
	acksCh := make(chan *consumermsg.ConsumerMessage, 100)
	in1 := newMockMuxIn(acksCh,
		msg(1001, 1),
		msg(1002, 3),
	)
	in2 := newMockMuxIn(acksCh,
		msg(2001, 2),
		msg(2002, 1),
	)
	in3 := newMockMuxIn(acksCh,
		msg(3001, 1),
		msg(3002, 4),
	)
	out := newMockMuxOut(100)
	m := spawnMultiplexer(s.cid, out, []muxInput{in1, in2, in3})

	checkMsg(c, out.messagesCh, acksCh, msg(2001, 2))
	checkMsg(c, out.messagesCh, acksCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(3002, 4))
	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1002, 3))
	checkMsg(c, out.messagesCh, acksCh, msg(2002, 1))
	m.stop()
}

// If there are no messages available on the inputs, multiplexer blocks waiting
// for a message to appear in any of the inputs.
func (s *MultiplexerSuite) TestNoMessages(c *C) {
	// Given
	acksCh := make(chan *consumermsg.ConsumerMessage, 100)
	in1 := newMockMuxIn(acksCh, msg(1001, 1))
	in2 := newMockMuxIn(acksCh)
	in3 := newMockMuxIn(acksCh)
	out := newMockMuxOut(100)
	m := spawnMultiplexer(s.cid, out, []muxInput{in1, in2, in3})

	// Make sure that multiplexer started and is pumping messages.
	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", *msg)
	case <-time.After(100 * time.Millisecond):
	}

	// When
	in2.messagesCh <- msg(2001, 1)

	// Then
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
	m.stop()
}

type mockMuxIn struct {
	messagesCh chan *consumermsg.ConsumerMessage
	acksCh     chan *consumermsg.ConsumerMessage
}

func newMockMuxIn(acksCh chan *consumermsg.ConsumerMessage, messages ...*consumermsg.ConsumerMessage) *mockMuxIn {
	mmi := mockMuxIn{
		messagesCh: make(chan *consumermsg.ConsumerMessage, len(messages)),
		acksCh:     acksCh,
	}
	for _, m := range messages {
		mmi.messagesCh <- m
	}
	return &mmi
}

func (mmi *mockMuxIn) messages() <-chan *consumermsg.ConsumerMessage {
	return mmi.messagesCh
}

func (mmi *mockMuxIn) acks() chan<- *consumermsg.ConsumerMessage {
	return mmi.acksCh
}

type mockMuxOut struct {
	messagesCh chan *consumermsg.ConsumerMessage
}

func newMockMuxOut(capacity int) *mockMuxOut {
	return &mockMuxOut{
		messagesCh: make(chan *consumermsg.ConsumerMessage, capacity),
	}
}

func (mmo *mockMuxOut) messages() chan<- *consumermsg.ConsumerMessage {
	return mmo.messagesCh
}

func msg(offset, lag int64) *consumermsg.ConsumerMessage {
	return &consumermsg.ConsumerMessage{
		Offset:        offset,
		HighWaterMark: offset + lag,
	}
}

func lag(lag int64) *consumermsg.ConsumerMessage {
	return &consumermsg.ConsumerMessage{
		Offset:        0,
		HighWaterMark: lag,
	}
}

func checkMsg(c *C, outCh, acksCh chan *consumermsg.ConsumerMessage, expected *consumermsg.ConsumerMessage) {
	c.Assert(<-outCh, DeepEquals, expected)
	c.Assert(<-acksCh, DeepEquals, expected)
}
