package consumer

import (
	"time"

	"github.com/mailgun/kafka-pixy/context"
	"github.com/mailgun/sarama"
	. "gopkg.in/check.v1"
)

type MultiplexerSuite struct {
	cid *context.ID
}

var _ = Suite(&MultiplexerSuite{})

func (s *MultiplexerSuite) SetUpSuite(c *C) {
	s.cid = context.RootID.NewChild("testMux")
}

func (s *MultiplexerSuite) TestFindMaxLag(c *C) {
	max, idx, cnt := findMaxLag([]*sarama.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)})
	c.Assert(max, Equals, int64(13))
	c.Assert(idx, Equals, 2)
	c.Assert(cnt, Equals, 1)

	max, idx, cnt = findMaxLag([]*sarama.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)})
	c.Assert(max, Equals, int64(11))
	c.Assert(idx, Equals, 1)
	c.Assert(cnt, Equals, 3)
}

func (s *MultiplexerSuite) TestSelectInputOne(c *C) {
	c.Assert(2, Equals, selectInput(-1, []*sarama.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)}))
	c.Assert(2, Equals, selectInput(2, []*sarama.ConsumerMessage{nil, lag(11), lag(13), nil, lag(12)}))
}

func (s *MultiplexerSuite) TestSelectInputMany(c *C) {
	c.Assert(1, Equals, selectInput(-1, []*sarama.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(2, Equals, selectInput(1, []*sarama.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(4, Equals, selectInput(2, []*sarama.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
	c.Assert(1, Equals, selectInput(4, []*sarama.ConsumerMessage{nil, lag(11), lag(11), nil, lag(11)}))
}

// If there is just one input then it is forwarded to the output.
func (s *MultiplexerSuite) TestOneInput(c *C) {
	acksCh := make(chan *sarama.ConsumerMessage, 100)
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
	acksCh := make(chan *sarama.ConsumerMessage, 100)
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
	acksCh := make(chan *sarama.ConsumerMessage, 100)
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
	acksCh := make(chan *sarama.ConsumerMessage, 100)
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
	messagesCh chan *sarama.ConsumerMessage
	acksCh     chan *sarama.ConsumerMessage
}

func newMockMuxIn(acksCh chan *sarama.ConsumerMessage, messages ...*sarama.ConsumerMessage) *mockMuxIn {
	mmi := mockMuxIn{
		messagesCh: make(chan *sarama.ConsumerMessage, len(messages)),
		acksCh:     acksCh,
	}
	for _, m := range messages {
		mmi.messagesCh <- m
	}
	return &mmi
}

func (mmi *mockMuxIn) messages() <-chan *sarama.ConsumerMessage {
	return mmi.messagesCh
}

func (mmi *mockMuxIn) acks() chan<- *sarama.ConsumerMessage {
	return mmi.acksCh
}

type mockMuxOut struct {
	messagesCh chan *sarama.ConsumerMessage
}

func newMockMuxOut(capacity int) *mockMuxOut {
	return &mockMuxOut{
		messagesCh: make(chan *sarama.ConsumerMessage, capacity),
	}
}

func (mmo *mockMuxOut) messages() chan<- *sarama.ConsumerMessage {
	return mmo.messagesCh
}

func msg(offset, lag int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Offset:        offset,
		HighWaterMark: offset + lag,
	}
}

func lag(lag int64) *sarama.ConsumerMessage {
	return &sarama.ConsumerMessage{
		Offset:        0,
		HighWaterMark: lag,
	}
}

func checkMsg(c *C, outCh, acksCh chan *sarama.ConsumerMessage, expected *sarama.ConsumerMessage) {
	c.Assert(<-outCh, DeepEquals, expected)
	c.Assert(<-acksCh, DeepEquals, expected)
}
