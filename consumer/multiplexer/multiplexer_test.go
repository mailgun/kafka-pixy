package multiplexer

import (
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type MultiplexerSuite struct {
	ns *actor.ID
}

var _ = Suite(&MultiplexerSuite{})

func (s *MultiplexerSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
}

func (s *MultiplexerSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

func (s *MultiplexerSuite) TestSortedInputs(c *C) {
	ins := map[int32]*fetchedInput{
		1: {newMockIn(nil), nil},
		2: {newMockIn(nil), nil},
		3: {newMockIn(nil), nil},
		4: {newMockIn(nil), nil},
		5: {newMockIn(nil), nil},
	}
	c.Assert(makeSortedIns(map[int32]*fetchedInput{}), DeepEquals,
		[]*fetchedInput{})

	c.Assert(makeSortedIns(map[int32]*fetchedInput{1: ins[1]}), DeepEquals,
		[]*fetchedInput{ins[1]})

	c.Assert(makeSortedIns(map[int32]*fetchedInput{5: ins[5], 2: ins[2], 4: ins[4], 1: ins[1], 3: ins[3]}), DeepEquals,
		[]*fetchedInput{ins[1], ins[2], ins[3], ins[4], ins[5]})
}

func (s *MultiplexerSuite) TestFindMaxLag(c *C) {
	inputs := []*fetchedInput{
		{nil, nil},
		{nil, lag(11)},
		{nil, lag(13)},
		{nil, nil},
		{nil, lag(12)},
	}
	max, idx, cnt := findMaxLag(inputs)
	c.Assert(max, Equals, int64(13))
	c.Assert(idx, Equals, 2)
	c.Assert(cnt, Equals, 1)
}

func (s *MultiplexerSuite) TestFindMaxLagSame(c *C) {
	inputs := []*fetchedInput{
		{nil, nil},
		{nil, lag(11)},
		{nil, lag(11)},
		{nil, nil},
		{nil, lag(11)},
	}
	max, idx, cnt := findMaxLag(inputs)
	c.Assert(max, Equals, int64(11))
	c.Assert(idx, Equals, 1)
	c.Assert(cnt, Equals, 3)
}

// SelectInput chooses an input that has a next message with the biggest lag.
func (s *MultiplexerSuite) TestSelectInput(c *C) {
	inputs := []*fetchedInput{
		{nil, nil},
		{nil, lag(11)},
		{nil, lag(13)},
		{nil, nil},
		{nil, lag(12)},
	}
	c.Assert(selectInput(-1, inputs), Equals, 2)
	c.Assert(selectInput(2, inputs), Equals, 2)
	c.Assert(selectInput(3, inputs), Equals, 2)
}

// If there are several inputs with the same biggest lag, then the last input
// index is used to chose between them.
func (s *MultiplexerSuite) TestSelectInputSameLag(c *C) {
	inputs := []*fetchedInput{
		{nil, nil},
		{nil, lag(11)},
		{nil, lag(11)},
		{nil, lag(10)},
		{nil, lag(11)},
	}
	c.Assert(selectInput(-1, inputs), Equals, 1)
	c.Assert(selectInput(0, inputs), Equals, 1)
	c.Assert(selectInput(1, inputs), Equals, 2)
	c.Assert(selectInput(2, inputs), Equals, 4)
	c.Assert(selectInput(3, inputs), Equals, 4)
	c.Assert(selectInput(4, inputs), Equals, 1)
}

// If there is just one input then it is forwarded to the output.
func (s *MultiplexerSuite) TestOneInput(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh,
			msg(1001, 1),
			msg(1002, 1),
			msg(1003, 1),
		),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()

	// When
	m.WireUp(out, []int32{1})

	// Then
	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1003, 1))
}

// If there are several inputs with the same max lag then messages from them
// are multiplexed in the round robin fashion. Note that messages are acknowledged
func (s *MultiplexerSuite) TestSameLag(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh,
			msg(1001, 1),
			msg(1002, 1),
			msg(1003, 1),
			msg(1004, 1),
			msg(1005, 1),
		),
		2: newMockIn(acksCh,
			msg(2001, 1),
			msg(2002, 1),
			msg(2003, 1),
		),
		3: newMockIn(acksCh,
			msg(3001, 1),
			msg(3002, 1),
		),
		4: newMockIn(acksCh,
			msg(4001, 1),
		)}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()

	// When
	m.WireUp(out, []int32{1, 2, 3, 4})

	// Then
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
}

// Messages with the largest lag among current channel heads is selected.
func (s *MultiplexerSuite) TestLargeLagPreferred(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh,
			msg(1001, 1),
			msg(1002, 3),
		),
		2: newMockIn(acksCh,
			msg(2001, 2),
			msg(2002, 1),
		),
		3: newMockIn(acksCh,
			msg(3001, 1),
			msg(3002, 4),
		),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	m.Stop()

	// When
	m.WireUp(out, []int32{1, 2, 3})

	// Then
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 2))
	checkMsg(c, out.messagesCh, acksCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(3002, 4))
	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(1002, 3))
	checkMsg(c, out.messagesCh, acksCh, msg(2002, 1))
}

// If there are no messages available on the inputs, multiplexer blocks waiting
// for a message to appear in any of the inputs.
func (s *MultiplexerSuite) TestNoMessages(c *C) {
	// Given
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh),
		3: newMockIn(acksCh),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	m.WireUp(out, []int32{1, 2, 3})

	// Make sure that multiplexer started and is pumping messages.
	checkMsg(c, out.messagesCh, acksCh, msg(1001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", *msg)
	case <-time.After(100 * time.Millisecond):
	}

	// When
	ins[2].(*mockIn).messagesCh <- msg(2001, 1)

	// Then
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
}

func (s *MultiplexerSuite) TestWireUp(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)

	// When
	m.WireUp(out, []int32{2, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpAdd(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, []int32{2, 3, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpRemove(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, []int32{4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpAddRemove(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, []int32{3, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, acksCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpSame(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, []int32{2, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, acksCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpEmpty(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, []int32{})

	// Then
	c.Assert(m.IsRunning(), Equals, false)
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpNil(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out, []int32{2, 4})

	// When
	m.WireUp(out, nil)

	// Then
	c.Assert(m.IsRunning(), Equals, false)
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpOutChanged(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out1 := newMockOut(0)
	out2 := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out1, []int32{2, 4})

	// When
	m.WireUp(out2, []int32{2, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	select {
	case msg := <-out1.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
	checkMsg(c, out2.messagesCh, acksCh, msg(2001, 1))
	checkMsg(c, out2.messagesCh, acksCh, msg(4001, 1))
	select {
	case msg := <-out2.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpOutNil(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out1 := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)
	m.WireUp(out1, []int32{2, 4})

	// When
	m.WireUp(nil, []int32{2, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, false)
	select {
	case msg := <-out1.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestStop(c *C) {
	acksCh := make(chan *consumer.Message, 100)
	ins := map[int32]In{
		1: newMockIn(acksCh, msg(1001, 1)),
		2: newMockIn(acksCh, msg(2001, 1)),
		3: newMockIn(acksCh, msg(3001, 1)),
		4: newMockIn(acksCh, msg(4001, 1)),
		5: newMockIn(acksCh, msg(5001, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	m.WireUp(out, []int32{2, 4})
	c.Assert(m.IsRunning(), Equals, true)

	// When
	m.Stop()

	// Then
	c.Assert(m.IsRunning(), Equals, false)
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

type mockIn struct {
	messagesCh chan *consumer.Message
	acksCh     chan *consumer.Message
}

func newMockIn(acksCh chan *consumer.Message, messages ...*consumer.Message) *mockIn {
	mi := mockIn{
		messagesCh: make(chan *consumer.Message, len(messages)),
		acksCh:     acksCh,
	}
	for _, m := range messages {
		mi.messagesCh <- m
	}
	return &mi
}

// implements `In`
func (mi *mockIn) Messages() <-chan *consumer.Message {
	return mi.messagesCh
}

// implements `In`
func (mi *mockIn) Acks() chan<- *consumer.Message {
	return mi.acksCh
}

// implements `In`
func (mi *mockIn) Stop() {
}

type mockOut struct {
	messagesCh chan *consumer.Message
}

func newMockOut(capacity int) *mockOut {
	return &mockOut{
		messagesCh: make(chan *consumer.Message, capacity),
	}
}

// implements `Out`
func (mo *mockOut) Messages() chan<- *consumer.Message {
	return mo.messagesCh
}

func msg(offset, lag int64) *consumer.Message {
	return &consumer.Message{
		Offset:        offset,
		HighWaterMark: offset + lag,
	}
}

func lag(lag int64) *consumer.Message {
	return &consumer.Message{
		Offset:        0,
		HighWaterMark: lag,
	}
}

func checkMsg(c *C, outCh, acksCh chan *consumer.Message, expected *consumer.Message) {
	c.Assert(<-outCh, DeepEquals, expected)
	c.Assert(<-acksCh, DeepEquals, expected)
}
