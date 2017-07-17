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
	testhelpers.InitLogging()
}

func (s *MultiplexerSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

func (s *MultiplexerSuite) TestSortedInputs(c *C) {
	ins := map[int32]*input{
		1: {In: newMockIn(), partition: 1},
		2: {In: newMockIn(), partition: 2},
		3: {In: newMockIn(), partition: 3},
		4: {In: newMockIn(), partition: 4},
		5: {In: newMockIn(), partition: 5},
	}
	c.Assert(makeSortedIns(map[int32]*input{}), DeepEquals,
		[]*input{})

	c.Assert(makeSortedIns(map[int32]*input{1: ins[1]}), DeepEquals,
		[]*input{ins[1]})

	c.Assert(makeSortedIns(map[int32]*input{5: ins[5], 2: ins[2], 4: ins[4], 1: ins[1], 3: ins[3]}), DeepEquals,
		[]*input{ins[1], ins[2], ins[3], ins[4], ins[5]})
}

// SelectInput chooses an input that has a next message with the biggest lag.
func (s *MultiplexerSuite) TestSelectInput(c *C) {
	inputs := []*input{
		{},
		{msg: lag(11), msgOk: true},
		{msg: lag(13), msgOk: true},
		{},
		{msg: lag(12), msgOk: true},
	}
	c.Assert(selectInput(-1, inputs), Equals, 2)
	c.Assert(selectInput(2, inputs), Equals, 2)
	c.Assert(selectInput(3, inputs), Equals, 2)
	c.Assert(selectInput(100, inputs), Equals, 2)
}

// If there are several inputs with the same biggest lag, then the last input
// index is used to chose between them.
func (s *MultiplexerSuite) TestSelectInputSameLag(c *C) {
	inputs := []*input{
		{},
		{msg: lag(11), msgOk: true},
		{msg: lag(11), msgOk: true},
		{msg: lag(10), msgOk: true},
		{msg: lag(11), msgOk: true},
	}
	c.Assert(selectInput(-1, inputs), Equals, 1)
	c.Assert(selectInput(0, inputs), Equals, 1)
	c.Assert(selectInput(1, inputs), Equals, 2)
	c.Assert(selectInput(2, inputs), Equals, 4)
	c.Assert(selectInput(3, inputs), Equals, 4)
	c.Assert(selectInput(4, inputs), Equals, 1)
	c.Assert(selectInput(100, inputs), Equals, 1)
}

// If there are several inputs with the same biggest lag, then the last input
// index is used to chose between them.
func (s *MultiplexerSuite) TestSelectInputNone(c *C) {
	inputs := []*input{
		{},
		{},
	}
	c.Assert(selectInput(-1, inputs), Equals, -1)
	c.Assert(selectInput(0, inputs), Equals, -1)
	c.Assert(selectInput(1, inputs), Equals, -1)
	c.Assert(selectInput(100, inputs), Equals, -1)
}

// If there is just one input then it is forwarded to the output.
func (s *MultiplexerSuite) TestOneInput(c *C) {
	ins := map[int32]In{
		1: newMockIn(
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
	checkMsg(c, out.messagesCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, msg(1003, 1))
}

// If there are several inputs with the same max lag then messages from them
// are multiplexed in the round robin fashion. Note that messages are acknowledged
func (s *MultiplexerSuite) TestSameLag(c *C) {
	ins := map[int32]In{
		1: newMockIn(
			msg(1001, 1),
			msg(1002, 1),
			msg(1003, 1),
			msg(1004, 1),
			msg(1005, 1),
		),
		2: newMockIn(
			msg(2001, 1),
			msg(2002, 1),
			msg(2003, 1),
		),
		3: newMockIn(
			msg(3001, 1),
			msg(3002, 1),
		),
		4: newMockIn(
			msg(4001, 1),
		)}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()

	// When
	m.WireUp(out, []int32{1, 2, 3, 4})

	// Then
	checkMsg(c, out.messagesCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, msg(4001, 1))

	checkMsg(c, out.messagesCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, msg(2002, 1))
	checkMsg(c, out.messagesCh, msg(3002, 1))

	checkMsg(c, out.messagesCh, msg(1003, 1))
	checkMsg(c, out.messagesCh, msg(2003, 1))

	checkMsg(c, out.messagesCh, msg(1004, 1))

	checkMsg(c, out.messagesCh, msg(1005, 1))
}

// Messages with the largest lag among current channel heads is selected.
func (s *MultiplexerSuite) TestLargeLagPreferred(c *C) {
	ins := map[int32]In{
		1: newMockIn(
			msg(1001, 1),
			msg(1002, 3),
		),
		2: newMockIn(
			msg(2001, 2),
			msg(2002, 1),
		),
		3: newMockIn(
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
	checkMsg(c, out.messagesCh, msg(2001, 2))
	checkMsg(c, out.messagesCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, msg(3002, 4))
	checkMsg(c, out.messagesCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, msg(1002, 3))
	checkMsg(c, out.messagesCh, msg(2002, 1))
}

// If there are no messages available on the inputs, multiplexer blocks waiting
// for a message to appear in any of the inputs.
func (s *MultiplexerSuite) TestNoMessages(c *C) {
	// Given
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(),
		3: newMockIn(),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	m.WireUp(out, []int32{1, 2, 3})

	// Make sure that multiplexer started and is pumping messages.
	checkMsg(c, out.messagesCh, msg(1001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	case <-time.After(100 * time.Millisecond):
	}

	// When
	ins[2].(*mockIn).messagesCh <- msg(2001, 1)

	// Then
	checkMsg(c, out.messagesCh, msg(2001, 1))
}

func (s *MultiplexerSuite) TestWireUp(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
	}
	out := newMockOut(100)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	c.Assert(m.IsRunning(), Equals, false)

	// When
	m.WireUp(out, []int32{2, 4})

	// Then
	c.Assert(m.IsRunning(), Equals, true)
	checkMsg(c, out.messagesCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpAdd(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	checkMsg(c, out.messagesCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpRemove(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	checkMsg(c, out.messagesCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpAddRemove(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	checkMsg(c, out.messagesCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpSame(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	checkMsg(c, out.messagesCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, msg(4001, 1))
	select {
	case msg := <-out.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpEmpty(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	checkMsg(c, out2.messagesCh, msg(2001, 1))
	checkMsg(c, out2.messagesCh, msg(4001, 1))
	select {
	case msg := <-out2.messagesCh:
		c.Errorf("Unexpected message: %v", msg)
	default:
	}
}

func (s *MultiplexerSuite) TestWireUpOutNil(c *C) {
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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
	ins := map[int32]In{
		1: newMockIn(msg(1001, 1)),
		2: newMockIn(msg(2001, 1)),
		3: newMockIn(msg(3001, 1)),
		4: newMockIn(msg(4001, 1)),
		5: newMockIn(msg(5001, 1)),
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

// If an input channel closes then respective input is removed from rotation.
func (s *MultiplexerSuite) TestInputChanClose(c *C) {
	ins := map[int32]In{
		1: newMockIn(
			msg(1001, 1),
			msg(1002, 1),
			msg(1003, 1)),
		2: newMockIn(
			msg(2001, 1)),
		3: newMockIn(
			msg(3001, 1),
			msg(3002, 1),
			msg(3003, 1)),
	}
	out := newMockOut(0)
	m := New(s.ns, func(p int32) In { return ins[p] })
	defer m.Stop()
	m.WireUp(out, []int32{1, 2, 3})
	c.Assert(m.IsRunning(), Equals, true)

	// When
	close(ins[2].(*mockIn).messagesCh)

	// Then
	checkMsg(c, out.messagesCh, msg(1001, 1))
	checkMsg(c, out.messagesCh, msg(2001, 1))
	checkMsg(c, out.messagesCh, msg(1002, 1))
	checkMsg(c, out.messagesCh, msg(3001, 1))
	checkMsg(c, out.messagesCh, msg(1003, 1))
	checkMsg(c, out.messagesCh, msg(3002, 1))
	checkMsg(c, out.messagesCh, msg(3003, 1))
}

type mockIn struct {
	messagesCh chan consumer.Message
}

func newMockIn(messages ...consumer.Message) *mockIn {
	mi := mockIn{
		messagesCh: make(chan consumer.Message, len(messages)),
	}
	for _, m := range messages {
		mi.messagesCh <- m
	}
	return &mi
}

// implements `In`
func (mi *mockIn) Messages() <-chan consumer.Message {
	return mi.messagesCh
}

// implements `In`
func (mi *mockIn) Stop() {
}

type mockOut struct {
	messagesCh chan consumer.Message
}

func newMockOut(capacity int) *mockOut {
	return &mockOut{
		messagesCh: make(chan consumer.Message, capacity),
	}
}

// implements `Out`
func (mo *mockOut) Messages() chan<- consumer.Message {
	return mo.messagesCh
}

func msg(offset, lag int64) consumer.Message {
	return consumer.Message{
		Offset:        offset,
		HighWaterMark: offset + lag,
	}
}

func lag(lag int64) consumer.Message {
	return consumer.Message{
		Offset:        0,
		HighWaterMark: lag,
	}
}

func checkMsg(c *C, outCh chan consumer.Message, want consumer.Message) {
	c.Assert(<-outCh, DeepEquals, want)
}
