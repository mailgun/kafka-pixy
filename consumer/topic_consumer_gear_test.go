package consumer

import (
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/consumermsg"
	"github.com/mailgun/kafka-pixy/consumer/multiplexer"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

type TopicConsumerGearSuite struct {
	spawnedPartitions map[int32]bool
	muxWiringSequence []muxWiring
}

var muxStopped = muxWiring{}

var _ = Suite(&TopicConsumerGearSuite{})

func (s *TopicConsumerGearSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
}

func (s *TopicConsumerGearSuite) SetUpTest(c *C) {
	s.spawnedPartitions = make(map[int32]bool)
	s.muxWiringSequence = make([]muxWiring, 0)
}

func (s *TopicConsumerGearSuite) TestSortedInputs(c *C) {
	c.Assert([]multiplexer.In{}, DeepEquals,
		sortedInputs(map[int32]muxInputActor{}))
	c.Assert([]multiplexer.In{in(1)}, DeepEquals,
		sortedInputs(map[int32]muxInputActor{1: in(1)}))
	c.Assert([]multiplexer.In{in(1), in(2), in(3), in(4), in(5)}, DeepEquals,
		sortedInputs(map[int32]muxInputActor{
			1: in(1), 2: in(2), 3: in(3), 4: in(4), 5: in(5)}))
}

func (s *TopicConsumerGearSuite) TestInitial(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{})
	c.Assert(tcg.isIdle(), Equals, true)

	// When
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{2: true, 3: true, 4: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}}})
	c.Assert(tcg.isIdle(), Equals, false)
}

func (s *TopicConsumerGearSuite) TestPartitionsAdd(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, []int32{2, 3, 4, 5})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{2: true, 3: true, 4: true, 5: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
		{tc, []int32{2, 3, 4, 5}}})
	c.Assert(tcg.isIdle(), Equals, false)
}

func (s *TopicConsumerGearSuite) TestPartitionsRemove(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, []int32{3, 4})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{3: true, 4: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
		{tc, []int32{3, 4}}})
	c.Assert(tcg.isIdle(), Equals, false)
}

func (s *TopicConsumerGearSuite) TestPartitionsAddRemove(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, []int32{3, 4, 5})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{3: true, 4: true, 5: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
		{tc, []int32{3, 4, 5}}})
	c.Assert(tcg.isIdle(), Equals, false)
}

// If muxInputs called with the same set of partitions and topic consumer as
// before, then such call is silently ignored.
func (s *TopicConsumerGearSuite) TestPartitionsSame(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{2: true, 3: true, 4: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}}})
	c.Assert(tcg.isIdle(), Equals, false)
}

func (s *TopicConsumerGearSuite) TestPartitionsNone(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, []int32{})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
	})
	c.Assert(tcg.isIdle(), Equals, true)
}

func (s *TopicConsumerGearSuite) TestPartitionsNil(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc, nil)

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
	})
	c.Assert(tcg.isIdle(), Equals, true)
}

// If it is only topic consumer that is changed then the multiplexer is
// restarted anyway.
func (s *TopicConsumerGearSuite) TestTopicConsumerChanged(c *C) {
	tc1 := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tc2 := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc1, []int32{2, 3, 4})

	// When
	tcg.muxInputs(tc2, []int32{2, 3, 4})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{2: true, 3: true, 4: true})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc1, []int32{2, 3, 4}},
		muxStopped,
		{tc2, []int32{2, 3, 4}},
	})
	c.Assert(tcg.isIdle(), Equals, false)
}

// If it is only topic consumer that is changed then the multiplexer is
// restarted anyway.
func (s *TopicConsumerGearSuite) TestTopicConsumerNil(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.muxInputs(nil, []int32{3, 4, 5})

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped,
	})
	c.Assert(tcg.isIdle(), Equals, true)
}

func (s *TopicConsumerGearSuite) TestStop(c *C) {
	tc := &topicConsumer{actorID: actor.RootID.NewChild("test")}
	tcg := newTopicConsumerGear(s.spawnInput)
	tcg.spawnMuxFn = s.spawnMultiplexer
	tcg.muxInputs(tc, []int32{2, 3, 4})

	// When
	tcg.stop()

	// Then
	c.Assert(s.spawnedPartitions, DeepEquals, map[int32]bool{})
	c.Assert(s.muxWiringSequence, DeepEquals, []muxWiring{
		{tc, []int32{2, 3, 4}},
		muxStopped})
	c.Assert(tcg.isIdle(), Equals, true)
}

type muxWiring struct {
	tc         *topicConsumer
	partitions []int32
}

type mockMux struct {
	s *TopicConsumerGearSuite
}

func (s *TopicConsumerGearSuite) spawnMultiplexer(output multiplexer.Out, inputs []multiplexer.In) muxActor {
	partitions := make([]int32, len(inputs))
	for i, in := range inputs {
		in := in.(*mockMuxInputActor)
		partitions[i] = in.partition
	}
	tc := output.(*topicConsumer)
	s.muxWiringSequence = append(s.muxWiringSequence, muxWiring{tc, partitions})
	return &mockMux{s: s}
}

func (m *mockMux) Stop() {
	m.s.muxWiringSequence = append(m.s.muxWiringSequence, muxStopped)
}

// mockMuxInputActor rigs spawn and stop functions to maintain a set of
// partitions that have active inputs in the `s.inputs` map.
type mockMuxInputActor struct {
	partition int32
	s         *TopicConsumerGearSuite
}

func (s *TopicConsumerGearSuite) spawnInput(topic string, partition int32) muxInputActor {
	input := &mockMuxInputActor{partition: partition, s: s}
	s.spawnedPartitions[partition] = true
	return input
}

func (m *mockMuxInputActor) Messages() <-chan *consumermsg.ConsumerMessage {
	return nil
}

func (m *mockMuxInputActor) Acks() chan<- *consumermsg.ConsumerMessage {
	return nil
}

func (m *mockMuxInputActor) Stop() {
	delete(m.s.spawnedPartitions, m.partition)
}

func in(p int32) muxInputActor {
	return &mockMuxInputActor{partition: p}
}
