package consumer

import (
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type GroupConsumerSuite struct {
}

var _ = Suite(&GroupConsumerSuite{})

func (s *GroupConsumerSuite) SetUpSuite(c *C) {
}

func (s *SmartConsumerSuite) TestResolveAssignments(c *C) {
	c.Assert(assignPartitionsToSubscribers(nil, nil), IsNil)
	c.Assert(assignPartitionsToSubscribers(nil, []string{}), IsNil)
	c.Assert(assignPartitionsToSubscribers(nil, []string{"a"}), IsNil)
	c.Assert(assignPartitionsToSubscribers(nil, []string{"a", "b"}), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{}, nil), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{}, []string{}), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{}, []string{"a"}), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{}, []string{"a", "b"}), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{1}, nil), IsNil)
	c.Assert(assignPartitionsToSubscribers([]int32{1}, []string{}), IsNil)

	c.Assert(assignPartitionsToSubscribers([]int32{0}, []string{"a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{1, 2, 0}, []string{"a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true, 2: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{0}, []string{"b", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{0, 3, 1, 2}, []string{"b", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true},
			"b": {2: true, 3: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{0, 3, 1, 2}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true},
			"b": {2: true},
			"c": {3: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{0, 3, 1, 2, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true},
			"b": {2: true, 3: true},
			"c": {4: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true},
			"b": {2: true, 3: true},
			"c": {4: true, 5: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true, 2: true},
			"b": {3: true, 4: true},
			"c": {5: true, 6: true},
		})
	c.Assert(assignPartitionsToSubscribers([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"d", "b", "c", "a"}),
		DeepEquals, map[string]map[int32]bool{
			"a": {0: true, 1: true},
			"b": {2: true, 3: true},
			"c": {4: true, 5: true},
			"d": {6: true},
		})
}
