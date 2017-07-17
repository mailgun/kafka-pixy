package groupcsm

import (
	"errors"
	"testing"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type GroupConsumerSuite struct {
	ns *actor.ID
}

var _ = Suite(&GroupConsumerSuite{})

func (s *GroupConsumerSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *GroupConsumerSuite) SetUpTest(*C) {
	s.ns = actor.RootID.NewChild("T")
}

func (s *GroupConsumerSuite) TestAssignTopicPartitions(c *C) {
	c.Assert(assignTopicPartitions(nil, nil), IsNil)
	c.Assert(assignTopicPartitions(nil, []string{}), IsNil)
	c.Assert(assignTopicPartitions(nil, []string{"a"}), IsNil)
	c.Assert(assignTopicPartitions(nil, []string{"a", "b"}), IsNil)
	c.Assert(assignTopicPartitions([]int32{}, nil), IsNil)
	c.Assert(assignTopicPartitions([]int32{}, []string{}), IsNil)
	c.Assert(assignTopicPartitions([]int32{}, []string{"a"}), IsNil)
	c.Assert(assignTopicPartitions([]int32{}, []string{"a", "b"}), IsNil)
	c.Assert(assignTopicPartitions([]int32{1}, nil), IsNil)
	c.Assert(assignTopicPartitions([]int32{1}, []string{}), IsNil)

	c.Assert(assignTopicPartitions([]int32{0}, []string{"a"}),
		DeepEquals, map[string][]int32{
			"a": {0},
		})
	c.Assert(assignTopicPartitions([]int32{1, 2, 0}, []string{"a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1, 2},
		})
	c.Assert(assignTopicPartitions([]int32{0}, []string{"b", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0},
		})
	c.Assert(assignTopicPartitions([]int32{0, 3, 1, 2}, []string{"b", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1},
			"b": {2, 3},
		})
	c.Assert(assignTopicPartitions([]int32{0, 3, 1, 2}, []string{"b", "c", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1},
			"b": {2},
			"c": {3},
		})
	c.Assert(assignTopicPartitions([]int32{0, 3, 1, 2, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1},
			"b": {2, 3},
			"c": {4},
		})
	c.Assert(assignTopicPartitions([]int32{0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1},
			"b": {2, 3},
			"c": {4, 5},
		})
	c.Assert(assignTopicPartitions([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"b", "c", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1, 2},
			"b": {3, 4},
			"c": {5, 6},
		})
	c.Assert(assignTopicPartitions([]int32{6, 0, 3, 1, 2, 5, 4}, []string{"d", "b", "c", "a"}),
		DeepEquals, map[string][]int32{
			"a": {0, 1},
			"b": {2, 3},
			"c": {4, 5},
			"d": {6},
		})
}

func (s *GroupConsumerSuite) TestResolvePartitions(c *C) {
	cfg := config.DefaultProxy()
	cfg.ClientID = "c"
	gc := T{
		cfg: cfg,
		fetchTopicPartitionsFn: func(topic string) ([]int32, error) {
			return map[string][]int32{
				"t1": {1, 2, 3, 4, 5},
				"t2": {1, 2},
				"t3": {1, 2, 3, 4, 5},
				"t4": {1, 2, 3},
				"t5": {1, 2, 3},
			}[topic], nil
		},
	}

	// When
	topicsToPartitions, err := gc.resolvePartitions(
		map[string][]string{
			"a": {"t1", "t2", "t3"},
			"b": {"t1", "t2", "t3"},
			"c": {"t1", "t2", "t4", "t5"},
			"d": {"t1", "t4"},
			"e": {},
			"f": nil,
		})

	// Then
	c.Assert(err, IsNil)
	c.Assert(topicsToPartitions, DeepEquals, map[string][]int32{
		"t1": {4},
		"t4": {1, 2},
		"t5": {1, 2, 3},
	})
}

func (s *GroupConsumerSuite) TestResolvePartitionsEmpty(c *C) {
	cfg := config.DefaultProxy()
	cfg.ClientID = "c"
	gc := T{
		cfg: cfg,
		fetchTopicPartitionsFn: func(topic string) ([]int32, error) {
			return nil, nil
		},
	}

	// When
	topicsToPartitions, err := gc.resolvePartitions(nil)

	// Then
	c.Assert(err, IsNil)
	c.Assert(topicsToPartitions, DeepEquals, map[string][]int32{})
}

func (s *GroupConsumerSuite) TestResolvePartitionsError(c *C) {
	cfg := config.DefaultProxy()
	cfg.ClientID = "c"
	gc := T{
		cfg: cfg,
		fetchTopicPartitionsFn: func(topic string) ([]int32, error) {
			return nil, errors.New("Kaboom!")
		},
	}

	// When
	topicsToPartitions, err := gc.resolvePartitions(map[string][]string{"c": {"t1"}})

	// Then
	c.Assert(err.Error(), Equals, "failed to get partition list, topic=t1: Kaboom!")
	c.Assert(topicsToPartitions, IsNil)
}
