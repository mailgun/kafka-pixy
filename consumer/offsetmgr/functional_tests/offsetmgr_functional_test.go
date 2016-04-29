package functional_tests

import (
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type OffsetMgrFuncSuite struct {
	ns *actor.ID
}

var _ = Suite(&OffsetMgrFuncSuite{})

func (s *OffsetMgrFuncSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging(c)
}

func (s *OffsetMgrFuncSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
}

// The latest committed offset saved by one partition manager instance is
// returned by another as the initial commit.
func (s *OffsetMgrFuncSuite) TestLatestOffsetSaved(c *C) {
	// Given
	newOffset := time.Now().Unix()

	config := sarama.NewConfig()
	client, err := sarama.NewClient(testhelpers.KafkaPeers, config)
	c.Assert(err, IsNil)
	f := offsetmgr.SpawnFactory(s.ns, client)
	om0_1, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "test.4", 0), "g1", "test.4", 0)
	c.Assert(err, IsNil)

	// When: several offsets are committed.
	om0_1.SubmitOffset(newOffset, "foo")
	om0_1.SubmitOffset(newOffset+1, "bar")
	om0_1.SubmitOffset(newOffset+2, "bazz")

	// Then: last committed request is the one that becomes effective.
	om0_1.Stop()
	om0_2, err := f.SpawnOffsetManager(s.ns.NewChild("g1", "test.4", 0), "g1", "test.4", 0)
	c.Assert(err, IsNil)

	fo := <-om0_2.InitialOffset()
	c.Assert(fo, Equals, offsetmgr.DecoratedOffset{newOffset + 2, "bazz"})

	om0_2.Stop()
	f.Stop()
}
