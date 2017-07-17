package offsetmgr_test

import (
	"fmt"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	. "gopkg.in/check.v1"
)

type OffsetMgrFuncSuite struct {
	ns  *actor.ID
	cfg *config.Proxy
	kh  *kafkahelper.T
}

var _ = Suite(&OffsetMgrFuncSuite{})

func (s *OffsetMgrFuncSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *OffsetMgrFuncSuite) SetUpTest(c *C) {
	s.ns = actor.RootID.NewChild("T")
	s.cfg = testhelpers.NewTestProxyCfg("c1")
	s.kh = kafkahelper.New(c)
}

// The latest committed offset saved by one partition manager instance is
// returned by another as the initial commit.
func (s *OffsetMgrFuncSuite) TestLatestOffsetSaved(c *C) {
	newOffset := time.Now().Unix()

	f := offsetmgr.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt())
	defer f.Stop()

	tid := s.ns.NewChild("g1", "test.4", 0)
	om0_1, err := f.Spawn(tid, "g1", "test.4", 0)
	c.Assert(err, IsNil)

	// When: several offsets are committed.
	om0_1.SubmitOffset(offsetmgr.Offset{newOffset, "foo"})
	om0_1.SubmitOffset(offsetmgr.Offset{newOffset + 1, "bar"})
	om0_1.SubmitOffset(offsetmgr.Offset{newOffset + 2, "bazz"})

	// Then: last committed request is the one that becomes effective.
	om0_1.Stop()
	om0_2, err := f.Spawn(tid, "g1", "test.4", 0)
	c.Assert(err, IsNil)

	offset := <-om0_2.CommittedOffsets()
	c.Assert(offset, Equals, offsetmgr.Offset{newOffset + 2, "bazz"})

	om0_2.Stop()
}

// One offset manager factory can produce offset managers for different
// consumer groups and the same topic/partition.
func (s *OffsetMgrFuncSuite) TestMultipleGroups(c *C) {
	newOffset := time.Now().Unix()

	f := offsetmgr.SpawnFactory(s.ns, s.cfg, s.kh.KafkaClt())
	defer f.Stop()

	oms := make([]offsetmgr.T, 10)
	var err error
	for i := range oms {
		group := fmt.Sprintf("g%d", i)
		tid := s.ns.NewChild(group, "test.1")
		oms[i], err = f.Spawn(tid, group, "test.1", 0)
		c.Assert(err, IsNil)
	}

	// When
	var wg sync.WaitGroup
	for i := range oms {
		meta := fmt.Sprintf("meta%d", i)
		om := oms[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				om.SubmitOffset(offsetmgr.Offset{newOffset + int64(j), meta})
			}
			om.Stop()
		}()
	}
	wg.Wait()

	// Then
	for i := range oms {
		group := fmt.Sprintf("g%d", i)
		tid := s.ns.NewChild(group, "test.1", "then")
		om, err := f.Spawn(tid, group, "test.1", 0)
		c.Assert(err, IsNil)

		offset := <-om.CommittedOffsets()
		meta := fmt.Sprintf("meta%d", i)
		c.Assert(offset, Equals, offsetmgr.Offset{newOffset + 99, meta})

		om.Stop()
	}
}
