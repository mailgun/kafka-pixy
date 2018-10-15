package kazoo

import (
	"sync"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/none"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/sirupsen/logrus"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type KazooModelSuite struct {
	kazoo Model
}

var _ = Suite(&KazooModelSuite{})

func (s *KazooModelSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
	zkConn, _, err := zk.Connect(
		testhelpers.ZookeeperPeers,
		300*time.Second,
		zk.WithLogger(logrus.StandardLogger()))
	c.Assert(err, IsNil)
	log := logrus.StandardLogger().WithFields(nil)
	s.kazoo = NewModel(zkConn, "", "group", "member0", log)
}

func (s *KazooModelSuite) TestCreateDeleteRace(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe/catch/a/tiger/by/the/toe/if/he/hollers/let/him/go/eeny/meeny/miny/moe"
	var wg sync.WaitGroup
	cancelCh := make(chan none.T)

	destructor := func(cancelCh <-chan none.T, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			err := s.kazoo.recursiveDeleteZNode("/eeny")
			cause := errors.Cause(err)
			if err != nil && cause != zk.ErrNoNode && cause != zk.ErrNotEmpty {
				c.Errorf("Unexpected destructor error %v", err)
			}
			select {
			case <-cancelCh:
				return
			default:
			}
		}
	}
	constructor := func(cancelCh <-chan none.T, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			err := s.kazoo.durableCreateZNode(path, []byte("foo"), 0)
			cause := errors.Cause(err)
			if err != nil && cause != zk.ErrNodeExists {
				c.Errorf("Unexpected constructor error %v", err)
			}
			select {
			case <-cancelCh:
				return
			default:
			}
		}
	}

	// Create a long path node.
	s.kazoo.durableCreateZNode(path, []byte("foo"), 0)
	_, _, err := s.kazoo.zkConn.Get(path)
	c.Assert(err, IsNil)

	// Start competing destructors
	wg.Add(1)
	go destructor(cancelCh, &wg)
	wg.Add(1)
	go destructor(cancelCh, &wg)

	// Wait for the root node to be deleted.
	for {
		_, _, err := s.kazoo.zkConn.Get("/eeny")
		if err == zk.ErrNoNode {
			break
		}
	}

	// Start competing constructors.
	wg.Add(1)
	go constructor(cancelCh, &wg)
	wg.Add(1)
	go constructor(cancelCh, &wg)

	// Observe tug-of-war with victories on both sides for 3 times, because
	// it is a charm ;-).
	for i := 0; i < 3; i++ {
		// Wait for the long path node to be created (constructors victory)
		for {
			_, _, err := s.kazoo.zkConn.Get(path)
			if err == nil {
				break
			}
		}
		// Wait for the root to be deleted (destructors victory)
		for {
			_, _, err := s.kazoo.zkConn.Get("/eeny")
			if err == zk.ErrNoNode {
				break
			}
		}
	}

	// Stop contenders and wait for them to terminate.
	close(cancelCh)
	wg.Wait()
}

// Recursive delete fails if the node children are being changed concurrently.
func (s *KazooModelSuite) TestDeepDeleteFail(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe/catch/a/tiger/by/the/toe"
	var wg sync.WaitGroup
	cancelCh := make(chan none.T)

	constructor := func(cancelCh <-chan none.T, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			err := s.kazoo.durableCreateZNode(path, []byte("foo"), 0)
			cause := errors.Cause(err)
			if err != nil && cause != zk.ErrNodeExists {
				c.Errorf("Unexpected constructor error %v", err)
			}
			select {
			case <-cancelCh:
				return
			default:
			}
		}
	}

	// Create a long path node.
	err := s.kazoo.durableCreateZNode(path, []byte("foo"), 0)
	c.Assert(err, IsNil)

	// Start a constructor.
	wg.Add(1)
	go constructor(cancelCh, &wg)

	// Wait for deep delete fail due to constructor interference.
	for {
		err := s.kazoo.recursiveDeleteZNode("/eeny")
		cause := errors.Cause(err)
		if cause == zk.ErrNotEmpty {
			// Constructor interference.
			break
		}
		if err != nil && cause != zk.ErrNoNode {
			c.Errorf("Unexpected destructor error %v", err)
			break
		}
	}

	// Stop constructors and wait for them to terminate.
	close(cancelCh)
	wg.Wait()
}

// Recursive delete fails if the node children are being changed concurrently.
func (s *KazooModelSuite) TestUpsert(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe/catch/a/tiger/by/the/toe"

	err := s.kazoo.durableUpsertZNode(path, []byte("foo"), 0)
	c.Assert(err, IsNil)

	got, _, err := s.kazoo.zkConn.Get(path)
	c.Assert(err, IsNil)
	c.Assert(string(got), Equals, "foo")

	err = s.kazoo.durableUpsertZNode(path, []byte("bar"), 0)
	c.Assert(err, IsNil)

	got, _, err = s.kazoo.zkConn.Get(path)
	c.Assert(err, IsNil)
	c.Assert(string(got), Equals, "bar")
}

func (s *KazooModelSuite) TestUpsertDeleteRace(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe/catch/a/tiger/by/the/toe/if/he/hollers/let/him/go/eeny/meeny/miny/moe"
	var wg sync.WaitGroup
	cancelCh := make(chan none.T)

	destructor := func(cancelCh <-chan none.T, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			err := s.kazoo.recursiveDeleteZNode("/eeny")
			cause := errors.Cause(err)
			if err != nil && cause != zk.ErrNoNode && cause != zk.ErrNotEmpty && cause != zk.ErrBadVersion {
				c.Errorf("Unexpected destructor error %v", err)
			}
			select {
			case <-cancelCh:
				return
			default:
			}
		}
	}
	upserter := func(cancelCh <-chan none.T, wg *sync.WaitGroup) {
		defer wg.Done()
		for {
			err := s.kazoo.durableUpsertZNode(path, []byte("foo"), 0)
			if err != nil {
				c.Errorf("Unexpected upserter error %v", err)
			}
			select {
			case <-cancelCh:
				return
			default:
			}
		}
	}

	// Create a long path node.
	s.kazoo.durableCreateZNode(path, []byte("foo"), 0)
	_, _, err := s.kazoo.zkConn.Get(path)
	c.Assert(err, IsNil)

	// Start competing destructors
	wg.Add(1)
	go destructor(cancelCh, &wg)
	wg.Add(1)
	go destructor(cancelCh, &wg)

	// Wait for the root node to be deleted.
	for {
		_, _, err := s.kazoo.zkConn.Get("/eeny")
		if err == zk.ErrNoNode {
			break
		}
	}

	// Start competing upserters.
	wg.Add(1)
	go upserter(cancelCh, &wg)

	// Observe tug-of-war with victories on both sides for 3 times, because
	// it is a charm ;-).
	for i := 0; i < 3; i++ {
		// Wait for the long path node to be created (upserter victory)
		for {
			_, _, err := s.kazoo.zkConn.Get(path)
			if err == nil {
				break
			}
		}
		// Wait for the root to be deleted (destructors victory)
		for {
			_, _, err := s.kazoo.zkConn.Get("/eeny")
			if err == zk.ErrNoNode {
				break
			}
		}
	}

	// Stop contenders and wait for them to terminate.
	close(cancelCh)
	wg.Wait()
}

func (s *KazooModelSuite) TestWatchChildren(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe"
	children, eventsCh, err := s.kazoo.watchZNodeChildren(path)
	c.Assert(err, IsNil)
	c.Assert(children, DeepEquals, []string{})

	select {
	case e := <-eventsCh:
		c.Errorf("Unexpected watch event %v", e)
	default:
	}

	err = s.kazoo.durableUpsertZNode(path+"/foo", nil, 0)
	c.Assert(err, IsNil)
	err = s.kazoo.durableUpsertZNode(path+"/bar", nil, 0)
	c.Assert(err, IsNil)

	select {
	case e := <-eventsCh:
		c.Assert(e, Equals, zk.Event{Type: zk.EventNodeChildrenChanged, State: 3, Path: path})
	case <-time.After(3 * time.Second):
		c.Error("Timeout waiting for watch event")
	}

	children, eventsCh, err = s.kazoo.watchZNodeChildren(path)
	c.Assert(err, IsNil)
	c.Assert(children, DeepEquals, []string{"bar", "foo"})
}

func (s *KazooModelSuite) TestChildrenWatchDelete(c *C) {
	s.kazoo.recursiveDeleteZNode("/eeny")

	path := "/eeny/meeny/miny/moe"
	children, eventsCh, err := s.kazoo.watchZNodeChildren(path)
	c.Assert(err, IsNil)
	c.Assert(children, DeepEquals, []string{})

	// When
	err = s.kazoo.zkConn.Delete(path, versionAny)
	c.Assert(err, IsNil)

	select {
	case e := <-eventsCh:
		c.Assert(e, Equals, zk.Event{Type: zk.EventNodeDeleted, State: 3, Path: path})
	case <-time.After(3 * time.Second):
		c.Error("Timeout waiting for watch event")
	}
}
