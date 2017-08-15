package actor

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type IDSuite struct{}

var _ = Suite(&IDSuite{})

func (s *IDSuite) TestRootID(c *C) {
	c.Assert(fmt.Sprintf("%s", Root()), Equals, "")
}

func (s *IDSuite) TestNewChild(c *C) {
	id1 := root.NewChild("foo")
	c.Assert("/foo.0", Equals, fmt.Sprintf("%s", id1))
	id2 := id1.NewChild("bar")
	c.Assert("/foo.0/bar.0", Equals, fmt.Sprintf("%s", id2))
	id3 := id1.NewChild("bar")
	c.Assert("/foo.0/bar.1", Equals, fmt.Sprintf("%s", id3))
	id4 := id1.NewChild("bazz")
	c.Assert("/foo.0/bazz.0", Equals, fmt.Sprintf("%s", id4))
	id5 := id4.NewChild("blah")
	c.Assert("/foo.0/bazz.0/blah.0", Equals, fmt.Sprintf("%s", id5))

	id6 := root.NewChild("foo")
	c.Assert("/foo.1", Equals, fmt.Sprintf("%s", id6))
	id7 := id6.NewChild("bar")
	c.Assert("/foo.1/bar.0", Equals, fmt.Sprintf("%s", id7))
	id8 := id6.NewChild("bar")
	c.Assert("/foo.1/bar.1", Equals, fmt.Sprintf("%s", id8))
	id9 := id6.NewChild("bar")
	c.Assert("/foo.1/bar.2", Equals, fmt.Sprintf("%s", id9))
}

func (s *IDSuite) TestNewChildEmpty(c *C) {
	id := root.NewChild("foo").NewChild("bar")
	c.Assert(id.NewChild(), Equals, id)
}

func (s *IDSuite) TestNewChildComplex(c *C) {
	c.Assert(root.NewChild("foo", 0, []string{"d"}, nil, "bar").String(), Equals, "/foo_0_[d]_<nil>_bar.0")
}
