package pixy

import (
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type UtilsSuite struct{}

var _ = Suite(&UtilsSuite{})

func (s *UtilsSuite) TestGetIP(c *C) {
	ip, err := getIP()
	c.Assert(err, IsNil)
	c.Assert(ip.String(), Matches, "\\d+.\\d+.\\d+.\\d+")
	c.Assert(ip.String(), Not(Equals), "127.0.0.1")
}
