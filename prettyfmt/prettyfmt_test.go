package prettyfmt

import (
	"testing"

	. "gopkg.in/check.v1"
)

type PrettyFmtSuite struct {
}

var _ = Suite(&PrettyFmtSuite{})

func Test(t *testing.T) {
	TestingT(t)
}

func (s *PrettyFmtSuite) TestCollapseJSON(c *C) {
	orig := `
		{
  			"a": [
				0,
				1,
				2,
				3,
				4,
				5,
				6,
				7
			],
			"b": [
				8
			],
			"c": [
			],
			"d": []
		}`
	transformed := CollapseJSON([]byte(orig))
	c.Assert(`
		{
  			"a": [0,1,2,3,4,5,6,7],
			"b": [8],
			"c": [],
			"d": []
		}`, Equals, string(transformed))
}
