package prettyfmt

import (
	"fmt"
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

func (s *PrettyFmtSuite) TestFormatVal(c *C) {
	for i, tc := range []struct {
		val       interface{}
		formatted string
	}{{
		val: map[string][]string{
			"a": {"b", "c"},
			"d": {},
			"e": {"f"},
		},
		formatted: "" +
			"{\n" +
			"    a: [b c]\n" +
			"    d: []\n" +
			"    e: [f]\n" +
			"}",
	}, {
		val: map[string][]int{
			"a": {15, 3},
			"d": {},
			"e": {7},
		},
		formatted: "" +
			"{\n" +
			"    a: [15 3]\n" +
			"    d: []\n" +
			"    e: [7]\n" +
			"}",
	}, {
		val: map[foo][]foo{
			foo{"a"}: {foo{"b"}, foo{"c"}},
			foo{"d"}: {},
			foo{"e"}: {foo{"f"}},
		},
		formatted: "" +
			"{\n" +
			"    {a}: [{b} {c}]\n" +
			"    {d}: []\n" +
			"    {e}: [{f}]\n" +
			"}",
	}, {
		val: map[bar][]bar{
			bar{"a"}: {bar{"b"}, bar{"c"}},
			bar{"d"}: {},
			bar{"e"}: {bar{"f"}},
		},
		formatted: "" +
			"{\n" +
			"    #a: [#b #c]\n" +
			"    #d: []\n" +
			"    #e: [#f]\n" +
			"}",
	}, {
		val: map[bazz][]bazz{
			bazz{"a"}: {bazz{"b"}, bazz{"c"}},
			bazz{"d"}: {},
			bazz{"e"}: {bazz{"f"}},
		},
		formatted: "" +
			"{\n" +
			"    $a: [$b $c]\n" +
			"    $d: []\n" +
			"    $e: [$f]\n" +
			"}",
	}} {
		fmt.Printf("Test case #%d\n", i)
		c.Assert(Val(tc.val), Equals, tc.formatted)
	}
}

type foo struct {
	s string
}

// bar is a test struct with a pointer String receiver.
type bar struct {
	s string
}

func (b *bar) String() string {
	return fmt.Sprintf("#%s", b.s)
}

// bazz is a test struct with a value String receiver.
type bazz struct {
	s string
}

func (b bazz) String() string {
	return fmt.Sprintf("$%s", b.s)
}
