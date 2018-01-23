package main

import (
	"bytes"
	"io"
	"testing"

	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}

type EventReaderSuite struct{}

var _ = Suite(&EventReaderSuite{})

func (s *EventReaderSuite) TestParseFromReader(c *C) {
	source := bytes.NewBuffer([]byte("first-message\rsecond-message\r{\"third\":\"message\"}"))

	reader := NewEventReader(source)
	event, err := reader.ReadEvent()
	c.Assert(err, IsNil)
	c.Assert(string(event), Equals, "first-message")

	event, err = reader.ReadEvent()
	c.Assert(err, IsNil)
	c.Assert(string(event), Equals, "second-message")

	event, err = reader.ReadEvent()
	c.Assert(err, IsNil)
	c.Assert(string(event), Equals, `{"third":"message"}`)

	event, err = reader.ReadEvent()
	c.Assert(err, Equals, io.EOF)
	c.Assert(string(event), Equals, "")
}
