package log

import (
	"bytes"

	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

type SysLoggerSuite struct {
}

var _ = Suite(&SysLoggerSuite{})

func (s *SysLoggerSuite) TestWriter(c *C) {
	info, warning, error := &bytes.Buffer{}, &bytes.Buffer{}, &bytes.Buffer{}

	// INFO logger should log INFO, WARN and ERROR
	l := &sysLogger{SeverityInfo, info, warning, error}
	c.Assert(l.Writer(SeverityInfo), Equals, info)
	c.Assert(l.Writer(SeverityWarning), Equals, warning)
	c.Assert(l.Writer(SeverityError), Equals, error)

	// WARN logger should log WARN and ERROR
	l = &sysLogger{SeverityWarning, info, warning, error}
	c.Assert(l.Writer(SeverityInfo), IsNil)
	c.Assert(l.Writer(SeverityWarning), Equals, warning)
	c.Assert(l.Writer(SeverityError), Equals, error)

	// ERROR logger should log only ERROR
	l = &sysLogger{SeverityError, info, warning, error}
	c.Assert(l.Writer(SeverityInfo), IsNil)
	c.Assert(l.Writer(SeverityWarning), IsNil)
	c.Assert(l.Writer(SeverityError), Equals, error)
}

func (s *SysLoggerSuite) TestNewSysLogger(c *C) {
	l, err := NewSysLogger(Config{Syslog, "info"})
	c.Assert(err, IsNil)
	c.Assert(l, NotNil)

	syslog := l.(*sysLogger)
	c.Assert(syslog.sev, Equals, SeverityInfo)
	c.Assert(syslog.infoW, NotNil)
	c.Assert(syslog.warnW, NotNil)
	c.Assert(syslog.errorW, NotNil)
}
