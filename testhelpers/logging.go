package testhelpers

import (
	"fmt"
	"io"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
	"github.com/mailgun/kafka-pixy/logging"
)

// InitLogging initializes both internal and 3rd party loggers to output logs
// using the test context object's `Log` function.
func InitLogging(c *C) {
	initTestOnce.Do(func() {
		log.Init(&logger{c})
		logging.Init3rdParty()
	})
}

// logger is a type of writerLogger that sends messages to the standard output.
type logger struct {
	c *C
}

func (l *logger) Writer(sev log.Severity) io.Writer {
	return l
}

func (l *logger) SetSeverity(sev log.Severity) {
}

func (l *logger) GetSeverity() log.Severity {
	return log.SeverityDebug
}

func (l *logger) Write(p []byte) (n int, err error) {
	l.c.Logf("%s", p)
	return len(p), nil
}

func (l *logger) FormatMessage(sev log.Severity, caller *log.CallerInfo, format string, args ...interface{}) string {
	return fmt.Sprintf("%v %-5s %s",
		time.Now().UTC().Format(time.StampMilli), sev, fmt.Sprintf(format, args...))
}
