package logging

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/go-zookeeper/zk"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

var initTestOnce = sync.Once{}

// Init3rdParty makes the internal loggers of various 3rd-party libraries
// used by `kafka-pixy` forward their output to `mailgun/log` facility.
func Init3rdParty() {
	sarama.Logger = &loggerAdaptor{prefix: "sarama"}
	zk.DefaultLogger = &loggerAdaptor{prefix: "zk"}
}

// InitTest initializes both internal and 3rd party loggers to generate console
// output. As the name suggests it should be used in unit tests only.
func InitTest() {
	initTestOnce.Do(func() {
		consoleLogger, _ := log.NewConsoleLogger(log.Config{Severity: "info"})
		log.Init(consoleLogger)
		Init3rdParty()
	})
}

type loggerAdaptor struct {
	prefix string
}

func (la *loggerAdaptor) Print(v ...interface{}) {
	log.Logfmt(1, log.SeverityInfo, "[%s] %s", la.prefix, fmt.Sprint(v...))
}

func (la *loggerAdaptor) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] == '\n' {
		format = format[:len(format)-1]
	}
	log.Logfmt(1, log.SeverityInfo, "[%s] %s", la.prefix, fmt.Sprintf(format, v...))
}

func (la *loggerAdaptor) Println(v ...interface{}) {
	log.Logfmt(1, log.SeverityInfo, "[%s] %s", la.prefix, fmt.Sprint(v...))
}
