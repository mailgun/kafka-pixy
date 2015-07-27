package pixy

import (
	"fmt"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/samuel/go-zookeeper/zk"
)

// InitLibraryLoggers makes the internal loggers of various 3rd-party libraries
// used by `kafka-pixy` forward their output to `mailgun/log` facility.
func InitLibraryLoggers() {
	lp := &loggerProxy{}
	sarama.Logger = lp
	zk.DefaultLogger = lp
}

type loggerProxy struct{}

func (sl *loggerProxy) Print(v ...interface{}) {
	log.Infof(fmt.Sprint(v...))
}

func (sl *loggerProxy) Printf(format string, v ...interface{}) {
	if len(format) > 0 && format[len(format)-1] == '\n' {
		format = format[:len(format)-1]
	}
	log.Infof(format, v...)
}

func (sl *loggerProxy) Println(v ...interface{}) {
	log.Infof(fmt.Sprint(v...))
}
