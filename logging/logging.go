package logging

import (
	"fmt"

	"github.com/mailgun/log"
	"github.com/mailgun/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

// Init3rdParty makes the internal loggers of various 3rd-party libraries
// used by `kafka-pixy` forward their output to `mailgun/log` facility.
func Init3rdParty() {
	sarama.Logger = &loggerAdaptor{prefix: "sarama"}
	zk.DefaultLogger = &loggerAdaptor{prefix: "zk"}
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
