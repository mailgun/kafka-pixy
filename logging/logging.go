package logging

import (
	"encoding/json"
	"log/syslog"
	"os"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	syslogrus "github.com/sirupsen/logrus/hooks/syslog"
)

// Init initializes sirupsen/logrus hooks from the JSON config string. It also
// sets the sirupsen/logrus as a logger for 3rd party libraries.
func Init(jsonCfg string, cfg *config.App) error {
	var formatter log.Formatter
	var loggingCfg []config.LoggerCfg

	if len(jsonCfg) != 0 {
		if err := json.Unmarshal([]byte(jsonCfg), &loggingCfg); err != nil {
			return errors.Wrap(err, "failed to parse logger config")
		}
	}

	// Prefer the command line logging config over the config file
	if loggingCfg == nil {
		loggingCfg = cfg.Logging
	}

	log.SetFormatter(newJSONFormatter())
	log.StandardLogger().Out = os.Stdout

	var hooks []log.Hook
	for _, loggerCfg := range loggingCfg {
		switch loggerCfg.Name {
		case "console":
			log.StandardLogger().Out = os.Stdout
		case "syslog":
			h, err := syslogrus.NewSyslogHook("udp", "127.0.0.1:514", syslog.LOG_INFO|syslog.LOG_MAIL, "kafka-pixy")
			if err != nil {
				continue
			}
			hooks = append(hooks, newLevelFilter(h, loggerCfg.Level()))
		case "json":
			formatter = newJSONFormatter()
			log.SetFormatter(formatter)
			log.StandardLogger().Out = os.Stdout
		}
	}

	// samuel/go-zookeeper/zk is using the standard logger.
	zk.DefaultLogger = log.WithField("category", "zk")

	// Shopify/sarama formatter removes trailing `\n` from log entries
	saramaLogger := log.New()
	saramaLogger.Out = log.StandardLogger().Out
	saramaLogger.Formatter = &saramaFormatter{formatter}
	sarama.Logger = saramaLogger.WithField("category", "sarama")

	for _, hook := range hooks {
		saramaLogger.Hooks.Add(hook)
		log.StandardLogger().Hooks.Add(hook)
	}

	return nil
}

// saramaFormatter is a sirupsen/logrus formatter that strips trailing new
// lines from the log lines.
type saramaFormatter struct {
	parentFormatter log.Formatter
}

func (f *saramaFormatter) Format(entry *log.Entry) ([]byte, error) {
	lastByteIdx := len(entry.Message) - 1
	if lastByteIdx >= 0 && entry.Message[lastByteIdx] == '\n' {
		entry.Message = entry.Message[:lastByteIdx]
	}
	return f.parentFormatter.Format(entry)
}
