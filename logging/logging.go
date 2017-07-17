package logging

import (
	"encoding/json"
	"io/ioutil"
	"log/syslog"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/logrus-hooks/kafkahook"
	"github.com/mailgun/logrus-hooks/levelfilter"
	"github.com/pkg/errors"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/syslog"
)

// Init initializes sirupsen/logrus hooks from the JSON config string. It also
// sets the sirupsen/logrus as a logger for 3rd party libraries.
func Init(jsonCfg string, cfg *config.App) error {
	var loggingCfg []loggerCfg
	if err := json.Unmarshal([]byte(jsonCfg), &loggingCfg); err != nil {
		return errors.Wrap(err, "failed to parse logger config")
	}

	stdoutEnabled := false
	nonStdoutEnabled := false
	for _, loggerCfg := range loggingCfg {
		switch loggerCfg.Name {
		case "console":
			stdoutEnabled = true
		case "syslog":
			h, err := logrus_syslog.NewSyslogHook("udp", "127.0.0.1:514", syslog.LOG_INFO|syslog.LOG_MAIL, "kafka-pixy")
			if err != nil {
				continue
			}
			log.AddHook(levelfilter.New(h, loggerCfg.level()))
			nonStdoutEnabled = true
		case "udplog":
			if cfg == nil {
				return errors.Errorf("App config must be provided")
			}
			// If a Kafka cluster is not specified in logging config or does
			// not exist in the Kafka-Pixy config, then the default cluster is
			// used.
			cluster := loggerCfg.Params["cluster"]
			proxyCfg := cfg.Proxies[cluster]
			if proxyCfg == nil {
				proxyCfg = cfg.Proxies[cfg.DefaultCluster]
			}
			// If the log topic is not specified then "udplog" is assumed.
			topic := loggerCfg.Params["topic"]
			if topic == "" {
				topic = "udplog"
			}
			h, err := kafkahook.New(kafkahook.Config{
				Endpoints: proxyCfg.Kafka.SeedPeers,
				Topic:     topic,
			})
			if err != nil {
				continue
			}
			log.AddHook(levelfilter.New(h, loggerCfg.level()))
			nonStdoutEnabled = true
		}
	}
	if !stdoutEnabled || nonStdoutEnabled {
		log.SetOutput(ioutil.Discard)
	}

	saramaLogger := log.New()
	saramaLogger.Formatter = &saramaFormatter{&log.TextFormatter{}}
	sarama.Logger = saramaLogger

	zk.DefaultLogger = log.StandardLogger()
	return nil
}

// loggerCfg represents a configuration of an individual logger.
type loggerCfg struct {
	// Name defines a logger to be used. It can be one of: console, syslog, or
	// udplog.
	Name string `json:"name"`

	// Severity indicates the minimum severity a logger will be logging messages at.
	Severity string `json:"severity"`

	// Logger parameters
	Params map[string]string `json:"params"`
}

func (lc *loggerCfg) level() log.Level {
	level, err := log.ParseLevel(lc.Severity)
	if err != nil {
		return log.WarnLevel
	}
	return level
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
