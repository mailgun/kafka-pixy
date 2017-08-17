package testhelpers

import (
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/logging"
)

const (
	VagrantKafkaPeers     = "192.168.100.67:9091"
	VagrantZookeeperPeers = "192.168.100.67:2181"
)

var (
	KafkaPeers     []string
	ZookeeperPeers []string

	initTestOnce = sync.Once{}
)

func init() {
	kafkaPeersStr := os.Getenv("KAFKA_PEERS")
	if kafkaPeersStr == "" {
		kafkaPeersStr = VagrantKafkaPeers
	}
	KafkaPeers = strings.Split(kafkaPeersStr, ",")

	zookeeperPeersStr := os.Getenv("ZOOKEEPER_PEERS")
	if zookeeperPeersStr == "" {
		zookeeperPeersStr = VagrantZookeeperPeers
	}
	ZookeeperPeers = strings.Split(zookeeperPeersStr, ",")
}

// InitLogging initializes both internal and 3rd party loggers to output logs
// using the test context object's `Log` function.
func InitLogging() {
	initTestOnce.Do(func() {
		logging.Init(`[{"name": "console"}]`, nil)
	})
}

func NewTestProxyCfg(clientID string) *config.Proxy {
	cfg := config.DefaultProxy()
	cfg.ClientID = clientID
	cfg.Kafka.SeedPeers = KafkaPeers
	cfg.ZooKeeper.SeedPeers = ZookeeperPeers
	cfg.Consumer.LongPollingTimeout = 3000 * time.Millisecond
	cfg.Consumer.RetryBackoff = 100 * time.Millisecond
	cfg.Consumer.RebalanceDelay = 100 * time.Millisecond
	cfg.Consumer.AckTimeout = 100 * time.Millisecond
	cfg.Consumer.OffsetsCommitInterval = 100 * time.Millisecond
	return cfg
}

// NewUDSHTTPClient creates an HTTP client that always connects to the
// specified unix domain socket ignoring the host part of requested HTTP URLs.
func NewUDSHTTPClient(unixSockAddr string) *http.Client {
	dial := func(proto, addr string) (net.Conn, error) {
		return net.Dial("unix", unixSockAddr)
	}
	tr := &http.Transport{Dial: dial}
	return &http.Client{Transport: tr}
}
