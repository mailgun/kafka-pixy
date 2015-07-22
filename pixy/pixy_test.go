package pixy

import (
	"os"
	"strings"
	"testing"

	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

const (
	// Use Shopify/sarama Vagrant box (copied over from https://github.com/Shopify/sarama/blob/master/functional_test.go#L18)
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	testKafkaPeers     []string
	testZookeeperPeers []string
)

func init() {
	kafkaPeersStr := os.Getenv("KAFKA_PEERS")
	if kafkaPeersStr == "" {
		kafkaPeersStr = VagrantKafkaPeers
	}
	testKafkaPeers = strings.Split(kafkaPeersStr, ",")

	zookeeperPeersStr := os.Getenv("ZOOKEEPER_PEERS")
	if zookeeperPeersStr == "" {
		zookeeperPeersStr = VagrantZookeeperPeers
	}
	testZookeeperPeers = strings.Split(zookeeperPeersStr, ",")
}

func Test(t *testing.T) {
	TestingT(t)
}
