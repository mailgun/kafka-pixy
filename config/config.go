package config

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/wvanbergen/kazoo-go"
)

type T struct {
	// A unix domain socket address that the service should listen at.
	UnixAddr string
	// A TCP address that the service should listen at.
	TCPAddr string
	// A unique id that identifies this particular Kafka-Pixy instance in both
	// Kafka and ZooKeeper.
	ClientID string

	Kafka struct {
		// A list of seed Kafka peers in the form "<host>:<port>" that the
		// service will try to connect to to resolve the cluster topology.
		SeedPeers []string
	}
	ZooKeeper struct {
		// A list of seed ZooKeeper peers in the form "<host>:<port>" that the
		// service will try to connect to to resolve the cluster topology.
		SeedPeers []string
		// The root directory where Kafka keeps all its znodes.
		Chroot string
	}
	Producer struct {
		// The period of time that a proxy should allow to `sarama.Producer` to
		// submit buffered messages to Kafka. It should be large enough to avoid
		// event loss when shutdown is performed during Kafka leader election.
		ShutdownTimeout time.Duration
		// DeadMessageCh is a channel to dump undelivered messages into. It is
		// used in testing only.
		DeadMessageCh chan<- *sarama.ProducerMessage
	}
	Consumer struct {
		// A consume request will wait at most this long until a message from
		// the specified group/topic becomes available. This timeout is
		// necessary to account for consumer rebalancing that happens whenever
		// a new consumer joins a group or subscribes to a topic.
		LongPollingTimeout time.Duration
		// The period of time that a proxy should keep registration with a
		// consumer group or subscription for a topic in the absence of requests
		// to the aforementioned consumer group or topic.
		RegistrationTimeout time.Duration
		// If a request to a KafkaBroker fails for any reason then the proxy
		// should wait this long before retrying.
		BackOffTimeout time.Duration
		// A consumer should wait this long after it gets notification that a
		// consumer joined/left its consumer group before it should rebalance.
		RebalanceDelay time.Duration
	}
	// All buffered channels created by the service will have this size.
	ChannelBufferSize int
}

func Default() *T {
	config := &T{}
	config.ClientID = newClientID()
	config.ChannelBufferSize = 256

	config.Producer.ShutdownTimeout = 30 * time.Second

	config.Consumer.LongPollingTimeout = 3 * time.Second
	config.Consumer.RegistrationTimeout = 20 * time.Second
	config.Consumer.BackOffTimeout = 500 * time.Millisecond
	config.Consumer.RebalanceDelay = 250 * time.Millisecond

	return config
}

// SaramaConfig generates a `Shopify/sarama` library config.
func (c *T) SaramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = c.ClientID
	saramaConfig.ChannelBufferSize = c.ChannelBufferSize

	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Compression = sarama.CompressionSnappy
	saramaConfig.Producer.Retry.Backoff = 4 * time.Second
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	saramaConfig.Producer.Flush.Bytes = 1024 * 1024

	saramaConfig.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	saramaConfig.Consumer.Retry.Backoff = c.Consumer.BackOffTimeout
	saramaConfig.Consumer.Fetch.Default = 1024 * 1024

	return saramaConfig
}

// KazooConfig generates a `wvanbergen/kazoo-go` library config.
func (c *T) KazooConfig() *kazoo.Config {
	kazooConfig := kazoo.NewConfig()
	kazooConfig.Chroot = c.ZooKeeper.Chroot
	// ZooKeeper documentation says following about the session timeout: "The
	// current (ZooKeeper) implementation requires that the timeout be a
	// minimum of 2 times the tickTime (as set in the server configuration) and
	// a maximum of 20 times the tickTime". The default tickTime is 2 seconds.
	//
	// See http://zookeeper.apache.org/doc/trunk/zookeeperProgrammers.html#ch_zkSessions
	kazooConfig.Timeout = 15 * time.Second
	return kazooConfig
}

// newClientID creates a unique id that identifies this particular Kafka-Pixy
// in both Kafka and ZooKeeper.
func newClientID() string {
	hostname, err := os.Hostname()
	if err != nil {
		ip, err := getIP()
		if err != nil {
			buffer := make([]byte, 8)
			_, _ = rand.Read(buffer)
			hostname = fmt.Sprintf("%X", buffer)

		} else {
			hostname = ip.String()
		}
	}
	timestamp := time.Now().UTC().Format(time.RFC3339)
	return fmt.Sprintf("pixy_%s_%d_%s", hostname, os.Getpid(), timestamp)
}

func getIP() (net.IP, error) {
	interfaceAddrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	var ipv6 net.IP
	for _, interfaceAddr := range interfaceAddrs {
		if ipAddr, ok := interfaceAddr.(*net.IPNet); ok && !ipAddr.IP.IsLoopback() {
			ipv4 := ipAddr.IP.To4()
			if ipv4 != nil {
				return ipv4, nil
			}
			ipv6 = ipAddr.IP
		}
	}
	if ipv6 != nil {
		return ipv6, nil
	}
	return nil, errors.New("Unknown IP address")
}
