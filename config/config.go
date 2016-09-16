package config

import (
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
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
		// Size of all buffered channels created by the producer components.
		ChannelBufferSize int
		// The period of time that a proxy should allow to `sarama.Producer` to
		// submit buffered messages to Kafka. It should be large enough to avoid
		// event loss when shutdown is performed during Kafka leader election.
		ShutdownTimeout time.Duration
	}
	Consumer struct {
		// Size of all buffered channels created by the consumer components.
		ChannelBufferSize int
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
		// How frequently to commit updated offsets. Defaults to 0.5s.
		OffsetsCommitInterval time.Duration
	}
}

func Default() *T {
	config := &T{}
	config.ClientID = newClientID()

	config.Producer.ChannelBufferSize = 4096
	config.Producer.ShutdownTimeout = 30 * time.Second

	config.Consumer.ChannelBufferSize = 64
	config.Consumer.LongPollingTimeout = 3 * time.Second
	config.Consumer.RegistrationTimeout = 20 * time.Second
	config.Consumer.BackOffTimeout = 500 * time.Millisecond
	config.Consumer.RebalanceDelay = 250 * time.Millisecond
	config.Consumer.OffsetsCommitInterval = 500 * time.Millisecond

	return config
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
	// sarama validation regexp for the client ID doesn't allow ':' characters
	timestamp = strings.Replace(timestamp, ":", ".", -1)
	return fmt.Sprintf("pixy_%s_%s_%d", hostname, timestamp, os.Getpid())
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
