package pixy

import (
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

type Config struct {
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
	Producer struct {
		// The period of time that a proxy should allow to `sarama.Producer` to
		// submit buffered messages to Kafka. It should be large enough to avoid
		// event loss when shutdown is performed during Kafka leader election.
		ShutdownTimeout time.Duration
		// DeadMessageCh is a channel to dump undelivered messages into. It is
		// used in testing only.
		DeadMessageCh chan<- *sarama.ProducerMessage
	}
	// All buffered channels created by the service will have this size.
	ChannelBufferSize int
}

func NewConfig() *Config {
	config := &Config{}
	config.ClientID = newClientID()
	config.Producer.ShutdownTimeout = 30 * time.Second
	config.ChannelBufferSize = 256
	return config
}

// saramaConfig generates a `Shopify/sarama` library config from this config.
func (c *Config) saramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = c.ClientID
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true
	saramaConfig.Producer.Compression = sarama.CompressionSnappy
	saramaConfig.Producer.Retry.Backoff = time.Second
	saramaConfig.Producer.Flush.Frequency = 500 * time.Millisecond
	saramaConfig.Producer.Flush.Bytes = 1024 * 1024
	return saramaConfig
}

type Service struct {
	producer   *GracefulProducer
	unixServer *HTTPAPIServer
	tcpServer  *HTTPAPIServer
	quitCh     chan struct{}
	wg         sync.WaitGroup
}

func SpawnService(config *Config) (*Service, error) {
	producer, err := SpawnGracefulProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn Kafka client, cause=(%v)", err)
	}
	unixServer, err := NewHTTPAPIServer(NetworkUnix, config.UnixAddr, producer)
	if err != nil {
		producer.Stop()
		return nil, fmt.Errorf("failed to start Unix socket based HTTP API, cause=(%v)", err)
	}
	var tcpServer *HTTPAPIServer
	if config.TCPAddr != "" {
		if tcpServer, err = NewHTTPAPIServer(NetworkTCP, config.TCPAddr, producer); err != nil {
			producer.Stop()
			return nil, fmt.Errorf("failed to start TCP socket based HTTP API, cause=(%v)", err)
		}
	}
	s := &Service{
		producer:   producer,
		unixServer: unixServer,
		tcpServer:  tcpServer,
		quitCh:     make(chan struct{}),
	}
	spawn(&s.wg, s.supervisor)
	return s, nil
}

func (s *Service) Stop() {
	close(s.quitCh)
	s.wg.Wait()
}

// supervisor takes care of the service graceful shutdown.
func (s *Service) supervisor() {
	defer sarama.RootCID.NewChild("supervisor").LogScope()()
	var tcpServerErrorCh <-chan error

	s.unixServer.Start()
	if s.tcpServer != nil {
		s.tcpServer.Start()
		tcpServerErrorCh = s.tcpServer.ErrorCh()
	}
	// Block to wait for quit signal or an API server crash.
	select {
	case <-s.quitCh:
	case err, ok := <-s.unixServer.ErrorCh():
		if ok {
			log.Errorf("Unix socket based HTTP API crashed, cause=(%v)", err)
		}
	case err, ok := <-tcpServerErrorCh:
		if ok {
			log.Errorf("TCP socket based HTTP API crashed, cause=(%v)", err)
		}
	}
	// Initiate stop of all API servers.
	s.unixServer.AsyncStop()
	if s.tcpServer != nil {
		s.tcpServer.AsyncStop()
	}
	// Wait until all API servers are stopped.
	<-s.unixServer.ErrorCh()
	if s.tcpServer != nil {
		<-s.tcpServer.ErrorCh()
	}
	// Only when all API servers are stopped it is safe to stop the Kafka client.
	s.producer.Stop()
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
