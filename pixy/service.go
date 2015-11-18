package pixy

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	"github.com/mailgun/kafka-pixy/config"
)

type Service struct {
	producer   *GracefulProducer
	consumer   *SmartConsumer
	admin      *Admin
	tcpServer  *HTTPAPIServer
	unixServer *HTTPAPIServer
	quitCh     chan struct{}
	wg         sync.WaitGroup
}

func SpawnService(config *config.T) (*Service, error) {
	producer, err := SpawnGracefulProducer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn producer, err=(%s)", err)
	}
	consumer, err := SpawnSmartConsumer(config)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn consumer, err=(%s)", err)
	}
	admin, err := SpawnAdmin(config)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn admin, err=(%s)", err)
	}
	tcpServer, err := NewHTTPAPIServer(NetworkTCP, config.TCPAddr, producer, consumer, admin)
	if err != nil {
		producer.Stop()
		return nil, fmt.Errorf("failed to start TCP socket based HTTP API, err=(%s)", err)
	}
	var unixServer *HTTPAPIServer
	if config.UnixAddr != "" {
		unixServer, err = NewHTTPAPIServer(NetworkUnix, config.UnixAddr, producer, consumer, admin)
		if err != nil {
			producer.Stop()
			return nil, fmt.Errorf("failed to start Unix socket based HTTP API, err=(%s)", err)
		}
	}
	s := &Service{
		producer:   producer,
		consumer:   consumer,
		admin:      admin,
		tcpServer:  tcpServer,
		unixServer: unixServer,
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
	var unixServerErrorCh <-chan error

	s.tcpServer.Start()
	if s.unixServer != nil {
		s.unixServer.Start()
		unixServerErrorCh = s.unixServer.ErrorCh()
	}
	// Block to wait for quit signal or an API server crash.
	select {
	case <-s.quitCh:
	case err, ok := <-s.tcpServer.ErrorCh():
		if ok {
			log.Errorf("Unix socket based HTTP API crashed, err=(%s)", err)
		}
	case err, ok := <-unixServerErrorCh:
		if ok {
			log.Errorf("TCP socket based HTTP API crashed, err=(%s)", err)
		}
	}
	// Initiate stop of all API servers.
	s.tcpServer.AsyncStop()
	if s.unixServer != nil {
		s.unixServer.AsyncStop()
	}
	// Wait until all API servers are stopped.
	for range s.tcpServer.ErrorCh() {
		// Drain the errors channel until it is closed.
	}
	if s.unixServer != nil {
		for range s.unixServer.ErrorCh() {
			// Drain the errors channel until it is closed.
		}
	}
	// There are no more requests in flight at this point so it is safe to stop
	// all Kafka clients.
	var wg sync.WaitGroup
	spawn(&wg, s.producer.Stop)
	spawn(&wg, s.consumer.Stop)
	spawn(&wg, s.admin.Stop)
	wg.Wait()
}
