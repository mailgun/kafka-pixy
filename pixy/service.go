package pixy

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

type ServiceCfg struct {
	UnixAddr    string
	TCPAddr     string
	BrokerAddrs []string
}

type Service struct {
	kafkaClient *KafkaClientImpl
	unixServer  *HTTPAPIServer
	tcpServer   *HTTPAPIServer
	quitCh      chan struct{}
	wg          sync.WaitGroup
}

func SpawnService(cfg *ServiceCfg) (*Service, error) {
	kafkaClientCfg := NewKafkaClientCfg()
	kafkaClientCfg.BrokerAddrs = cfg.BrokerAddrs
	kafkaClient, err := SpawnKafkaClient(kafkaClientCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to spawn Kafka client, cause=(%v)", err)
	}

	unixServer, err := NewHTTPAPIServer(NetworkUnix, cfg.UnixAddr, kafkaClient)
	if err != nil {
		kafkaClient.Stop()
		return nil, fmt.Errorf("failed to start Unix socket based HTTP API, cause=(%v)", err)
	}

	var tcpServer *HTTPAPIServer
	if cfg.TCPAddr != "" {
		if tcpServer, err = NewHTTPAPIServer(NetworkTCP, cfg.TCPAddr, kafkaClient); err != nil {
			kafkaClient.Stop()
			return nil, fmt.Errorf("failed to start TCP socket based HTTP API, cause=(%v)", err)
		}
	}

	s := &Service{
		kafkaClient: kafkaClient,
		unixServer:  unixServer,
		tcpServer:   tcpServer,
		quitCh:      make(chan struct{}),
	}

	goGo(&s.wg, s.supervisor)
	return s, nil
}

func (s *Service) Stop() {
	close(s.quitCh)
}

func (s *Service) Wait4Stop() {
	s.wg.Wait()
}

// supervisor takes care of the service graceful shutdown.
func (s *Service) supervisor() {
	defer logScope("Service Supervisor")()
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
	s.kafkaClient.Stop()
}
