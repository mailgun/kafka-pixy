package service

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/frontend/httpsrv"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/mailgun/log"
)

type T struct {
	actorID    *actor.ID
	proxies    map[string]*proxy.T
	tcpServer  *httpsrv.T
	unixServer *httpsrv.T
	quitCh     chan struct{}
	wg         sync.WaitGroup
}

func Spawn(cfg *config.App) (*T, error) {
	s := &T{
		actorID: actor.RootID.NewChild("service"),
		proxies: make(map[string]*proxy.T, len(cfg.Proxies)),
		quitCh:  make(chan struct{}),
	}
	var err error

	for pxyAlias, pxyCfg := range cfg.Proxies {
		pxy, err := proxy.Spawn(actor.RootID, pxyAlias, pxyCfg)
		if err != nil {
			s.stopProxies()
			return nil, fmt.Errorf("failed to spawn proxy, name=%s, err=(%s)", pxyAlias, err)
		}
		s.proxies[pxyAlias] = pxy
	}

	proxySet := proxy.NewSet(s.proxies, s.proxies[cfg.DefaultProxy])
	if s.tcpServer, err = httpsrv.New(cfg.TCPAddr, proxySet); err != nil {
		s.stopProxies()
		return nil, fmt.Errorf("failed to start TCP socket based HTTP API, err=(%s)", err)
	}
	if cfg.UnixAddr != "" {
		if s.unixServer, err = httpsrv.New(cfg.UnixAddr, proxySet); err != nil {
			s.stopProxies()
			return nil, fmt.Errorf("failed to start Unix socket based HTTP API, err=(%s)", err)
		}
	}
	actor.Spawn(s.actorID, &s.wg, s.run)
	return s, nil
}

func (s *T) Stop() {
	close(s.quitCh)
	s.wg.Wait()
}

// supervisor takes care of the service graceful shutdown.
func (s *T) run() {
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
	// all proxies.
	s.stopProxies()
}

func (s *T) stopProxies() {
	var wg sync.WaitGroup
	for pxyAlias, pxy := range s.proxies {
		actor.Spawn(s.actorID.NewChild(fmt.Sprintf("%s_stop", pxyAlias)), &wg, pxy.Stop)
	}
	wg.Wait()
}
