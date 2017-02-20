package service

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/frontend/httpsrv"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/mailgun/log"
	"github.com/pkg/errors"
)

type T struct {
	actorID *actor.ID
	proxies map[string]*proxy.T
	tcpSrv  *httpsrv.T
	unixSrv *httpsrv.T
	quitCh  chan struct{}
	wg      sync.WaitGroup
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
			return nil, errors.Wrapf(err, "failed to spawn proxy, name=%s", pxyAlias)
		}
		s.proxies[pxyAlias] = pxy
	}

	proxySet := proxy.NewSet(s.proxies, s.proxies[cfg.DefaultProxy])
	if s.tcpSrv, err = httpsrv.New(cfg.TCPAddr, proxySet); err != nil {
		s.stopProxies()
		return nil, errors.Wrap(err, "failed to start TCP socket based HTTP API server")
	}
	if cfg.UnixAddr != "" {
		if s.unixSrv, err = httpsrv.New(cfg.UnixAddr, proxySet); err != nil {
			s.stopProxies()
			return nil, errors.Wrapf(err, "failed to start Unix socket based HTTP API server")
		}
	}
	actor.Spawn(s.actorID, &s.wg, s.run)
	return s, nil
}

func (s *T) Stop() {
	close(s.quitCh)
	s.wg.Wait()
}

// run implements main supervisor loop, that boils down to starting all
// configured API servers, waiting for a stop signal and terminating everything
// gracefully.
func (s *T) run() {
	var unixServerErrorCh <-chan error

	s.tcpSrv.Start()
	if s.unixSrv != nil {
		s.unixSrv.Start()
		unixServerErrorCh = s.unixSrv.ErrorCh()
	}
	// Block to wait for quit signal or an API server crash.
	select {
	case <-s.quitCh:
	case err, ok := <-s.tcpSrv.ErrorCh():
		if ok {
			log.Errorf("Unix socket based HTTP API crashed: %+v", err)
		}
	case err, ok := <-unixServerErrorCh:
		if ok {
			log.Errorf("TCP socket based HTTP API crashed: %+v", err)
		}
	}
	// Initiate stop of all API servers.
	var wg sync.WaitGroup
	actor.Spawn(s.actorID.NewChild("tcpSrvStop"), &wg, s.tcpSrv.Stop)
	if s.unixSrv != nil {
		actor.Spawn(s.actorID.NewChild("unixSrvStop"), &wg, s.unixSrv.Stop)
	}
	wg.Wait()
	// There are no more requests in flight at this point so it is safe to stop
	// all proxies.
	s.stopProxies()
}

func (s *T) stopProxies() {
	var wg sync.WaitGroup
	for pxyAlias, pxy := range s.proxies {
		actor.Spawn(s.actorID.NewChild(fmt.Sprintf("%sStop", pxyAlias)), &wg, pxy.Stop)
	}
	wg.Wait()
}
