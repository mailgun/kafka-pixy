package service

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/mailgun/kafka-pixy/server"
	"github.com/mailgun/kafka-pixy/server/grpcsrv"
	"github.com/mailgun/kafka-pixy/server/httpsrv"
	"github.com/pkg/errors"
)

type T struct {
	actDesc *actor.Descriptor
	proxies map[string]*proxy.T
	servers []server.T
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

func Spawn(cfg *config.App) (*T, error) {
	s := &T{
		actDesc: actor.Root().NewChild("service"),
		proxies: make(map[string]*proxy.T, len(cfg.Proxies)),
		stopCh:  make(chan struct{}),
	}

	for cluster, pxyCfg := range cfg.Proxies {
		pxy, err := proxy.Spawn(actor.Root(), cluster, pxyCfg)
		if err != nil {
			s.stopProxies()
			return nil, errors.Wrapf(err, "failed to spawn proxy, name=%s", cluster)
		}
		s.proxies[cluster] = pxy
	}

	proxySet := proxy.NewSet(s.proxies, s.proxies[cfg.DefaultCluster])

	if cfg.GRPCAddr != "" {
		securityOpts, err := cfg.GRPCSecurityOpts()
		if err != nil {
			s.stopProxies()
			return nil, errors.Wrap(err, "failed to configure gRPC security")
		}
		grpcSrv, err := grpcsrv.New(cfg.GRPCAddr, proxySet, securityOpts...)
		if err != nil {
			s.stopProxies()
			return nil, errors.Wrap(err, "failed to start gRPC server")
		}
		s.servers = append(s.servers, grpcSrv)
	}
	if cfg.TCPAddr != "" {
		tcpSrv, err := httpsrv.New(cfg.TCPAddr, proxySet, cfg.TLS.CertPath, cfg.TLS.KeyPath)
		if err != nil {
			s.stopProxies()
			return nil, errors.Wrap(err, "failed to start TCP socket based HTTP API server")
		}
		s.servers = append(s.servers, tcpSrv)
	}
	if cfg.UnixAddr != "" {
		unixSrv, err := httpsrv.New(cfg.UnixAddr, proxySet, "", "")
		if err != nil {
			s.stopProxies()
			return nil, errors.Wrapf(err, "failed to start Unix socket based HTTP API server")
		}
		s.servers = append(s.servers, unixSrv)
	}

	if len(s.servers) == 0 {
		return nil, errors.Errorf("at least one API server should be configured")
	}

	actor.Spawn(s.actDesc, &s.wg, s.run)
	return s, nil
}

func (s *T) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// run implements main supervisor loop, that boils down to starting all
// configured API servers, waiting for a stop signal and terminating everything
// gracefully.
func (s *T) run() {
	selectCases := make([]reflect.SelectCase, len(s.servers)+1)
	for i, srv := range s.servers {
		srv.Start()
		selectCases[i] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(srv.ErrorCh()),
		}
	}
	selectCases[len(s.servers)] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(s.stopCh),
	}

	// Block until either an server error is reported or a Stop is called.
	chosen, val, ok := reflect.Select(selectCases)
	if chosen < len(s.servers) && ok {
		serverErr := val.Interface().(error)
		s.actDesc.Log().WithError(serverErr).Error("API server crashed")
	}

	s.actDesc.Log().Info("Shutting down")

	// Stop all proxies first. It is important to keep API servers running
	// so that offered messages can be acknowledged by consumers.
	s.stopProxies()
	s.actDesc.Log().Info("All proxies shutdown")

	// Stop all API servers.
	var wg sync.WaitGroup
	for _, srv := range s.servers {
		actor.Spawn(s.actDesc.NewChild("srv_stop"), &wg, srv.Stop)
	}
	wg.Wait()
	s.actDesc.Log().Info("All API servers shutdown")
}

func (s *T) stopProxies() {
	var wg sync.WaitGroup
	for pxyAlias, pxy := range s.proxies {
		actor.Spawn(s.actDesc.NewChild(fmt.Sprintf("%s_pxy_stop", pxyAlias)), &wg, pxy.Stop)
	}
	wg.Wait()
}
