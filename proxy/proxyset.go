package proxy

import (
	"github.com/pkg/errors"
)

// Set represents a collection of proxy.T instances with a default value.
type Set struct {
	proxies    map[string]*T
	defaultPxy *T
}

// NewSet creates a proxy.Set from a cluster-to-proxy map and a default proxy.
func NewSet(proxies map[string]*T, defaultPxy *T) *Set {
	if len(proxies) < 1 {
		panic("set must contain at least one proxy")
	}
	if defaultPxy == nil {
		panic("default proxy must be provided")
	}
	return &Set{proxies: proxies, defaultPxy: defaultPxy}
}

// Get returns a proxy for a cluster name. If there is no proxy configured for
// the cluster name, then the default proxy is returned.
func (s *Set) Get(cluster string) (*T, error) {
	if cluster == "" {
		return s.defaultPxy, nil
	}
	if pxy := s.proxies[cluster]; pxy != nil {
		return pxy, nil
	}
	return nil, errors.Errorf("proxy `%s` does not exist", cluster)
}
