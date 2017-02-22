package proxy

import (
	"github.com/pkg/errors"
)

// Set represents a collection of proxy.T instances with a default value.
type Set struct {
	proxies    map[string]*T
	defaultPxy *T
}

// NewSet creates a proxy.Set from an alias to proxy map and a default proxy.
func NewSet(proxies map[string]*T, defaultPxy *T) *Set {
	if len(proxies) < 1 {
		panic("set must contain at least one proxy")
	}
	if defaultPxy == nil {
		panic("default proxy must be provided")
	}
	return &Set{proxies: proxies, defaultPxy: defaultPxy}
}

// Get returns a proxy with the specified alias or the default proxy if there
// is no proxy with such alias.
func (s *Set) Get(alias string) (*T, error) {
	if alias == "" {
		return s.defaultPxy, nil
	}
	if pxy := s.proxies[alias]; pxy != nil {
		return pxy, nil
	}
	return nil, errors.Errorf("proxy `%s` does not exist", alias)
}
