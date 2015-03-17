package manners

import (
	"net"
	"net/http"
	"sync"
)

var (
	servers []*GracefulServer
	m       sync.Mutex
)

// ListenAndServe provides a graceful version of the function provided by the net/http package.
// Call Close() to stop all started servers.
func ListenAndServe(addr string, handler http.Handler) error {
	server := NewWithServer(&http.Server{Addr: addr, Handler: handler})
	m.Lock()
	servers = append(servers, server)
	m.Unlock()
	return server.ListenAndServe()
}

// ListenAndServeTLS provides a graceful version of the function provided by the net/http package.
// Call Close() to stop all started servers.
func ListenAndServeTLS(addr string, certFile string, keyFile string, handler http.Handler) error {
	server := NewWithServer(&http.Server{Addr: addr, Handler: handler})
	m.Lock()
	servers = append(servers, server)
	m.Unlock()
	return server.ListenAndServeTLS(certFile, keyFile)
}

// Serve provides a graceful version of the function provided by the net/http package.
// Call Close() to stop all started servers.
func Serve(l net.Listener, handler http.Handler) error {
	server := NewWithServer(&http.Server{Handler: handler})
	m.Lock()
	servers = append(servers, server)
	m.Unlock()
	return server.Serve(l)
}

// Close triggers a shutdown of all running Graceful servers.
// Call Close() to stop all started servers.
func Close() {
	m.Lock()
	for _, s := range servers {
		s.Close()
	}
	servers = nil
	m.Unlock()
}
