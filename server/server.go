package server

// T represents a server usually based on http.Server.
type T interface {

	// Start triggers the server start. If the start is successful then it
	// keeps running in a dedicated goroutine until explicitly stopped.
	Start()

	// Stop stops the server gracefully, that is makes server cease to accept
	// new requests and blocks until all pending requests are over.
	Stop()

	// ErrorCh return an input channel that user should check for errors they
	// may have occurred on server startup.
	ErrorCh() <-chan error
}
