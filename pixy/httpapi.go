package pixy

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/manners"
)

const (
	NetworkTCP  = "tcp"
	NetworkUnix = "unix"

	// HTTP Headers:
	contentLength = "Content-Length"

	// API call parameters:
	topicName  = "topic"
	routingKey = "key"
)

type HTTPAPIServer struct {
	addr       string
	listener   net.Listener
	httpServer *manners.GracefulServer
	kafkaProxy KafkaProxy
	errorCh    chan error
}

// SpawnHTTPAPIServer starts an HTTP server instance that accepts API requests
// on the specified network/address and forwards them to the associated
// Kafka client. The server initialization is performed asynchronously and
// if it fails then the error is sent down to `HTTPAPIServer.ErrorCh()`.
func SpawnHTTPAPIServer(network, addr string, kafkaProxy KafkaProxy) (*HTTPAPIServer, error) {
	as, err := NewHTTPAPIServer(network, addr, kafkaProxy)
	if err != nil {
		return nil, err
	}
	as.Start()
	return as, nil
}

// NewHTTPAPIServer creates an HTTP server instance that will accept API
// requests specified network/address and forwards them to the associated
// Kafka client.
func NewHTTPAPIServer(network, addr string, kafkaProxy KafkaProxy) (*HTTPAPIServer, error) {
	if kafkaProxy == nil {
		return nil, fmt.Errorf("kafkaProxy must be specified")
	}
	// Start listening on the specified unix domain socket address.
	listener, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener, cause=(%v)", err)
	}
	// Create a graceful HTTP server instance.
	router := mux.NewRouter()
	httpServer := manners.NewWithServer(&http.Server{Handler: router})
	as := &HTTPAPIServer{
		addr:       addr,
		listener:   manners.NewListener(listener),
		httpServer: httpServer,
		kafkaProxy: kafkaProxy,
		errorCh:    make(chan error, 1),
	}
	// Configure the API request handlers.
	produceUrl := fmt.Sprintf("/topics/{%s}", topicName)
	router.HandleFunc(produceUrl, as.handleProduce).Methods("POST")
	return as, nil
}

// Starts triggers asynchronous HTTP server start. If it fails then the error
// will be sent down to `HTTPAPIServer.ErrorCh()`.
func (as *HTTPAPIServer) Start() {
	goGo(fmt.Sprintf("API@%s", as.addr), nil, func() {
		defer close(as.errorCh)
		if err := as.httpServer.Serve(as.listener); err != nil {
			as.errorCh <- fmt.Errorf("HTTP API listener failed, cause=(%v)", err)
		}
	})
}

// ErrorCh returns an output channel that HTTP server running in another
// goroutine will use if it stops with error if one occurs. The channel will be
// closed when the server is fully stopped due to an error or otherwise..
func (as *HTTPAPIServer) ErrorCh() <-chan error {
	return as.errorCh
}

// Stop triggers HTTP API listener stop. The caller should wait on `wg` passed
// to the respective call of `Start` if it needs to know when the lister is
// stopped and all pending requests has completed gracefully.
func (as *HTTPAPIServer) Stop() {
	as.httpServer.Close()
}

// handleProduce is an HTTP request handler for `POST /topic/{topic-name}`
func (as *HTTPAPIServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[topicName]
	key := getParamBytes(r, routingKey)

	// Get the message body from the HTTP request.
	if _, ok := r.Header[contentLength]; !ok {
		errorText := fmt.Sprintf("Missing %s header", contentLength)
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}
	messageSizeStr := r.Header.Get(contentLength)
	messageSize, err := strconv.Atoi(messageSizeStr)
	if err != nil {
		errorText := fmt.Sprintf("Invalid %s header: %s", contentLength, messageSizeStr)
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}

	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read a message: cause=(%v)", err)
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}
	if len(message) != messageSize {
		errorText := fmt.Sprintf("Message size does not match %s: expected=%v, actual=%v",
			contentLength, messageSize, len(message))
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}

	// Asynchronously submit the message to the Kafka cluster.
	as.kafkaProxy.Produce(topic, key, message)
	w.WriteHeader(http.StatusOK)
}

// getParamBytes returns the request parameter as a slice of bytes. It works
// pretty much the same way as `http.FormValue`, except it distinguishes empty
// value (`[]byte{}`) from missing one (`nil`).
func getParamBytes(r *http.Request, name string) []byte {
	r.ParseForm() // Ignore errors, the go library does the same in FormValue.
	values, ok := r.Form[name]
	if !ok || len(values) == 0 {
		return nil
	}
	return []byte(values[0])
}
