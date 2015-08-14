package pixy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/gorilla/mux"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/manners"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

const (
	NetworkTCP  = "tcp"
	NetworkUnix = "unix"

	// HTTP headers used by the API.
	HeaderContentLength = "Content-Length"
	HeaderContentType   = "Content-Type"

	// HTTP request parameters.
	ParamTopic = "topic"
	ParamKey   = "key"
	ParamSync  = "sync"
	ParamGroup = "group"
)

type HTTPAPIServer struct {
	addr       string
	listener   net.Listener
	httpServer *manners.GracefulServer
	producer   *GracefulProducer
	consumer   *SmartConsumer
	errorCh    chan error
}

// NewHTTPAPIServer creates an HTTP server instance that will accept API
// requests specified network/address and forwards them to the associated
// Kafka client.
func NewHTTPAPIServer(network, addr string, producer *GracefulProducer, consumer *SmartConsumer) (*HTTPAPIServer, error) {
	if producer == nil {
		return nil, fmt.Errorf("producer must be specified")
	}
	if consumer == nil {
		return nil, fmt.Errorf("consumer must be specified")
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
		producer:   producer,
		consumer:   consumer,
		errorCh:    make(chan error, 1),
	}
	// Configure the API request handlers.
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", ParamTopic),
		as.handleProduce).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", ParamTopic),
		as.handleConsume).Methods("GET")
	// TODO deprecated endpoint, use `/topics/{topic}/messages` instead.
	router.HandleFunc(fmt.Sprintf("/topics/{%s}", ParamTopic),
		as.handleProduce).Methods("POST")
	return as, nil
}

// Starts triggers asynchronous HTTP server start. If it fails then the error
// will be sent down to `HTTPAPIServer.ErrorCh()`.
func (as *HTTPAPIServer) Start() {
	go func() {
		hid := sarama.RootCID.NewChild(fmt.Sprintf("API@%s", as.addr))
		defer hid.LogScope()()
		defer close(as.errorCh)
		if err := as.httpServer.Serve(as.listener); err != nil {
			as.errorCh <- fmt.Errorf("HTTP API listener failed, cause=(%v)", err)
		}
	}()
}

// ErrorCh returns an output channel that HTTP server running in another
// goroutine will use if it stops with error if one occurs. The channel will be
// closed when the server is fully stopped due to an error or otherwise..
func (as *HTTPAPIServer) ErrorCh() <-chan error {
	return as.errorCh
}

// AsyncStop triggers HTTP API listener stop. If a caller wants to know when
// the server terminates it should read from the `Error()` channel that will be
// closed upon server termination.
func (as *HTTPAPIServer) AsyncStop() {
	as.httpServer.Close()
}

// handleProduce is an HTTP request handler for `POST /topic/{topic}/messages`
func (as *HTTPAPIServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[ParamTopic]
	key := getParamBytes(r, ParamKey)
	_, isSync := r.Form[ParamSync]

	// Get the message body from the HTTP request.
	if _, ok := r.Header[HeaderContentLength]; !ok {
		errorText := fmt.Sprintf("Missing %s header", HeaderContentLength)
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}
	messageSizeStr := r.Header.Get(HeaderContentLength)
	messageSize, err := strconv.Atoi(messageSizeStr)
	if err != nil {
		errorText := fmt.Sprintf("Invalid %s header: %s", HeaderContentLength, messageSizeStr)
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
			HeaderContentLength, messageSize, len(message))
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}

	if isSync {
		_, err := as.producer.Produce(topic, toEncoderPreservingNil(key), sarama.StringEncoder(message))
		switch err {
		case nil:
			w.WriteHeader(http.StatusOK)
		case sarama.ErrUnknownTopicOrPartition:
			http.Error(w, err.Error(), http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	// Asynchronously submit the message to the Kafka cluster.
	as.producer.AsyncProduce(topic, toEncoderPreservingNil(key), sarama.StringEncoder(message))
	w.WriteHeader(http.StatusOK)
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (as *HTTPAPIServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[ParamTopic]
	r.ParseForm()
	groups := r.Form[ParamGroup]
	if len(groups) != 1 {
		errorText := fmt.Sprintf("One consumer group is expected, but %d provided", len(groups))
		http.Error(w, errorText, http.StatusBadRequest)
		return
	}

	consMsg, err := as.consumer.Consume(groups[0], topic)
	switch err := err.(type) {
	case nil:
		break
	case ErrConsumerRequestTimeout:
		http.Error(w, err.Error(), http.StatusRequestTimeout)
		return
	case ErrConsumerBufferOverflow:
		http.Error(w, err.Error(), 429) // StatusTooManyRequests
	default:
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	encodedMsg, err := json.Marshal(newMessageView(consMsg))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}

	w.Header().Add(HeaderContentType, "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(encodedMsg); err != nil {
		log.Errorf("Failed to send message")
	}
}

type messageView struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

func newMessageView(consMsg *sarama.ConsumerMessage) messageView {
	return messageView{
		Key:       consMsg.Key,
		Value:     consMsg.Value,
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
	}
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
