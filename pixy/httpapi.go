package pixy

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
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

var (
	EmptyResponse = map[string]interface{}{}
)

type HTTPAPIServer struct {
	addr       string
	listener   net.Listener
	httpServer *manners.GracefulServer
	producer   *GracefulProducer
	consumer   *SmartConsumer
	admin      *Admin
	errorCh    chan error
}

// NewHTTPAPIServer creates an HTTP server instance that will accept API
// requests at the specified `network`/`address` and execute them with the
// specified `producer`, `consumer`, or `admin`, depending on the request type.
func NewHTTPAPIServer(network, addr string, producer *GracefulProducer, consumer *SmartConsumer, admin *Admin) (*HTTPAPIServer, error) {
	// Start listening on the specified network/address.
	listener, err := net.Listen(network, addr)
	if err != nil {
		return nil, fmt.Errorf("failed to create listener, err=(%s)", err)
	}
	// If the address is Unix Domain Socket then make it accessible for everyone.
	if network == NetworkUnix {
		if err := os.Chmod(addr, 0777); err != nil {
			return nil, fmt.Errorf("failed to change socket permissions, err=(%s)", err)
		}
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
		admin:      admin,
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
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", ParamTopic),
		as.handleGetOffsets).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", ParamTopic),
		as.handleSetOffsets).Methods("POST")
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
			as.errorCh <- fmt.Errorf("HTTP API listener failed, err=(%s)", err)
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
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}
	messageSizeStr := r.Header.Get(HeaderContentLength)
	messageSize, err := strconv.Atoi(messageSizeStr)
	if err != nil {
		errorText := fmt.Sprintf("Invalid %s header: %s", HeaderContentLength, messageSizeStr)
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}
	message, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read a message: err=(%s)", err)
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}
	if len(message) != messageSize {
		errorText := fmt.Sprintf("Message size does not match %s: expected=%v, actual=%v",
			HeaderContentLength, messageSize, len(message))
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}

	// Asynchronously submit the message to the Kafka cluster.
	if !isSync {
		as.producer.AsyncProduce(topic, toEncoderPreservingNil(key), sarama.StringEncoder(message))
		respondWithJSON(w, http.StatusOK, EmptyResponse)
		return
	}

	prodMsg, err := as.producer.Produce(topic, toEncoderPreservingNil(key), sarama.StringEncoder(message))
	if err != nil {
		var status int
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}
		respondWithJSON(w, status, errorHTTPResponse{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, produceHTTPResponse{
		Partition: prodMsg.Partition,
		Offset:    prodMsg.Offset,
	})
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (as *HTTPAPIServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[ParamTopic]
	group, err := getGroupParam(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{err.Error()})
		return
	}

	consMsg, err := as.consumer.Consume(group, topic)
	if err != nil {
		var status int
		switch err.(type) {
		case ErrConsumerRequestTimeout:
			status = http.StatusRequestTimeout
		case ErrConsumerBufferOverflow:
			status = 429 // StatusTooManyRequests
		default:
			status = http.StatusInternalServerError
		}
		respondWithJSON(w, status, errorHTTPResponse{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, consumeHTTPResponse{
		Key:       consMsg.Key,
		Value:     consMsg.Value,
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
	})
}

// handleGetOffsets is an HTTP request handler for `GET /topic/{topic}/offsets`
func (as *HTTPAPIServer) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[ParamTopic]
	group, err := getGroupParam(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{err.Error()})
		return
	}

	partitionOffsets, err := as.admin.GetGroupOffsets(group, topic)
	if err != nil {
		if err, ok := err.(ErrAdminKafkaReq); ok && err.Cause() == sarama.ErrUnknownTopicOrPartition {
			respondWithJSON(w, http.StatusNotFound, errorHTTPResponse{"Unknown topic"})
			return
		}
		respondWithJSON(w, http.StatusInternalServerError, errorHTTPResponse{err.Error()})
		return
	}

	partitionOffsetView := make([]partitionOffsetView, len(partitionOffsets))
	for i, po := range partitionOffsets {
		partitionOffsetView[i].Partition = po.Partition
		partitionOffsetView[i].Begin = po.Begin
		partitionOffsetView[i].End = po.End
		partitionOffsetView[i].Count = po.End - po.Begin
		partitionOffsetView[i].Offset = po.Offset
		if po.Offset == sarama.OffsetNewest {
			partitionOffsetView[i].Lag = 0
		} else if po.Offset == sarama.OffsetOldest {
			partitionOffsetView[i].Lag = po.End - po.Begin
		} else {
			partitionOffsetView[i].Lag = po.End - po.Offset
		}
		partitionOffsetView[i].Metadata = po.Metadata
	}
	respondWithJSON(w, http.StatusOK, partitionOffsetView)
}

// handleGetOffsets is an HTTP request handler for `POST /topic/{topic}/offsets`
func (as *HTTPAPIServer) handleSetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	topic := mux.Vars(r)[ParamTopic]
	group, err := getGroupParam(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{err.Error()})
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read the request: err=(%s)", err)
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}

	var partitionOffsetViews []partitionOffsetView
	if err := json.Unmarshal(body, &partitionOffsetViews); err != nil {
		errorText := fmt.Sprintf("Failed to parse the request: err=(%s)", err)
		respondWithJSON(w, http.StatusBadRequest, errorHTTPResponse{errorText})
		return
	}

	partitionOffsets := make([]PartitionOffset, len(partitionOffsetViews))
	for i, pov := range partitionOffsetViews {
		partitionOffsets[i].Partition = pov.Partition
		partitionOffsets[i].Offset = pov.Offset
		partitionOffsets[i].Metadata = pov.Metadata
	}

	err = as.admin.SetGroupOffsets(group, topic, partitionOffsets)
	if err != nil {
		if err, ok := err.(ErrAdminKafkaReq); ok && err.Cause() == sarama.ErrUnknownTopicOrPartition {
			respondWithJSON(w, http.StatusNotFound, errorHTTPResponse{"Unknown topic"})
			return
		}
		respondWithJSON(w, http.StatusInternalServerError, errorHTTPResponse{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, EmptyResponse)
}

type produceHTTPResponse struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type consumeHTTPResponse struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type partitionOffsetView struct {
	Partition int32  `json:"partition"`
	Begin     int64  `json:"begin"`
	End       int64  `json:"end"`
	Count     int64  `json:"count"`
	Offset    int64  `json:"offset"`
	Lag       int64  `json:"lag"`
	Metadata  string `json:"metadata,omitempty"`
}

type errorHTTPResponse struct {
	Error string `json:"error"`
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

// respondWithJSON marshals `body` to a JSON string and sends it as an HTTP
// response body along with the specified `status` code.
func respondWithJSON(w http.ResponseWriter, status int, body interface{}) {
	encodedRes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		log.Errorf("Failed to send HTTP reponse: status=%d, body=%v, reason=%v", status, body, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add(HeaderContentType, "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(encodedRes); err != nil {
		log.Errorf("Failed to send HTTP reponse: status=%d, body=%v, reason=%v", status, body, err)
	}
}

func getGroupParam(r *http.Request) (string, error) {
	r.ParseForm()
	groups := r.Form[ParamGroup]
	if len(groups) != 1 {
		return "", fmt.Errorf("One consumer group is expected, but %d provided", len(groups))
	}
	return groups[0], nil
}
