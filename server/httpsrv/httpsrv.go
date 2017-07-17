package httpsrv

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/gorilla/mux"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/admin"
	"github.com/mailgun/kafka-pixy/consumer"
	"github.com/mailgun/kafka-pixy/consumer/offsettrk"
	"github.com/mailgun/kafka-pixy/offsetmgr"
	"github.com/mailgun/kafka-pixy/prettyfmt"
	"github.com/mailgun/kafka-pixy/proxy"
	"github.com/mailgun/manners"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

const (
	networkTCP  = "tcp"
	networkUnix = "unix"

	// HTTP headers used by the API.
	hdrContentLength = "Content-Length"
	hdrContentType   = "Content-Type"

	// HTTP request parameters.
	prmCluster      = "cluster"
	prmTopic        = "topic"
	prmKey          = "key"
	prmSync         = "sync"
	prmGroup        = "group"
	prmNoAck        = "noAck"
	prmAckPartition = "ackPartition"
	prmPartition    = "partition"
	prmAckOffset    = "ackOffset"
	prmOffset       = "offset"
)

var (
	EmptyResponse = map[string]interface{}{}
)

type T struct {
	actorID    *actor.ID
	addr       string
	listener   net.Listener
	httpServer *manners.GracefulServer
	proxySet   *proxy.Set
	wg         sync.WaitGroup
	errorCh    chan error
}

// New creates an HTTP server instance that will accept API requests at the
// specified `network`/`address` and execute them with the specified `producer`,
// `consumer`, or `admin`, depending on the request type.
func New(addr string, proxySet *proxy.Set) (*T, error) {
	network := networkUnix
	if strings.Contains(addr, ":") {
		network = networkTCP
	}
	// Start listening on the specified network/address.
	listener, err := net.Listen(network, addr)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create listener")
	}
	// If the address is Unix Domain Socket then make it accessible for everyone.
	if network == networkUnix {
		if err := os.Chmod(addr, 0777); err != nil {
			return nil, errors.Wrap(err, "failed to change socket permissions")
		}
	}
	// Create a graceful HTTP server instance.
	router := mux.NewRouter()
	httpServer := manners.NewWithServer(&http.Server{Handler: router})
	hs := &T{
		actorID:    actor.RootID.NewChild(fmt.Sprintf("http://%s", addr)),
		addr:       addr,
		listener:   manners.NewListener(listener),
		httpServer: httpServer,
		proxySet:   proxySet,
		errorCh:    make(chan error, 1),
	}
	// Configure the API request handlers.
	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleProduce).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleProduce).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleConsume).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleConsume).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/acks", prmCluster, prmTopic), hs.handleConsume).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/acks", prmTopic), hs.handleConsume).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleGetOffsets).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleGetOffsets).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleSetOffsets).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleSetOffsets).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/consumers", prmCluster, prmTopic), hs.handleGetTopicConsumers).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/consumers", prmTopic), hs.handleGetTopicConsumers).Methods("GET")

	router.HandleFunc("/_ping", hs.handlePing).Methods("GET")
	return hs, nil
}

// Starts triggers asynchronous HTTP server start. If it fails then the error
// will be sent down to `ErrorCh()`.
func (s *T) Start() {
	actor.Spawn(s.actorID, &s.wg, func() {
		if err := s.httpServer.Serve(s.listener); err != nil {
			s.errorCh <- errors.Wrap(err, "HTTP API server failed")
		}
	})
}

// ErrorCh returns an output channel that HTTP server running in another
// goroutine will use if it stops with error if one occurs. The channel will be
// closed when the server is fully stopped due to an error or otherwise..
func (s *T) ErrorCh() <-chan error {
	return s.errorCh
}

// Stop gracefully stops the HTTP API server. It stops listening on the socket
// for incoming requests first, and then blocks waiting for pending requests to
// complete.
func (s *T) Stop() {
	s.httpServer.Close()
	s.wg.Wait()
	close(s.errorCh)
}

func (s *T) getProxy(r *http.Request) (*proxy.T, error) {
	cluster := mux.Vars(r)[prmCluster]
	return s.proxySet.Get(cluster)
}

// handleProduce is an HTTP request handler for `POST /topic/{topic}/messages`
func (s *T) handleProduce(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	key := getParamBytes(r, prmKey)
	_, isSync := r.Form[prmSync]

	// Get the message body from the HTTP request.
	var msg sarama.Encoder
	if msg, err = s.readMsg(r); err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	// Asynchronously submit the message to the Kafka cluster.
	if !isSync {
		pxy.AsyncProduce(topic, toEncoderPreservingNil(key), msg)
		respondWithJSON(w, http.StatusOK, EmptyResponse)
		return
	}

	prodMsg, err := pxy.Produce(topic, toEncoderPreservingNil(key), msg)
	if err != nil {
		var status int
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			status = http.StatusNotFound
		default:
			status = http.StatusInternalServerError
		}
		respondWithJSON(w, status, errorRs{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, produceRs{
		Partition: prodMsg.Partition,
		Offset:    prodMsg.Offset,
	})
}

// readMsg reads message from the HTTP request based on the Content-Type header.
func (s *T) readMsg(r *http.Request) (sarama.Encoder, error) {
	contentType := r.Header.Get(hdrContentType)
	if contentType == "text/plain" || contentType == "application/json" {
		if _, ok := r.Header[hdrContentLength]; !ok {
			return nil, errors.Errorf("missing %s header", hdrContentLength)
		}
		messageSizeStr := r.Header.Get(hdrContentLength)
		msgSize, err := strconv.Atoi(messageSizeStr)
		if err != nil {
			return nil, errors.Errorf("invalid %s header: %s", hdrContentLength, messageSizeStr)
		}
		msg, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read message")
		}
		if len(msg) != msgSize {
			return nil, errors.Errorf("message size does not match %s: expected=%v, actual=%v",
				hdrContentLength, msgSize, len(msg))
		}
		return sarama.ByteEncoder(msg), nil
	}
	if contentType == "application/x-www-form-urlencoded" {
		msg := r.FormValue("msg")
		if msg == "" {
			return nil, errors.Errorf("empty message")
		}
		return sarama.StringEncoder(msg), nil
	}
	return nil, errors.Errorf("unsupported content type %s", contentType)
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (s *T) handleConsume(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, true)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	consMsg, err := pxy.Consume(group, topic, ack)
	if err != nil {
		var status int
		switch err {
		case consumer.ErrRequestTimeout:
			status = http.StatusRequestTimeout
		case consumer.ErrTooManyRequests:
			status = http.StatusTooManyRequests
		default:
			status = http.StatusInternalServerError
		}
		respondWithJSON(w, status, errorRs{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, consumeRs{
		Key:       consMsg.Key,
		Value:     consMsg.Value,
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
	})
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (s *T) handleAck(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, true)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	err = pxy.Ack(group, topic, ack)
	if err != nil {
		respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}
	respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetOffsets is an HTTP request handler for `GET /topic/{topic}/offsets`
func (s *T) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	partitionOffsets, err := pxy.GetGroupOffsets(group, topic)
	if err != nil {
		if errors.Cause(err) == sarama.ErrUnknownTopicOrPartition {
			respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	offsetViews := make([]partitionInfo, len(partitionOffsets))
	for i, po := range partitionOffsets {
		offsetViews[i].Partition = po.Partition
		offsetViews[i].Begin = po.Begin
		offsetViews[i].End = po.End
		offsetViews[i].Count = po.End - po.Begin
		offsetViews[i].Offset = po.Offset
		if po.Offset == sarama.OffsetNewest {
			offsetViews[i].Lag = 0
		} else if po.Offset == sarama.OffsetOldest {
			offsetViews[i].Lag = po.End - po.Begin
		} else {
			offsetViews[i].Lag = po.End - po.Offset
		}
		offsetViews[i].Metadata = po.Metadata
		offset := offsetmgr.Offset{Val: po.Offset, Meta: po.Metadata}
		offsetViews[i].SparseAcks = offsettrk.SparseAcks2Str(offset)
	}
	respondWithJSON(w, http.StatusOK, offsetViews)
}

// handleGetOffsets is an HTTP request handler for `POST /topic/{topic}/offsets`
func (s *T) handleSetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read the request: err=(%s)", err)
		respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
		return
	}

	var partitionOffsetViews []partitionInfo
	if err := json.Unmarshal(body, &partitionOffsetViews); err != nil {
		errorText := fmt.Sprintf("Failed to parse the request: err=(%s)", err)
		respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
		return
	}

	partitionOffsets := make([]admin.PartitionOffset, len(partitionOffsetViews))
	for i, pov := range partitionOffsetViews {
		partitionOffsets[i].Partition = pov.Partition
		partitionOffsets[i].Offset = pov.Offset
		partitionOffsets[i].Metadata = pov.Metadata
	}

	err = pxy.SetGroupOffsets(group, topic, partitionOffsets)
	if err != nil {
		if err = errors.Cause(err); err == sarama.ErrUnknownTopicOrPartition {
			respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetTopicConsumers is an HTTP request handler for `GET /topic/{topic}/consumers`
func (s *T) handleGetTopicConsumers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	group, err := getGroupParam(r, true)
	if err != nil {
		respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	var consumers map[string]map[string][]int32
	if group == "" {
		consumers, err = pxy.GetAllTopicConsumers(topic)
		if err != nil {
			respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
	} else {
		groupConsumers, err := pxy.GetTopicConsumers(group, topic)
		if err != nil {
			if _, ok := err.(admin.ErrInvalidParam); ok {
				respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
				return
			}
			respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
		consumers = make(map[string]map[string][]int32)
		if len(groupConsumers) != 0 {
			consumers[group] = groupConsumers
		}
	}

	encodedRes, err := json.MarshalIndent(consumers, "", "  ")
	if err != nil {
		log.Errorf("Failed to send HTTP response: status=%d, body=%v, err=%+v", http.StatusOK, encodedRes, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	encodedRes = prettyfmt.CollapseJSON(encodedRes)

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(encodedRes); err != nil {
		log.Errorf("Failed to send HTTP response: status=%d, body=%v, err=%+v", http.StatusOK, encodedRes, err)
	}
}

func (s *T) handlePing(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("pong"))
}

type produceRs struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type consumeRs struct {
	Key       []byte `json:"key"`
	Value     []byte `json:"value"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

type partitionInfo struct {
	Partition  int32  `json:"partition"`
	Begin      int64  `json:"begin"`
	End        int64  `json:"end"`
	Count      int64  `json:"count"`
	Offset     int64  `json:"offset"`
	Lag        int64  `json:"lag"`
	Metadata   string `json:"metadata,omitempty"`
	SparseAcks string `json:"sparse_acks,omitempty"`
}

type errorRs struct {
	Error string `json:"error"`
}

// getParamBytes returns the request parameter s a slice of bytes. It works
// pretty much the same way s `http.FormValue`, except it distinguishes empty
// value (`[]byte{}`) from missing one (`nil`).
func getParamBytes(r *http.Request, name string) []byte {
	r.ParseForm() // Ignore errors, the go library does the same in FormValue.
	values, ok := r.Form[name]
	if !ok || len(values) == 0 {
		return nil
	}
	return []byte(values[0])
}

// respondWithJSON marshals `body` to a JSON string and sends it s an HTTP
// response body along with the specified `status` code.
func respondWithJSON(w http.ResponseWriter, status int, body interface{}) {
	encodedRes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		log.Errorf("Failed to send HTTP response: status=%d, body=%v, err=%+v", status, body, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(encodedRes); err != nil {
		log.Errorf("Failed to send HTTP response: status=%d, body=%v, err=%+v", status, body, err)
	}
}

func getGroupParam(r *http.Request, opt bool) (string, error) {
	r.ParseForm()
	groups := r.Form[prmGroup]
	if len(groups) > 1 || (!opt && len(groups) == 0) {
		return "", errors.Errorf("one consumer group is expected, but %d provided", len(groups))
	}
	if len(groups) == 0 {
		return "", nil
	}
	return groups[0], nil
}

// toEncoderPreservingNil converts a slice of bytes to `sarama.Encoder` but
// returns `nil` if the passed slice is `nil`.
func toEncoderPreservingNil(b []byte) sarama.Encoder {
	if b != nil {
		return sarama.StringEncoder(b)
	}
	return nil
}

func parseAck(r *http.Request, isConsReq bool) (proxy.Ack, error) {
	var partitionPrmName, offsetPrmName string
	if isConsReq {
		partitionPrmName = prmAckPartition
		offsetPrmName = prmAckOffset
	} else {
		partitionPrmName = prmPartition
		offsetPrmName = prmOffset
	}

	_, noAck := mux.Vars(r)[prmNoAck]
	if noAck {
		return proxy.NoAck(), nil
	}
	var err error
	var partition int64
	partitionStr, partitionOk := mux.Vars(r)[partitionPrmName]
	if partitionOk {
		partition, err = strconv.ParseInt(partitionStr, 10, 32)
		if err == nil || partition < 0 {
			return proxy.NoAck(), errors.Wrapf(err, "bad %s: %s", partitionPrmName, partitionStr)
		}
	}
	var offset int64
	offsetStr, offsetOk := mux.Vars(r)[offsetPrmName]
	if offsetOk {
		offset, err = strconv.ParseInt(offsetStr, 10, 64)
		if err == nil || offset < 0 {
			return proxy.NoAck(), errors.Wrapf(err, "bad %s: %s", offsetPrmName, offsetStr)
		}
	}
	if partitionOk && offsetOk {
		return proxy.NewAck(int32(partition), offset)
	}
	if !partitionOk && !offsetOk {
		return proxy.AutoAck(), nil
	}
	return proxy.NoAck(), errors.Errorf("%s and %s either both should be provided or neither", partitionPrmName, offsetPrmName)
}
