package httpsrv

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"regexp"
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
	"github.com/pkg/errors"
)

const (
	networkTCP  = "tcp"
	networkUnix = "unix"

	// HTTP headers used by the API.
	hdrContentLength = "Content-Length"
	hdrContentType   = "Content-Type"
	hdrKafkaPrefix   = "X-Kafka-"

	// HTTP request parameters.
	prmCluster              = "cluster"
	prmTopic                = "topic"
	prmKey                  = "key"
	prmSync                 = "sync"
	prmGroup                = "group"
	prmNoAck                = "noAck"
	prmAckPartition         = "ackPartition"
	prmPartition            = "partition"
	prmAckOffset            = "ackOffset"
	prmOffset               = "offset"
	prmTopicsWithPartitions = "withPartitions"
	prmTopicsWithConfig     = "withConfig"
)

var (
	EmptyResponse          = map[string]interface{}{}
	jsonContentTypePattern *regexp.Regexp
)

type T struct {
	actDesc    *actor.Descriptor
	addr       string
	listener   net.Listener
	httpServer *http.Server
	proxySet   *proxy.Set
	wg         sync.WaitGroup
	errorCh    chan error

	// needed to pass in security info
	certPath string
	keyPath  string
}

func init() {
	var err error
	if jsonContentTypePattern, err = regexp.Compile("^application/(?:.*\\+)?json(?:;.*)?$"); err != nil {
		panic(err)
	}
}

// New creates an HTTP server instance that will accept API requests at the
// specified `network`/`address` and execute them with the specified `producer`,
// `consumer`, or `admin`, depending on the request type.
//
// It also passes in the provided certificate and key paths for TLS. If
// empty strings, it is run in non-TLS mode.
func New(addr string, proxySet *proxy.Set, certPath, keyPath string) (*T, error) {
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
	httpServer := &http.Server{Handler: router}

	hs := &T{
		actDesc:    actor.Root().NewChild(addr),
		addr:       addr,
		listener:   listener,
		httpServer: httpServer,
		proxySet:   proxySet,
		errorCh:    make(chan error, 1),
		certPath:   certPath,
		keyPath:    keyPath,
	}
	// Configure the API request handlers.
	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleProduce).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleProduce).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/messages", prmCluster, prmTopic), hs.handleConsume).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/messages", prmTopic), hs.handleConsume).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/acks", prmCluster, prmTopic), hs.handleAck).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/acks", prmTopic), hs.handleAck).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleGetOffsets).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleGetOffsets).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/offsets", prmCluster, prmTopic), hs.handleSetOffsets).Methods("POST")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/offsets", prmTopic), hs.handleSetOffsets).Methods("POST")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}/consumers", prmCluster, prmTopic), hs.handleGetTopicConsumers).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}/consumers", prmTopic), hs.handleGetTopicConsumers).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics", prmCluster), hs.handleListTopics).Methods("GET")
	router.HandleFunc("/topics", hs.handleListTopics).Methods("GET")

	router.HandleFunc(fmt.Sprintf("/clusters/{%s}/topics/{%s}", prmCluster, prmTopic), hs.handleGetTopicMetadata).Methods("GET")
	router.HandleFunc(fmt.Sprintf("/topics/{%s}", prmTopic), hs.handleGetTopicMetadata).Methods("GET")

	router.HandleFunc("/_ping", hs.handlePing).Methods("GET")
	return hs, nil
}

// Start starts triggers asynchronous HTTP server start. If it fails then the error
// will be sent down to `ErrorCh()`.
func (s *T) Start() {
	actor.Spawn(s.actDesc, &s.wg, func() {
		if s.certPath != "" && s.keyPath != "" {
			if err := s.httpServer.ServeTLS(s.listener, s.certPath, s.keyPath); err != nil {
				s.errorCh <- errors.Wrap(err, "HTTP API server failed")
			}
		} else {
			if err := s.httpServer.Serve(s.listener); err != nil {
				s.errorCh <- errors.Wrap(err, "HTTP API server failed")
			}
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
	s.httpServer.Shutdown(context.Background())
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
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	key := getParamBytes(r, prmKey)
	_, isSync := r.Form[prmSync]

	// Get the message body from the HTTP request.
	var msg sarama.Encoder
	if msg, err = s.readMsg(r); err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	// Look for headers with the "X-Kafka" prefix
	var headers []sarama.RecordHeader
	for header, values := range r.Header {
		if !strings.HasPrefix(header, hdrKafkaPrefix) {
			continue
		}

		headerBytes := []byte(header[len(hdrKafkaPrefix):])
		for _, v := range values {
			decoded, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				errorText := fmt.Sprintf("Invalid base64 encoding for header: %s", header)
				s.respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
				return
			}
			headers = append(headers, sarama.RecordHeader{
				Key:   headerBytes,
				Value: decoded,
			})
		}
	}

	// Asynchronously submit the message to the Kafka cluster.
	if !isSync {
		pxy.AsyncProduce(topic, toEncoderPreservingNil(key), msg, headers)
		s.respondWithJSON(w, http.StatusOK, EmptyResponse)
		return
	}

	prodMsg, err := pxy.Produce(topic, toEncoderPreservingNil(key), msg, headers)
	if err != nil {
		var status int
		switch err {
		case sarama.ErrUnknownTopicOrPartition:
			status = http.StatusNotFound
		case proxy.ErrDisabled:
			fallthrough
		case proxy.ErrUnavailable:
			status = http.StatusServiceUnavailable
		case proxy.ErrHeadersUnsupported:
			status = http.StatusBadRequest
		default:
			status = http.StatusInternalServerError
		}
		s.respondWithJSON(w, status, errorRs{err.Error()})
		return
	}

	s.respondWithJSON(w, http.StatusOK, produceRs{
		Partition: prodMsg.Partition,
		Offset:    prodMsg.Offset,
	})
}

// readMsg reads message from the HTTP request based on the Content-Type header.
func (s *T) readMsg(r *http.Request) (sarama.Encoder, error) {
	contentType := r.Header.Get(hdrContentType)
	if contentType == "text/plain" || jsonContentTypePattern.MatchString(contentType) {
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
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, true)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
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
		case consumer.ErrUnavailable:
			fallthrough
		case proxy.ErrDisabled:
			fallthrough
		case proxy.ErrUnavailable:
			status = http.StatusServiceUnavailable
		default:
			status = http.StatusInternalServerError
		}
		s.respondWithJSON(w, status, errorRs{err.Error()})
		return
	}

	headers := make([]consumeHeader, 0, len(consMsg.Headers))
	for _, h := range consMsg.Headers {
		headers = append(headers, consumeHeader{
			Key:   string(h.Key),
			Value: h.Value,
		})
	}

	s.respondWithJSON(w, http.StatusOK, consumeRs{
		Key:       consMsg.Key,
		Value:     consMsg.Value,
		Partition: consMsg.Partition,
		Offset:    consMsg.Offset,
		Headers:   headers,
	})
}

// handleConsume is an HTTP request handler for `GET /topic/{topic}/messages`
func (s *T) handleAck(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	ack, err := parseAck(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	err = pxy.Ack(group, topic, ack)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}
	s.respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetOffsets is an HTTP request handler for `GET /topic/{topic}/offsets`
func (s *T) handleGetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	partitionOffsets, err := pxy.GetGroupOffsets(group, topic)
	if err != nil {
		if errors.Cause(err) == sarama.ErrUnknownTopicOrPartition {
			s.respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
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
	s.respondWithJSON(w, http.StatusOK, offsetViews)
}

// handleGetOffsets is an HTTP request handler for `POST /topic/{topic}/offsets`
func (s *T) handleSetOffsets(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]
	group, err := getGroupParam(r, false)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		errorText := fmt.Sprintf("Failed to read the request: err=(%s)", err)
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
		return
	}

	var partitionOffsetViews []partitionInfo
	if err := json.Unmarshal(body, &partitionOffsetViews); err != nil {
		errorText := fmt.Sprintf("Failed to parse the request: err=(%s)", err)
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{errorText})
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
			s.respondWithJSON(w, http.StatusNotFound, errorRs{"Unknown topic"})
			return
		}
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	s.respondWithJSON(w, http.StatusOK, EmptyResponse)
}

// handleGetTopicConsumers is an HTTP request handler for `GET /topic/{topic}/consumers`
func (s *T) handleGetTopicConsumers(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	group, err := getGroupParam(r, true)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	var consumers map[string]map[string][]int32
	if group == "" {
		consumers, err = pxy.GetAllTopicConsumers(topic)
		if err != nil {
			s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
	} else {
		groupConsumers, err := pxy.GetTopicConsumers(group, topic)
		if err != nil {
			if _, ok := err.(admin.ErrInvalidParam); ok {
				s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
				return
			}
			s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
			return
		}
		consumers = make(map[string]map[string][]int32)
		if len(groupConsumers) != 0 {
			consumers[group] = groupConsumers
		}
	}

	encodedRes, err := json.MarshalIndent(consumers, "", "  ")
	if err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", http.StatusOK, encodedRes)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	encodedRes = prettyfmt.CollapseJSON(encodedRes)

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(encodedRes); err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", http.StatusOK, encodedRes)
	}
}

// handleListTopics is an HTTP request handler for `GET /topics`
func (s *T) handleListTopics(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	err = r.ParseForm()
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	_, withConfig := r.Form[prmTopicsWithConfig]
	_, withPartitions := r.Form[prmTopicsWithPartitions]

	topicsMetadata, err := pxy.ListTopics(withPartitions, withConfig)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	if withPartitions || withConfig {
		topicMetadataViews := make(map[string]*topicMetadata)
		for _, tm := range topicsMetadata {
			topicMetadataView := newTopicMetadataView(withPartitions, withConfig, tm)
			topicMetadataViews[tm.Topic] = &topicMetadataView
		}
		s.respondWithJSON(w, http.StatusOK, topicMetadataViews)
		return
	}

	topics := make([]string, 0, len(topicsMetadata))
	for _, tm := range topicsMetadata {
		topics = append(topics, tm.Topic)
	}
	s.respondWithJSON(w, http.StatusOK, topics)
}

// handleGetTopicMetadata is an HTTP request handler for `GET /topics/{topic}`
func (s *T) handleGetTopicMetadata(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var err error

	pxy, err := s.getProxy(r)
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}
	topic := mux.Vars(r)[prmTopic]

	err = r.ParseForm()
	if err != nil {
		s.respondWithJSON(w, http.StatusBadRequest, errorRs{err.Error()})
		return
	}

	withConfig := true
	_, withPartitions := r.Form[prmTopicsWithPartitions]

	tm, err := pxy.GetTopicMetadata(topic, withPartitions, withConfig)
	if err != nil {
		s.respondWithJSON(w, http.StatusInternalServerError, errorRs{err.Error()})
		return
	}

	tm_view := newTopicMetadataView(withPartitions, withConfig, tm)
	s.respondWithJSON(w, http.StatusOK, tm_view)
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

type consumeHeader struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type consumeRs struct {
	Key       []byte          `json:"key"`
	Value     []byte          `json:"value"`
	Partition int32           `json:"partition"`
	Offset    int64           `json:"offset"`
	Headers   []consumeHeader `json:"headers"`
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

type topicConfig struct {
	Version int32             `json:"version"`
	Config  map[string]string `json:"config"`
}

type partitionMetadata struct {
	ID       int32   `json:"partition"`
	Leader   int32   `json:"leader"`
	Replicas []int32 `json:"replicas"`
	ISR      []int32 `json:"isr"`
}

type topicMetadata struct {
	Config     *topicConfig        `json:"config,omitempty"`
	Partitions []partitionMetadata `json:"partitions,omitempty"`
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
func (s *T) respondWithJSON(w http.ResponseWriter, status int, body interface{}) {
	encodedRes, err := json.MarshalIndent(body, "", "  ")
	if err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", status, body)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Add(hdrContentType, "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(encodedRes); err != nil {
		s.actDesc.Log().WithError(err).Errorf("Failed to send HTTP response: status=%d, body=%v", status, body)
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

	r.ParseForm()
	_, noAck := r.Form[prmNoAck]
	if noAck {
		return proxy.NoAck(), nil
	}
	var err error
	var partition int64
	partitionStr, partitionOk := r.Form[partitionPrmName]
	if partitionOk {
		partition, err = strconv.ParseInt(partitionStr[0], 10, 32)
		if err != nil || partition < 0 {
			return proxy.NoAck(), errors.Wrapf(err, "bad %s: %s", partitionPrmName, partitionStr)
		}
	}
	var offset int64
	offsetStr, offsetOk := r.Form[offsetPrmName]
	if offsetOk {
		offset, err = strconv.ParseInt(offsetStr[0], 10, 64)
		if err != nil || offset < 0 {
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

func newTopicMetadataView(withPartitions, withConfig bool, tm admin.TopicMetadata) topicMetadata {
	topicMetadataView := topicMetadata{}
	if withPartitions {
		for _, p := range tm.Partitions {
			partitionView := partitionMetadata{
				ID:       p.ID,
				Leader:   p.Leader,
				Replicas: p.Replicas,
				ISR:      p.ISR,
			}
			topicMetadataView.Partitions = append(topicMetadataView.Partitions, partitionView)
		}
	}
	if withConfig {
		topicConfig := topicConfig{
			Version: tm.Config.Version,
			Config:  tm.Config.Config,
		}
		topicMetadataView.Config = &topicConfig
	}
	return topicMetadataView
}
