package service

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	. "gopkg.in/check.v1"

	"github.com/Shopify/sarama"
	"github.com/davecgh/go-spew/spew"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	pb "github.com/mailgun/kafka-pixy/gen/golang"
	"github.com/mailgun/kafka-pixy/server/httpsrv"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/kafka-pixy/testhelpers/kafkahelper"
	"github.com/pkg/errors"
)

type ServiceHTTPSuite struct {
	ns         *actor.Descriptor
	cfg        *config.App
	proxyCfg   *config.Proxy
	kh         *kafkahelper.T
	unixClient *http.Client
	tcpClient  *http.Client
}

var _ = Suite(&ServiceHTTPSuite{})

func (s *ServiceHTTPSuite) SetUpSuite(c *C) {
	testhelpers.InitLogging()
}

func (s *ServiceHTTPSuite) SetUpTest(c *C) {
	s.ns = actor.Root().NewChild("T")
	s.cfg = &config.App{Proxies: make(map[string]*config.Proxy)}
	s.cfg.TCPAddr = "127.0.0.1:19092"
	s.cfg.UnixAddr = path.Join(os.TempDir(), "kafka-pixy.sock")
	s.proxyCfg = testhelpers.NewTestProxyCfg("pxyH_client_id")
	s.proxyCfg.Consumer.OffsetsCommitInterval = 50 * time.Millisecond
	s.cfg.Proxies["pxyH"] = s.proxyCfg
	s.cfg.DefaultCluster = "pxyH"

	os.Remove(s.cfg.UnixAddr)
	s.kh = kafkahelper.New(c)
	s.unixClient = testhelpers.NewUDSHTTPClient(s.cfg.UnixAddr)
	// The default HTTP client cannot be used, because it caches connections,
	// but each test must have a brand new connection.
	s.tcpClient = &http.Client{Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}}
}

func (s *ServiceHTTPSuite) TearDownTest(c *C) {
	s.kh.Close()
}

func (s *ServiceHTTPSuite) TestInvalidUnixAddr(c *C) {
	// Server TCP socket will be left hanging in this case. In real life it is
	// kind of ok, because application would soon terminate anyway releasing
	// the socket. But in tests that may result in service.Spawn failure in
	// another test. To avoid that we chose a unique port number.
	s.cfg.TCPAddr = "127.0.0.1:19099"
	s.cfg.UnixAddr = "/tmp"

	// When
	svc, err := Spawn(s.cfg)

	// Then
	c.Check(err.Error(), Equals, "failed to start Unix socket based HTTP API server: "+
		"failed to create listener: "+
		"listen unix /tmp: "+
		"bind: address already in use")
	c.Check(svc, IsNil)
}

func (s *ServiceHTTPSuite) TestInvalidKafkaPeers(c *C) {
	s.cfg.Proxies[s.cfg.DefaultCluster].Kafka.SeedPeers = []string{"localhost:12345"}

	// When
	svc, err := Spawn(s.cfg)

	// Then
	c.Check(err.Error(), Equals, "failed to spawn proxy, name=pxyH: "+
		"failed to create Kafka client: "+
		"kafka: client has run out of available brokers to talk to (Is your cluster reachable?)")
	c.Check(svc, IsNil)
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *ServiceHTTPSuite) TestProduce(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 10; i++ {
		s.unixClient.Post("http://_/topics/test.4/messages?key=1",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
		s.unixClient.Post("http://_/topics/test.4/messages?key=2",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
		s.unixClient.Post("http://_/topics/test.4/messages?key=3",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
		s.unixClient.Post("http://_/topics/test.4/messages?key=4",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
		s.unixClient.Post("http://_/topics/test.4/messages?key=5",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0]+20)
	c.Check(offsetsAfter[1], Equals, offsetsBefore[1]+10)
	c.Check(offsetsAfter[2], Equals, offsetsBefore[2]+10)
	c.Check(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// If `key` of a produced message is `nil` then it is submitted to a random
// partition. Therefore a batch of such messages is evenly distributed among
// all available partitions.
func (s *ServiceHTTPSuite) TestProduceNilKey(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/test.4/messages",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	delta0 := offsetsAfter[0] - offsetsBefore[0]
	delta1 := offsetsAfter[1] - offsetsBefore[1]
	if delta0 == 0 || delta1 == 0 {
		panic(errors.Errorf("Too high imbalance: %v != %v", delta0, delta1))
	}
}

// If `key` of a produced message is empty then it is deterministically
// submitted to a particular partition determined by the empty key hash.
func (s *ServiceHTTPSuite) TestProduceEmptyKey(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	for i := 0; i < 10; i++ {
		s.unixClient.Post("http://_/topics/test.4/messages?key=",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Check(offsetsAfter[1], Equals, offsetsBefore[1])
	c.Check(offsetsAfter[2], Equals, offsetsBefore[2])
	c.Check(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// Utf8 messages are submitted without a problem.
func (s *ServiceHTTPSuite) TestUtf8Message(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Превед Медвед"))
	svc.Stop() // Have to stop before getOffsets

	// Then
	offsetsAfter := s.kh.GetNewestOffsets("test.4")
	msgs := s.kh.GetMessages("test.4", offsetsBefore, offsetsAfter)
	c.Check(msgs, DeepEquals,
		[][]string{[]string(nil), {"Превед Медвед"}, []string(nil), []string(nil)})
}

// Headers are submitted without a problem
func (s *ServiceHTTPSuite) TestProduceHeaders(c *C) {
	if !s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers not supported before Kafka v0.11")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.1")

	// When
	req, err := http.NewRequest("POST", "http://_/topics/test.1/messages?key=foo&sync",
		strings.NewReader("test"))
	c.Assert(err, IsNil)
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("X-Kafka-Foo", base64.StdEncoding.EncodeToString([]byte("bar")))
	rs, err := s.unixClient.Do(req)

	// Then
	c.Check(err, IsNil)
	c.Check(rs.StatusCode, Equals, http.StatusOK)

	svc.Stop() // Stop before getting offsets
	offsetsAfter := s.kh.GetNewestOffsets("test.1")
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0]+1)
}

// Invalid base64-encoded headers should produce an error
func (s *ServiceHTTPSuite) TestProduceInvalidHeaders(c *C) {
	if !s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers not supported before Kafka v0.11")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	req, err := http.NewRequest("POST", "http://_/topics/test.1/messages?key=foo&sync",
		strings.NewReader("test"))
	c.Assert(err, IsNil)
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("X-Kafka-Foo", "invalid!")
	rs, err := s.unixClient.Do(req)

	// Then
	c.Check(err, IsNil)
	c.Check(rs.StatusCode, Equals, http.StatusBadRequest)

	body := ParseJSONBody(c, rs).(map[string]interface{})
	c.Check(body["error"], Equals, "Invalid base64 encoding for header: X-Kafka-Foo")
}

// Submitting headers when they're not supported by the Kafka version setting
// should produce an error
func (s *ServiceHTTPSuite) TestProduceHeadersUnsupported(c *C) {
	if s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers are supported after Kafka v0.11")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	req, err := http.NewRequest("POST", "http://_/topics/test.1/messages?key=foo&sync",
		strings.NewReader("test"))
	c.Assert(err, IsNil)
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("X-Kafka-Foo", base64.StdEncoding.EncodeToString([]byte("bar")))
	rs, err := s.unixClient.Do(req)

	// Then
	c.Check(err, IsNil)
	c.Check(rs.StatusCode, Equals, http.StatusBadRequest)

	body := ParseJSONBody(c, rs).(map[string]interface{})
	c.Check(body["error"], Matches, "headers are not supported with this version of Kafka.*")

}

func (s *ServiceHTTPSuite) TestProduceXWWWFormUrlencoded(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.1")

	// When
	rs, err := s.unixClient.Post("http://_/topics/test.1/messages?key=1",
		"application/x-www-form-urlencoded", strings.NewReader("msg=foo"))

	// Then
	c.Check(err, IsNil)
	c.Check(rs.StatusCode, Equals, http.StatusOK)

	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.kh.GetNewestOffsets("test.1")
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0]+1)
}

// API is served on a TCP socket if it is explicitly configured.
func (s *ServiceHTTPSuite) TestBothAPI(c *C) {
	offsetsBefore := s.kh.GetNewestOffsets("test.4")
	s.cfg.TCPAddr = "127.0.0.1:55501"
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	// When
	_, err1 := s.tcpClient.Post("http://localhost:55501/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Превед"))
	_, err2 := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Kitty"))

	// Then
	svc.Stop() // Have to stop before getOffsets
	c.Check(err1, IsNil)
	c.Check(err2, IsNil)
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	fmt.Printf("*** Before=%v, After=%v\n", offsetsBefore, offsetsAfter)

	msgs := s.kh.GetMessages("test.4", offsetsBefore, offsetsAfter)
	c.Check(msgs, DeepEquals,
		[][]string{[]string(nil), {"Превед", "Kitty"}, []string(nil), []string(nil)})
}

func (s *ServiceHTTPSuite) TestStoppedServerCall(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	_, err = s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Hello"))
	c.Check(err, IsNil)

	// When
	svc.Stop()

	// Then
	r, err := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Kitty"))
	c.Check(true, Equals, strings.Contains(err.Error(), "no such file or directory"))
	c.Check(r, IsNil)
}

// Messages that have maximum possible size indeed go through. Note that we
// assume that the broker's limit is the same as the producer's one or higher.
func (s *ServiceHTTPSuite) TestLargestMessage(c *C) {
	offsetsBefore := s.kh.GetNewestOffsets("test.4")
	saramaCfg := s.proxyCfg.SaramaProducerCfg()
	maxMsgSize := saramaCfg.Producer.MaxMessageBytes

	// Since v0.11.0.0 the broker configuration max.message.bytes now applies
	// to the total size of a batch of messages. Previously the setting applied
	// to batches of compressed messages, or to non-compressed messages
	// individually. A message batch may consist of only a single message, so
	// in most cases, the limitation on the size of individual messages is only
	// reduced by the overhead of the batch format.
	if saramaCfg.Version.IsAtLeast(sarama.V0_11_0_0) {
		maxMsgSize -= 39
	} else {
		maxMsgSize -= ProdMsgMetadataSize([]byte("foo"))
	}

	msg := GenMessage(maxMsgSize)
	s.cfg.TCPAddr = "127.0.0.1:55501"
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55501/topics/test.4/messages?key=foo", msg)
	svc.Stop() // Have to stop before getOffsets

	// Then
	c.Check(r.StatusCode, Equals, http.StatusOK)
	c.Check(ParseJSONBody(c, r), DeepEquals, map[string]interface{}{})
	offsetsAfter := s.kh.GetNewestOffsets("test.4")
	messages := s.kh.GetMessages("test.4", offsetsBefore, offsetsAfter)
	readMsg := messages[1][0]
	c.Check(readMsg, Equals, msg)
}

// Messages that are larger then producer's MaxMessageBytes size are silently
// dropped. Note that we assume that the broker's limit is the same as the
// producer's one or higher.
func (s *ServiceHTTPSuite) TestMessageTooLarge(c *C) {
	offsetsBefore := s.kh.GetNewestOffsets("test.4")
	maxMsgSize := sarama.NewConfig().Producer.MaxMessageBytes - ProdMsgMetadataSize([]byte("foo")) + 1
	msg := GenMessage(maxMsgSize)
	s.cfg.TCPAddr = "127.0.0.1:55501"
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55501/topics/test.4/messages?key=foo", msg)
	svc.Stop() // Have to stop before getOffsets

	// Then
	c.Check(r.StatusCode, Equals, http.StatusOK)
	c.Check(ParseJSONBody(c, r), DeepEquals, map[string]interface{}{})
	offsetsAfter := s.kh.GetNewestOffsets("test.4")
	c.Check(offsetsAfter, DeepEquals, offsetsBefore)
}

func (s *ServiceHTTPSuite) TestSyncProduce(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	offsetsBefore := s.kh.GetNewestOffsets("test.4")

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/messages?key=1&sync",
		"text/plain", strings.NewReader("Foo"))
	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.kh.GetNewestOffsets("test.4")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(int(body["partition"].(float64)), Equals, 0)
	c.Check(int64(body["offset"].(float64)), Equals, offsetsBefore[0])
	c.Check(offsetsAfter[0], Equals, offsetsBefore[0]+1)
}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
//func (s *ServiceHTTPSuite) TestSyncProduceInvalidTopic(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	r, err := s.unixClient.Post("http://_/topics/no-such-topic/messages?sync=true",
//		"text/plain", strings.NewReader("Foo"))
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(r.StatusCode, Equals, http.StatusNotFound)
//	body := ParseJSONBody(c, r).(map[string]interface{})
//	c.Check(body["error"], Equals, sarama.ErrUnknownTopicOrPartition.Error())
//}

func (s *ServiceHTTPSuite) TestConsumeNoGroup(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "one consumer group is expected, but 0 provided")
}

func (s *ServiceHTTPSuite) TestConsumeManyGroups(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=a&group=b")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "one consumer group is expected, but 2 provided")
}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
//func (s *ServiceHTTPSuite) TestConsumeInvalidTopic(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	r, err := s.unixClient.Get("http://_/topics/no-such-topic/messages?group=foo")
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(r.StatusCode, Equals, http.StatusRequestTimeout)
//	body := ParseJSONBody(c, r).(map[string]interface{})
//	c.Check(body["error"], Equals, "long polling timeout")
//}

// By default auto-ack mode is assumed when consuming.
func (s *ServiceHTTPSuite) TestConsumeAutoAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("auto-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")
	spew.Dump(offsetsBefore)

	// When
	for i := 0; i < 88; i++ {
		res, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo")
		c.Check(err, IsNil, Commentf("failed to consume message #%d", i))
		consRes := ParseConsRes(c, res)
		key := string(consRes.KeyValue)
		consumed[key] = append(consumed[key], consRes)
	}
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	spew.Dump(offsetsAfter)
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Check(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Check(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Check(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

// If message is consumed with noAck but is not explicitly acknowledged, then
// its offset is not committed.
func (s *ServiceHTTPSuite) TestConsumeNoAck(c *C) {
	s.proxyCfg.Consumer.AckTimeout = 500 * time.Millisecond
	s.proxyCfg.Consumer.SubscriptionTimeout = 500 * time.Millisecond
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.1")
	s.kh.PutMessages("no-ack", "test.1", map[string]int{"A": 1})
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.1")

	// When
	_, err = s.unixClient.Get("http://_/topics/test.1/messages?group=foo&noAck")
	c.Check(err, IsNil)
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.1")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val)
}

func (s *ServiceHTTPSuite) TestConsumeExplicitAck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	produced := s.kh.PutMessages("explicit-ack", "test.4", map[string]int{"A": 17, "B": 19, "C": 23, "D": 29})
	consumed := make(map[string][]*pb.ConsRs)
	offsetsBefore := s.kh.GetCommittedOffsets("foo", "test.4")

	// When:

	// First message has to be consumed with noAck.
	res, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo&noAck")
	c.Check(err, IsNil, Commentf("failed to consume first message"))
	consRes := ParseConsRes(c, res)
	key := string(consRes.KeyValue)
	consumed[key] = append(consumed[key], consRes)
	// Whenever a message is consumed previous one is acked.
	for i := 1; i < 88; i++ {
		url := fmt.Sprintf("http://_/topics/test.4/messages?group=foo&ackPartition=%d&ackOffset=%d",
			consRes.Partition, consRes.Offset)
		httpRs, err := s.unixClient.Get(url)
		consRes = ParseConsRes(c, httpRs)
		c.Check(err, IsNil, Commentf("failed to consume message #%d", i))
		c.Check(httpRs.StatusCode, Equals, http.StatusOK)
		key := string(consRes.KeyValue)
		consumed[key] = append(consumed[key], consRes)
	}
	// Ack last message.
	url := fmt.Sprintf("http://_/topics/test.4/acks?group=foo&partition=%d&offset=%d",
		consRes.Partition, consRes.Offset)
	httpRs, err := s.unixClient.Post(url, "text/plain", nil)
	c.Check(err, IsNil, Commentf("failed ack last message"))
	ackRs := ParseJSONBody(c, httpRs).(map[string]interface{})
	c.Check(ackRs, DeepEquals, map[string]interface{}{})
	c.Check(httpRs.StatusCode, Equals, http.StatusOK)
	svc.Stop()

	// Then
	offsetsAfter := s.kh.GetCommittedOffsets("foo", "test.4")
	c.Check(offsetsAfter[0].Val, Equals, offsetsBefore[0].Val+17)
	c.Check(offsetsAfter[1].Val, Equals, offsetsBefore[1].Val+29)
	c.Check(offsetsAfter[2].Val, Equals, offsetsBefore[2].Val+23)
	c.Check(offsetsAfter[3].Val, Equals, offsetsBefore[3].Val+19)

	assertMsgs(c, consumed, produced)
}

func (s *ServiceHTTPSuite) TestConsumeDisabled(c *C) {
	s.proxyCfg.Consumer.Disabled = true
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusServiceUnavailable)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "service is disabled by configuration")
}

// When a message that was produced with headers is conusmed, the headers should
// be present
func (s *ServiceHTTPSuite) TestConsumeHeaders(c *C) {
	if !s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V0_11_0_0) {
		c.Skip("Headers not supported before Kafka v0.11")
	}

	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)

	s.kh.ResetOffsets("foo", "test.4")
	req, err := http.NewRequest("POST", "http://_/topics/test.4/messages?key=foo&sync",
		strings.NewReader("test"))
	c.Assert(err, IsNil)
	req.Header.Add("Content-Type", "text/plain")
	req.Header.Add("X-Kafka-Foo", base64.StdEncoding.EncodeToString([]byte("bar")))
	rs, err := s.unixClient.Do(req)
	c.Assert(err, IsNil)
	c.Assert(rs.StatusCode, Equals, http.StatusOK)

	// When
	res, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo")
	c.Check(err, IsNil)
	consRes := ParseConsRes(c, res)
	svc.Stop()

	// Then
	c.Check(consRes.Headers, Not(HasLen), 0)
	c.Check(consRes.Headers[0], DeepEquals, &pb.RecordHeader{Key: "Foo", Value: []byte("bar")})
}

// If offsets for a group that does not exist are requested then -1 is returned
// as the next offset to be consumed for all topic partitions.
func (s *ServiceHTTPSuite) TestGetOffsetsNoSuchGroup(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/offsets?group=no_such_group")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).([]interface{})
	for i := 0; i < 4; i++ {
		partitionView := body[i].(map[string]interface{})
		c.Check(partitionView["partition"].(float64), Equals, float64(i))
		c.Check(partitionView["offset"].(float64), Equals, float64(-1))
	}
}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
// An attempt to retrieve offsets for a topic that does not exist fails with 404.
//func (s *ServiceHTTPSuite) TestGetOffsetsNoSuchTopic(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	r, err := s.unixClient.Get("http://_/topics/no_such_topic/offsets?group=foo")
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(r.StatusCode, Equals, http.StatusNotFound)
//	body := ParseJSONBody(c, r).(map[string]interface{})
//	c.Check(body["error"], Equals, "Unknown topic")
//}

// Committed offsets are returned in a following GET request.
func (s *ServiceHTTPSuite) TestSetOffsets(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(
			`[{"partition": 0, "offset": 1100, "metadata": "A100"},
			  {"partition": 1, "offset": 1101, "metadata": "A101"},
			  {"partition": 2, "offset": 1102, "metadata": "A102"},
			  {"partition": 3, "offset": 1103, "metadata": "A103"}]`))

	// Then
	c.Check(err, IsNil)
	c.Check(ParseJSONBody(c, r), DeepEquals, httpsrv.EmptyResponse)
	c.Check(r.StatusCode, Equals, http.StatusOK)

	r, err = s.unixClient.Get("http://_/topics/test.4/offsets?group=foo")
	c.Check(err, IsNil)
	body := ParseJSONBody(c, r).([]interface{})
	for i := 0; i < 4; i++ {
		partitionView := body[i].(map[string]interface{})
		c.Check(partitionView["partition"].(float64), Equals, float64(i))
		c.Check(partitionView["offset"].(float64), Equals, float64(1100+i))
		c.Check(partitionView["metadata"].(string), Equals, fmt.Sprintf("A10%d", i))
		c.Check(partitionView["count"], Equals, partitionView["end"].(float64)-partitionView["begin"].(float64))
	}
}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
// Result of setting offsets for a non-existent topic depends on the Kafka
// version. It is ok for 0.8, but error for 0.9.x and higher.
//func (s *ServiceHTTPSuite) TestSetOffsetsNoSuchTopic(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	r, err := s.unixClient.Post("http://_/topics/no_such_topic/offsets?group=foo",
//		"application/json", strings.NewReader(`[{"partition": 0, "offset": 1100, "metadata": "A100"}]`))
//
//	// Then
//	kafkaVersion := os.Getenv("KAFKA_VERSION")
//	if strings.HasPrefix(kafkaVersion, "0.8") {
//		c.Check(err, IsNil)
//		c.Check(r.StatusCode, Equals, http.StatusOK)
//		c.Check(ParseJSONBody(c, r), DeepEquals, httpsrv.EmptyResponse)
//		return
//	}
//
//	c.Check(err, IsNil)
//	c.Check(r.StatusCode, Equals, http.StatusNotFound)
//	body := ParseJSONBody(c, r).(map[string]interface{})
//	c.Check(body["error"], Equals, "Unknown topic")
//}

// Invalid body is detected and properly reported.
func (s *ServiceHTTPSuite) TestSetOffsetsInvalidBody(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(`garbage`))

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "Failed to parse the request: err=(invalid character 'g' looking for beginning of value)")
}

// It is not an error to set an offset for a missing partition.
func (s *ServiceHTTPSuite) TestSetOffsetsInvalidPartition(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(`[{"partition": 5}]`))

	// Then
	c.Check(err, IsNil)
	body := ParseJSONBody(c, r).(map[string]interface{})
	if s.proxyCfg.Kafka.Version.IsAtLeast(sarama.V2_0_0_0) {
		c.Check(r.StatusCode, Equals, http.StatusNotFound)
		c.Check(body["error"], Equals, "Unknown topic")
	} else {
		c.Check(r.StatusCode, Equals, http.StatusOK)
		c.Check(body, DeepEquals, httpsrv.EmptyResponse)
	}
}

// Reported partition lags are correct, including those corresponding to -1 and
// -2 special case offset values.
func (s *ServiceHTTPSuite) TestGetOffsetsLag(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(
			`[{"partition": 0, "offset": -1},
			  {"partition": 1, "offset": -2},
			  {"partition": 2, "offset": 1}]`))
	c.Check(err, IsNil)

	// When
	r, err = s.unixClient.Get("http://_/topics/test.4/offsets?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).([]interface{})
	partition0View := body[0].(map[string]interface{})
	c.Check(partition0View["lag"], Equals, float64(0))
	partition1View := body[1].(map[string]interface{})
	c.Check(partition1View["lag"], Equals, partition1View["end"].(float64)-partition1View["begin"].(float64))
	partition2View := body[2].(map[string]interface{})
	c.Check(partition2View["lag"], Equals, partition2View["end"].(float64)-partition2View["offset"].(float64))
}

func (s *ServiceHTTPSuite) TestGetConsumersTopicNotConsumed(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// Issue a consume request to initialize the consumer group.
	_, err = s.unixClient.Get("http://_/topics/test.1/messages?group=foo")
	c.Assert(err, IsNil)

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/consumers?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "topic test.4 is not consumed by foo")
}

func (s *ServiceHTTPSuite) TestGetConsumersUnknownGroup(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/consumers?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(body["error"], Equals, "unknown consumer group foo")
}

func (s *ServiceHTTPSuite) TestGetTopicConsumersOne(c *C) {
	s.kh.ResetOffsets("foo", "test.4")
	s.kh.PutMessages("get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1, "D": 1})
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()
	for i := 0; i < 4; i++ {
		s.unixClient.Get("http://_/topics/test.4/messages?group=foo")
	}

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/consumers?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	consumers := ParseJSONBody(c, r).(map[string]interface{})
	assertConsumedPartitions(c, consumers, map[string]map[string][]int32{
		"foo": {
			"pxyH_client_id": {0, 1, 2, 3}},
	})
}

// If `group` parameter is not passed to `GET /topics/{}/consumers` then
// a topic consumer report includes members of all consumer groups consuming
// the topic.
func (s *ServiceHTTPSuite) TestGetAllTopicConsumers(c *C) {
	var wg sync.WaitGroup
	s.kh.ResetOffsets("foo", "test.4")
	s.kh.ResetOffsets("bar", "test.1")
	s.kh.ResetOffsets("bazz", "test.4")
	s.kh.PutMessages("get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1})
	s.kh.PutMessages("get.consumers", "test.1", map[string]int{"D": 1})

	svc := make([]*T, 3)
	for i := range svc {
		i := i
		actor.Spawn(s.ns.NewChild("spawn_svc"), &wg, func() {
			svc[i] = spawnHTTPSvc(c, 55501+i)
		})
	}
	wg.Wait()
	defer svc[0].Stop()
	defer svc[1].Stop()
	defer svc[2].Stop()

	for _, r := range []struct {
		port  int
		group string
		topic string
	}{
		{port: 55501, group: "foo", topic: "test.4"},
		{port: 55502, group: "foo", topic: "test.4"},
		{port: 55503, group: "foo", topic: "test.4"},
		{port: 55501, group: "bar", topic: "test.1"},
		{port: 55502, group: "bar", topic: "test.1"},
		{port: 55502, group: "bazz", topic: "test.4"},
		{port: 55502, group: "bazz", topic: "test.4"},
		{port: 55502, group: "bazz", topic: "test.4"},
	} {
		r := r
		actor.Spawn(s.ns.NewChild("get"), &wg, func() {
			_, err := s.tcpClient.Get(fmt.Sprintf(
				"http://127.0.0.1:%d/topics/%s/messages?group=%s", r.port, r.topic, r.group))
			c.Check(err, IsNil)
		})
	}
	wg.Wait()

	// When
	r, err := s.tcpClient.Get("http://127.0.0.1:55501/topics/test.4/consumers")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)

	consumers := ParseJSONBody(c, r).(map[string]interface{})
	assertConsumedPartitions(c, consumers, map[string]map[string][]int32{
		"foo": {
			"C55501": {0, 1},
			"C55502": {2},
			"C55503": {3}},
		"bazz": {
			"C55502": {0, 1, 2, 3}},
	})
}

// If consumers are requested for a particular group then only members of that
// group are returned.
func (s *ServiceHTTPSuite) TestGetTopicConsumers(c *C) {
	s.kh.ResetOffsets("foo", "test.4")
	s.kh.ResetOffsets("bar", "test.4")
	s.kh.PutMessages("get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1})

	svc1 := spawnHTTPSvc(c, 55501)
	defer svc1.Stop()
	svc2 := spawnHTTPSvc(c, 55502)
	defer svc2.Stop()

	_, err := s.tcpClient.Get("http://127.0.0.1:55501/topics/test.4/messages?group=foo")
	c.Check(err, IsNil)
	_, err = s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/messages?group=foo")
	c.Check(err, IsNil)

	_, err = s.tcpClient.Get("http://127.0.0.1:55501/topics/test.1/messages?group=bar")
	c.Check(err, IsNil)

	// When
	r, err := s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/consumers?group=foo")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	consumers := ParseJSONBody(c, r).(map[string]interface{})
	assertConsumedPartitions(c, consumers, map[string]map[string][]int32{
		"foo": {
			"C55501": {0, 1},
			"C55502": {2, 3}},
	})
}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
//func (s *ServiceHTTPSuite) TestGetTopics(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	r, err := s.unixClient.Get("http://_/topics")
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(r.StatusCode, Equals, http.StatusOK)
//	var topics []string
//	ParseResponseBody(c, r, &topics)
//	c.Check(topics, DeepEquals, []string{"__consumer_offsets", "test.1", "test.4", "test.64"})
//}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
//func (s *ServiceHTTPSuite) TestGetTopicsWithPartitions(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	rs, err := s.unixClient.Get("http://_/topics?withPartitions=true")
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(rs.StatusCode, Equals, http.StatusOK)
//
//	var topicsWithPartition map[string]struct {
//		Config     *struct{} `json:"config"`
//		Partitions []struct {
//			Partition int   `json:"partition"`
//			Leader    int   `json:"leader"`
//			Replicas  []int `json:"replicas"`
//			ISR       []int `json:"isr"`
//		} `json:"partitions"`
//	}
//	ParseResponseBody(c, rs, &topicsWithPartition)
//
//	var topics []string
//	for topic := range topicsWithPartition {
//		topics = append(topics, topic)
//	}
//	sort.Strings(topics)
//	c.Check(topics, DeepEquals, []string{"__consumer_offsets", "test.1", "test.4", "test.64"})
//
//	for topic, topicMeta := range topicsWithPartition {
//		c.Check(topicMeta.Config, IsNil)
//		expectedPartitionCount := map[string]int{
//			"__consumer_offsets": 50,
//			"test.1":             1,
//			"test.4":             4,
//			"test.64":            64,
//		}[topic]
//		c.Check(len(topicMeta.Partitions), Equals, expectedPartitionCount)
//
//		// Get replication factor used, when bootstrapping the cluster.
//		defaultReplicationFactorStr := os.Getenv("REPLICATION_FACTOR")
//		defaultReplicationFactor, _ := strconv.Atoi(defaultReplicationFactorStr)
//		if defaultReplicationFactor == 0 {
//			defaultReplicationFactor = 2
//		}
//
//		for _, partitionMeta := range topicMeta.Partitions {
//			replicationFactor := map[string]int{
//				"__consumer_offsets": 3,
//				"test.1":             defaultReplicationFactor,
//				"test.4":             defaultReplicationFactor,
//				"test.64":            defaultReplicationFactor,
//			}[topic]
//			c.Logf("Checking: topic=%v, partition=%v", topic, partitionMeta)
//			c.Check(len(partitionMeta.Replicas), Equals, replicationFactor)
//			c.Check(len(partitionMeta.ISR), Equals, replicationFactor)
//		}
//	}
//}

// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
//func (s *ServiceHTTPSuite) TestGetTopicsWithConfig(c *C) {
//	svc, err := Spawn(s.cfg)
//	c.Assert(err, IsNil)
//	defer svc.Stop()
//
//	// When
//	rs, err := s.unixClient.Get("http://_/topics?withConfig")
//
//	// Then
//	c.Check(err, IsNil)
//	c.Check(rs.StatusCode, Equals, http.StatusOK)
//
//	var topicsWithConfig map[string]struct {
//		Config struct {
//			Version int               `json:"version"`
//			Config  map[string]string `json:"config"`
//		} `json:"config"`
//		Partitions []struct{} `json:"partitions"`
//	}
//	ParseResponseBody(c, rs, &topicsWithConfig)
//
//	var topics []string
//	for topic := range topicsWithConfig {
//		topics = append(topics, topic)
//	}
//	sort.Strings(topics)
//	c.Check(topics, DeepEquals, []string{"__consumer_offsets", "test.1", "test.4", "test.64"})
//
//	for topic, topicMeta := range topicsWithConfig {
//		c.Check(topicMeta.Partitions, IsNil)
//		c.Check(topicMeta.Config.Version, Equals, 1)
//		c.Check(topicMeta.Config.Config, DeepEquals,
//			map[string]map[string]string{
//				"__consumer_offsets": {
//					"cleanup.policy":   "compact",
//					"compression.type": "producer",
//					"segment.bytes":    "104857600"},
//				"test.1":  {},
//				"test.4":  {},
//				"test.64": {},
//			}[topic])
//	}
//}

func (s *ServiceHTTPSuite) TestGetTopicsWithPartitionsAndWithConfig(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	rs, err := s.unixClient.Get("http://_/topics?withPartitions&withConfig")

	// Then
	c.Check(err, IsNil)
	c.Check(rs.StatusCode, Equals, http.StatusOK)

	var topicsWithConfig map[string]struct {
		Config struct {
			Version int               `json:"version"`
			Config  map[string]string `json:"config"`
		} `json:"config"`
		Partitions []struct{} `json:"partitions"`
	}
	ParseResponseBody(c, rs, &topicsWithConfig)

	var topics []string
	for topic := range topicsWithConfig {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	// TODO(thrawn01): The current test suite uses a kafka container that auto creates topics
	c.Check(topics, DeepEquals, []string{"__consumer_offsets", "missing", "no-such-topic", "no_such_topic", "test.1", "test.4", "test.64"})

	for _, topicMeta := range topicsWithConfig {
		c.Check(topicMeta.Partitions, NotNil)
		c.Check(topicMeta.Config.Version, NotNil)
	}
}

// Reported partition lags are correct, including those corresponding to -1 and
// -2 special case offset values.
func (s *ServiceHTTPSuite) TestHealthCheck(c *C) {
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/_ping")

	// Then
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body, err := ioutil.ReadAll(r.Body)
	c.Check(err, IsNil)
	c.Check(string(body), Equals, "pong")
}

// Ensure that API endpoints that explicitly select a proxy to operate on work.
func (s *ServiceHTTPSuite) TestExplicitProxyAPIEndpoints(c *C) {
	s.kh.ResetOffsets("foo", "test.1")
	svc, err := Spawn(s.cfg)
	c.Assert(err, IsNil)
	defer svc.Stop()

	// When/Then
	r, err := s.unixClient.Post("http://_/clusters/pxyH/topics/test.1/messages?sync", "text/plain", strings.NewReader("Bazinga!"))
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Check(int(body["partition"].(float64)), Equals, 0)
	prodOffset := int64(body["offset"].(float64))

	r, err = s.unixClient.Get("http://_/clusters/pxyH/topics/test.1/messages?group=foo")
	c.Check(err, IsNil)
	c.Check(r.StatusCode, Equals, http.StatusOK)
	body = ParseJSONBody(c, r).(map[string]interface{})
	c.Check(ParseBase64(c, body["value"].(string)), Equals, "Bazinga!")
	c.Check(int64(body["offset"].(float64)), Equals, prodOffset)
}

func spawnHTTPSvc(c *C, port int) *T {
	cfg := &config.App{Proxies: make(map[string]*config.Proxy)}
	cfg.UnixAddr = path.Join(os.TempDir(), fmt.Sprintf("kafka-pixy.%d.sock", port))
	cluster := fmt.Sprintf("pxy%d", port)
	cfg.Proxies[cluster] = testhelpers.NewTestProxyCfg(fmt.Sprintf("C%d", port))
	cfg.DefaultCluster = cluster
	os.Remove(cfg.UnixAddr)
	cfg.TCPAddr = fmt.Sprintf("127.0.0.1:%d", port)
	svc, err := Spawn(cfg)
	c.Assert(err, IsNil)
	return svc
}

func assertConsumedPartitions(c *C, consumers map[string]interface{}, expected map[string]map[string][]int32) {
	c.Check(len(consumers), Equals, len(expected))
	for group, expectedClients := range expected {
		groupConsumers := consumers[group].(map[string]interface{})
		c.Check(len(groupConsumers), Equals, len(expectedClients))
		for clientID, expectedPartitions := range expectedClients {
			clientPartitions := groupConsumers[clientID].([]interface{})
			partitions := make([]int32, len(clientPartitions))
			for i, p := range clientPartitions {
				partitions[i] = int32(p.(float64))
			}
			c.Check(partitions, DeepEquals, expectedPartitions)
		}
	}
}

// GenMessage generates an ASCII message of the specified size.
func GenMessage(size int) string {
	b := bytes.NewBuffer(nil)
	for b.Len() < size {
		b.WriteString(strconv.Itoa(b.Len()))
		b.WriteString("-")
	}
	return string(b.Bytes()[:size])
}

// ChunkReader allows reading its underlying buffer in chunks making the
// specified pauses between the chunks. After each pause `Read()` returns
// `0, nil`. This kind of reader is useful to simulate HTTP requests that
// require several read operations on the request body to get all of it.
type ChunkReader struct {
	buf         []byte
	begin       int
	chunkSize   int
	pause       time.Duration
	shouldPause bool
}

func NewChunkReader(s string, count int, pause time.Duration) *ChunkReader {
	return &ChunkReader{
		buf:       []byte(s),
		chunkSize: len(s) / count,
		pause:     pause,
	}
}

func (cr *ChunkReader) Read(b []byte) (int, error) {
	if cr.begin == len(cr.buf) {
		return 0, io.EOF
	}
	if cr.shouldPause = !cr.shouldPause; cr.shouldPause {
		time.Sleep(cr.pause)
		return 0, nil
	}
	chunkSize := cr.chunkSize
	if len(b) < chunkSize {
		chunkSize = len(b)
	}
	if len(cr.buf)-cr.begin < chunkSize {
		chunkSize = len(cr.buf) - cr.begin
	}
	end := cr.begin + chunkSize
	copied := copy(b, cr.buf[cr.begin:end])
	cr.begin = end
	return copied, nil
}

func PostChunked(clt *http.Client, url, msg string) *http.Response {
	req, err := http.NewRequest("POST", url, NewChunkReader(msg, 1, 10*time.Millisecond))
	if err != nil {
		panic(fmt.Sprintf("Failed to make a request object: err=(%s)", err))
	}
	req.Header.Add("Content-Type", "text/plain")
	req.ContentLength = int64(len(msg))
	resp, err := clt.Do(req)
	if err != nil {
		panic(fmt.Sprintf("Failed to do a request: err=(%s)", err))
	}
	return resp
}

func ProdMsgMetadataSize(key []byte) int {
	size := 26 // the metadata overhead of CRC, flags, etc.
	if key != nil {
		size += sarama.ByteEncoder(key).Length()
	}
	return size
}

func ParseJSONBody(c *C, res *http.Response) interface{} {
	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		c.Error(err)
		return nil
	}
	res.Body.Close()
	var parsedBody interface{}
	if err := json.Unmarshal(body, &parsedBody); err != nil {
		c.Error(err)
		return nil
	}
	return parsedBody
}

func ParseConsRes(c *C, res *http.Response) *pb.ConsRs {
	body := ParseJSONBody(c, res).(map[string]interface{})
	var headers []*pb.RecordHeader
	for _, raw := range body["headers"].([]interface{}) {
		h := raw.(map[string]interface{})
		value, err := base64.StdEncoding.DecodeString(h["value"].(string))
		if err != nil {
			c.Error(err)
			return nil
		}
		headers = append(headers, &pb.RecordHeader{
			Key:   h["key"].(string),
			Value: value,
		})
	}

	return &pb.ConsRs{
		KeyValue:  []byte(ParseBase64(c, body["key"].(string))),
		Message:   []byte(ParseBase64(c, body["value"].(string))),
		Partition: int32(body["partition"].(float64)),
		Offset:    int64(body["offset"].(float64)),
		Headers:   headers,
	}
}

func ParseBase64(c *C, encoded string) string {
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
	decoded, err := ioutil.ReadAll(decoder)
	if err != nil {
		c.Error(err)
		return ""
	}
	return string(decoded)
}

func ParseResponseBody(c *C, rs *http.Response, body interface{}) {
	defer rs.Body.Close()
	err := json.NewDecoder(rs.Body).Decode(&body)
	c.Assert(err, IsNil)
}
