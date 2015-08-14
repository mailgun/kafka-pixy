package pixy

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

const (
	testSocket = "kafka-pixy.sock"
)

type ServiceSuite struct {
	config     *Config
	tkc        *TestKafkaClient
	unixClient *http.Client
	tcpClient  *http.Client
}

var _ = Suite(&ServiceSuite{})

func (s *ServiceSuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *ServiceSuite) SetUpTest(c *C) {
	s.config = NewConfig()
	s.config.UnixAddr = path.Join(os.TempDir(), testSocket)
	s.config.Kafka.SeedPeers = testKafkaPeers
	s.config.ZooKeeper.SeedPeers = testZookeeperPeers
	os.Remove(s.config.UnixAddr)

	s.tkc = NewTestKafkaClient(s.config.Kafka.SeedPeers)
	s.unixClient = NewUDSHTTPClient(s.config.UnixAddr)
	s.tcpClient = &http.Client{}
}

func (s *ServiceSuite) TestStartAndStop(c *C) {
	svc, err := SpawnService(s.config)
	c.Assert(err, IsNil)
	svc.Stop()
}

func (s *ServiceSuite) TestInvalidUnixAddr(c *C) {
	// Given
	s.config.UnixAddr = "/tmp"
	// When
	svc, err := SpawnService(s.config)
	// Then
	c.Assert(err.Error(), Equals,
		"failed to start Unix socket based HTTP API, cause=(failed to create listener, cause=(listen unix /tmp: bind: address already in use))")
	c.Assert(svc, IsNil)
}

func (s *ServiceSuite) TestInvalidKafkaPeers(c *C) {
	// Given
	s.config.Kafka.SeedPeers = []string{"localhost:12345"}
	// When
	svc, err := SpawnService(s.config)
	// Then
	c.Assert(err.Error(), Equals,
		"failed to spawn producer, cause=(failed to create sarama.Client, cause=(kafka: client has run out of available brokers to talk to (Is your cluster reachable?)))")
	c.Assert(svc, IsNil)
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *ServiceSuite) TestProduce(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	offsetsBefore := s.tkc.getOffsets("test.4")
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
	svc.Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+20)
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+10)
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2]+10)
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// If `key` of a produced message is `nil` then it is submitted to a random
// partition. Therefore a batch of such messages is evenly distributed among
// all available partitions.
func (s *ServiceSuite) TestProduceNilKey(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/test.4/messages",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	delta0 := offsetsAfter[0] - offsetsBefore[0]
	delta1 := offsetsAfter[1] - offsetsBefore[1]
	imbalance := int(math.Abs(float64(delta1 - delta0)))
	if imbalance > 20 {
		panic(fmt.Errorf("Too high imbalance: %v != %v", delta0, delta1))
	}
}

// If `key` of a produced message is empty then it is deterministically
// submitted to a particular partition determined by the empty key hash.
func (s *ServiceSuite) TestProduceEmptyKey(c *C) {
	svc, _ := SpawnService(s.config)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	for i := 0; i < 10; i++ {
		s.unixClient.Post("http://_/topics/test.4/messages?key=",
			"text/plain", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1])
	c.Assert(offsetsAfter[2], Equals, offsetsBefore[2])
	c.Assert(offsetsAfter[3], Equals, offsetsBefore[3]+10)
}

// Utf8 messages are submitted without a problem.
func (s *ServiceSuite) TestUtf8Message(c *C) {
	svc, _ := SpawnService(s.config)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Превед Медвед"))
	svc.Stop()
	// Then
	offsetsAfter := s.tkc.getOffsets("test.4")
	msgs := s.tkc.getMessages("test.4", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), []string{"Превед Медвед"}, []string(nil), []string(nil)})
}

// TCP API is not started by default.
func (s *ServiceSuite) TestTCPDoesNotWork(c *C) {
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.tcpClient.Post("http://localhost:55501/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Hello Kitty"))
	// Then
	svc.Stop()
	c.Assert(err.Error(), Matches,
		"Post http://localhost:55501/topics/test.4/messages\\?key=foo: .* connection refused")
	c.Assert(r, IsNil)
}

// API is served on a TCP socket if it is explicitly configured.
func (s *ServiceSuite) TestBothAPI(c *C) {
	offsetsBefore := s.tkc.getOffsets("test.4")
	s.config.TCPAddr = "127.0.0.1:55502"
	svc, err := SpawnService(s.config)
	c.Assert(err, IsNil)
	// When
	_, err1 := s.tcpClient.Post("http://localhost:55502/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Превед"))
	_, err2 := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Kitty"))
	// Then
	svc.Stop()
	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	offsetsAfter := s.tkc.getOffsets("test.4")
	msgs := s.tkc.getMessages("test.4", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), []string{"Превед", "Kitty"}, []string(nil), []string(nil)})
}

func (s *ServiceSuite) TestStoppedServerCall(c *C) {
	svc, _ := SpawnService(s.config)
	_, err := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Hello"))
	c.Assert(err, IsNil)
	// When
	svc.Stop()
	// Then
	r, err := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Kitty"))
	c.Assert(err.Error(), Equals, "Post http://_/topics/test.4/messages?key=foo: EOF")
	c.Assert(r, IsNil)
}

// If the TCP API Server crashes then the service terminates gracefully.
func (s *ServiceSuite) TestTCPServerCrash(c *C) {
	s.config.TCPAddr = "127.0.0.1:55502"
	svc, _ := SpawnService(s.config)
	// When
	svc.tcpServer.errorCh <- fmt.Errorf("Kaboom!")
	// Then
	svc.Stop()
}

// Messages that have maximum possible size indeed go through. Note that we
// assume that the broker's limit is the same as the producer's one or higher.
func (s *ServiceSuite) TestLargestMessage(c *C) {
	offsetsBefore := s.tkc.getOffsets("test.4")
	maxMsgSize := sarama.NewConfig().Producer.MaxMessageBytes - ProdMsgMetadataSize([]byte("foo"))
	msg := GenMessage(maxMsgSize)
	s.config.TCPAddr = "127.0.0.1:55503"
	svc, _ := SpawnService(s.config)
	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55503/topics/test.4/messages?key=foo", msg)
	svc.Stop()
	// Then
	AssertHTTPResp(c, r, http.StatusOK, "")
	offsetsAfter := s.tkc.getOffsets("test.4")
	messages := s.tkc.getMessages("test.4", offsetsBefore, offsetsAfter)
	readMsg := messages[1][0]
	c.Assert(readMsg, Equals, msg)
}

// Messages that are larger then producer's MaxMessageBytes size are silently
// dropped. Note that we assume that the broker's limit is the same as the
// producer's one or higher.
func (s *ServiceSuite) TestMessageTooLarge(c *C) {
	offsetsBefore := s.tkc.getOffsets("test.4")
	maxMsgSize := sarama.NewConfig().Producer.MaxMessageBytes - ProdMsgMetadataSize([]byte("foo")) + 1
	msg := GenMessage(maxMsgSize)
	s.config.TCPAddr = "127.0.0.1:55504"
	svc, _ := SpawnService(s.config)
	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55504/topics/test.4/messages?key=foo", msg)
	svc.Stop()
	// Then
	AssertHTTPResp(c, r, http.StatusOK, "")
	offsetsAfter := s.tkc.getOffsets("test.4")
	c.Assert(offsetsAfter, DeepEquals, offsetsBefore)
}

func (s *ServiceSuite) TestSyncProduce(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	offsetsBefore := s.tkc.getOffsets("test.4")
	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/messages?key=1&sync",
		"text/plain", strings.NewReader("Foo"))
	svc.Stop()
	offsetsAfter := s.tkc.getOffsets("test.4")
	// Then
	c.Assert(err, IsNil)
	AssertHTTPResp(c, r, http.StatusOK, "")
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+1)
}

func (s *ServiceSuite) TestSyncProduceInvalidTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.unixClient.Post("http://_/topics/no-such-topic?sync=true",
		"text/plain", strings.NewReader("Foo"))
	svc.Stop()
	// Then
	c.Assert(err, IsNil)
	AssertHTTPResp(c, r, http.StatusNotFound,
		fmt.Sprintf("%s\n", sarama.ErrUnknownTopicOrPartition.Error()))
}

// If the Unix API Server crashes then the service terminates gracefully.
func (s *ServiceSuite) TestUnixServerCrash(c *C) {
	svc, _ := SpawnService(s.config)
	// When
	svc.unixServer.errorCh <- fmt.Errorf("Kaboom!")
	// Then
	svc.Stop()
}

func (s *ServiceSuite) TestConsumeNoGroup(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages")
	svc.Stop()
	// Then
	c.Assert(err, IsNil)
	AssertHTTPResp(c, r, http.StatusBadRequest, "One consumer group is expected, but 0 provided\n")
}

func (s *ServiceSuite) TestConsumeManyGroups(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=a&group=b")
	svc.Stop()
	// Then
	c.Assert(err, IsNil)
	AssertHTTPResp(c, r, http.StatusBadRequest, "One consumer group is expected, but 2 provided\n")
}

func (s *ServiceSuite) TestConsumeInvalidTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.unixClient.Get("http://_/topics/no-such-topic/messages?group=foo")
	svc.Stop()
	// Then
	c.Assert(err, IsNil)
	AssertHTTPResp(c, r, http.StatusRequestTimeout,
		"</smartConsumer.*/foo.*/no-such-topic.*> timeout\n")
}

func (s *ServiceSuite) TestConsumeSingleMessage(c *C) {
	// Given
	ResetOffsets(c, "foo", "test.4")
	produced := GenMessages(c, "service.consume", "test.4", map[string]int{"B": 1})
	svc, _ := SpawnService(s.config)
	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo")
	svc.Stop()
	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r)
	c.Assert(ParseBase64(c, body["key"].(string)), Equals, "B")
	c.Assert(ParseBase64(c, body["value"].(string)), Equals, ProdMsgVal(produced["B"][0]))
	c.Assert(int(body["partition"].(float64)), Equals, 3)
	c.Assert(int64(body["offset"].(float64)), Equals, produced["B"][0].Offset)
}
