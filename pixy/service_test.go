package pixy

import (
	"fmt"
	"math"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

const (
	testBroker = "localhost:9092"
	testSocket = "kafka-pixy.sock"
)

type ServiceSuite struct {
	serviceCfg *ServiceCfg
	tkc        *TestKafkaClient
	unixClient *http.Client
	tcpClient  *http.Client
}

var _ = Suite(&ServiceSuite{})

func (s *ServiceSuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *ServiceSuite) SetUpTest(c *C) {
	s.serviceCfg = &ServiceCfg{
		UnixAddr:    path.Join(os.TempDir(), testSocket),
		BrokerAddrs: []string{testBroker},
	}
	os.Remove(s.serviceCfg.UnixAddr)

	s.tkc = NewTestKafkaClient(s.serviceCfg.BrokerAddrs)
	s.unixClient = NewUDSHTTPClient(s.serviceCfg.UnixAddr)
	s.tcpClient = &http.Client{}
}

func (s *ServiceSuite) TestStartAndStop(c *C) {
	svc, err := SpawnService(s.serviceCfg)
	c.Assert(err, IsNil)
	svc.Stop()
	svc.Wait4Stop()
}

func (s *ServiceSuite) TestInvalidUnixAddr(c *C) {
	// Given
	s.serviceCfg.UnixAddr = "/tmp"
	// When
	svc, err := SpawnService(s.serviceCfg)
	// Then
	c.Assert(err.Error(), Equals,
		"failed to start Unix socket based HTTP API, cause=(failed to create listener, cause=(listen unix /tmp: bind: address already in use))")
	c.Assert(svc, IsNil)
}

func (s *ServiceSuite) TestInvalidBrokers(c *C) {
	// Given
	s.serviceCfg.BrokerAddrs = []string{"localhost:12345"}
	// When
	svc, err := SpawnService(s.serviceCfg)
	// Then
	c.Assert(err.Error(), Equals,
		"failed to spawn Kafka client, cause=(failed to create Kafka client, cause=(kafka: Client has run out of available brokers to talk to. Is your cluster reachable?))")
	c.Assert(svc, IsNil)
}

// If `key` is not `nil` then produced messages are deterministically
// distributed between partitions based on the `key` hash.
func (s *ServiceSuite) TestProduce(c *C) {
	// Given
	svc, _ := SpawnService(s.serviceCfg)
	offsetsBefore := s.tkc.getOffsets("service-test")
	// When
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/service-test?key=1",
			"", strings.NewReader(strconv.Itoa(i)))
	}
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/service-test?key=2",
			"", strings.NewReader(strconv.Itoa(i)))
	}
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/service-test?key=3",
			"", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop()
	svc.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("service-test")
	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+200)
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+100)
}

// If `key` of a produced message is `nil` then it is submitted to a random
// partition. Therefore a batch of such messages is evenly distributed among
// all available partitions.
func (s *ServiceSuite) TestProduceNilKey(c *C) {
	// Given
	svc, _ := SpawnService(s.serviceCfg)
	offsetsBefore := s.tkc.getOffsets("service-test")
	// When
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/service-test",
			"", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop()
	svc.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("service-test")
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
	svc, _ := SpawnService(s.serviceCfg)
	offsetsBefore := s.tkc.getOffsets("service-test")
	// When
	for i := 0; i < 100; i++ {
		s.unixClient.Post("http://_/topics/service-test?key=",
			"", strings.NewReader(strconv.Itoa(i)))
	}
	svc.Stop()
	svc.Wait4Stop()
	offsetsAfter := s.tkc.getOffsets("service-test")
	// Then
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[1], Equals, offsetsBefore[1]+100)
}

// Utf8 messages are submitted without a problem.
func (s *ServiceSuite) TestUtf8Message(c *C) {
	svc, _ := SpawnService(s.serviceCfg)
	offsetsBefore := s.tkc.getOffsets("service-test")
	// When
	s.unixClient.Post("http://_/topics/service-test?key=foo",
		"", strings.NewReader("Превед Медвед"))
	svc.Stop()
	svc.Wait4Stop()
	// Then
	offsetsAfter := s.tkc.getOffsets("service-test")
	msgs := s.tkc.getMessages("service-test", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), []string{"Превед Медвед"}})
}

// TCP API is not started by default.
func (s *ServiceSuite) TestTCPDoesNotWork(c *C) {
	svc, _ := SpawnService(s.serviceCfg)
	// When
	r, err := s.tcpClient.Post("http://localhost:55501/topics/service-test?key=foo",
		"", strings.NewReader("Hello Kitty"))
	// Then
	svc.Stop()
	svc.Wait4Stop()
	c.Assert(err.Error(), Equals,
		"Post http://localhost:55501/topics/service-test?key=foo: dial tcp 127.0.0.1:55501: connection refused")
	c.Assert(r, IsNil)
}

// API is served on a TCP socket if it is explicitly configured.
func (s *ServiceSuite) TestBothAPI(c *C) {
	offsetsBefore := s.tkc.getOffsets("service-test")
	s.serviceCfg.TCPAddr = "127.0.0.1:55502"
	svc, _ := SpawnService(s.serviceCfg)
	// When
	_, err1 := s.tcpClient.Post("http://localhost:55502/topics/service-test?key=foo",
		"", strings.NewReader("Превед"))
	_, err2 := s.unixClient.Post("http://_/topics/service-test?key=foo",
		"", strings.NewReader("Kitty"))
	// Then
	svc.Stop()
	svc.Wait4Stop()
	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	offsetsAfter := s.tkc.getOffsets("service-test")
	msgs := s.tkc.getMessages("service-test", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), []string{"Превед", "Kitty"}})
}

func (s *ServiceSuite) TestStoppedServerCall(c *C) {
	svc, _ := SpawnService(s.serviceCfg)
	_, err := s.unixClient.Post("http://_/topics/service-test?key=foo",
		"", strings.NewReader("Hello"))
	c.Assert(err, IsNil)
	// When
	svc.Stop()
	svc.Wait4Stop()
	// Then
	r, err := s.unixClient.Post("http://_/topics/service-test?key=foo",
		"", strings.NewReader("Kitty"))
	c.Assert(err.Error(), Equals, "Post http://_/topics/service-test?key=foo: EOF")
	c.Assert(r, IsNil)
}

// If the TCP API Server crashes then the service terminates gracefully.
func (s *ServiceSuite) TestTCPServerCrash(c *C) {
	s.serviceCfg.TCPAddr = "127.0.0.1:55502"
	svc, _ := SpawnService(s.serviceCfg)
	// When
	svc.tcpServer.errorCh <- fmt.Errorf("Kaboom!")
	// Then
	svc.Stop()
	svc.Wait4Stop()
}

// Messages that have maximum possible size indeed go through. Note that we
// assume that the broker's limit is the same as the producer's one or higher.
func (s *ServiceSuite) TestLargestMessage(c *C) {
	offsetsBefore := s.tkc.getOffsets("service-test")
	maxMsgSize := sarama.NewConfig().Producer.MaxMessageBytes - max_size_adjustment
	msg := GenMessage(maxMsgSize)
	s.serviceCfg.TCPAddr = "127.0.0.1:55503"
	svc, _ := SpawnService(s.serviceCfg)
	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55503/topics/service-test?key=foo", msg)
	svc.Stop()
	svc.Wait4Stop()
	// Then
	AssertHTTPResp(c, r, http.StatusOK, "")
	offsetsAfter := s.tkc.getOffsets("service-test")
	messages := s.tkc.getMessages("service-test", offsetsBefore, offsetsAfter)
	readMsg := messages[1][0]
	c.Assert(readMsg, Equals, msg)
}

// Messages that are larger then producer's MaxMessageBytes size are silently
// dropped. Note that we assume that the broker's limit is the same as the
// producer's one or higher.
func (s *ServiceSuite) TestMessageTooLarge(c *C) {
	offsetsBefore := s.tkc.getOffsets("service-test")
	maxMsgSize := sarama.NewConfig().Producer.MaxMessageBytes - max_size_adjustment + 1
	msg := GenMessage(maxMsgSize)
	s.serviceCfg.TCPAddr = "127.0.0.1:55504"
	svc, _ := SpawnService(s.serviceCfg)
	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55504/topics/service-test?key=foo", msg)
	svc.Stop()
	svc.Wait4Stop()
	// Then
	AssertHTTPResp(c, r, http.StatusOK, "")
	offsetsAfter := s.tkc.getOffsets("service-test")
	c.Assert(offsetsAfter, DeepEquals, offsetsBefore)
}

// If the Unix API Server crashes then the service terminates gracefully.
func (s *ServiceSuite) TestUnixServerCrash(c *C) {
	svc, _ := SpawnService(s.serviceCfg)
	// When
	svc.unixServer.errorCh <- fmt.Errorf("Kaboom!")
	// Then
	svc.Stop()
	svc.Wait4Stop()
}
