package pixy

import (
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
	"github.com/mailgun/kafka-pixy/config"
)

type ServiceSuite struct {
	config     *config.T
	tkc        *TestKafkaClient
	unixClient *http.Client
	tcpClient  *http.Client
}

var _ = Suite(&ServiceSuite{})

func (s *ServiceSuite) SetUpSuite(c *C) {
	InitTestLog()
}

func (s *ServiceSuite) SetUpTest(c *C) {
	s.config = NewTestConfig("service-default")
	os.Remove(s.config.UnixAddr)
	s.tkc = NewTestKafkaClient(s.config.Kafka.SeedPeers)
	s.unixClient = NewUDSHTTPClient(s.config.UnixAddr)
	// The default HTTP client cannot be used, because it caches connections,
	// but each test must have a brand new connection.
	s.tcpClient = &http.Client{Transport: &http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	}}
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
		"failed to start Unix socket based HTTP API, err=(failed to create listener, err=(listen unix /tmp: bind: address already in use))")
	c.Assert(svc, IsNil)
}

func (s *ServiceSuite) TestInvalidKafkaPeers(c *C) {
	// Given
	s.config.Kafka.SeedPeers = []string{"localhost:12345"}

	// When
	svc, err := SpawnService(s.config)

	// Then
	c.Assert(err.Error(), Equals,
		"failed to spawn producer, err=(failed to create sarama.Client, err=(kafka: client has run out of available brokers to talk to (Is your cluster reachable?)))")
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
	svc.Stop() // Have to stop before getOffsets
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
	svc.Stop() // Have to stop before getOffsets
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
	svc.Stop() // Have to stop before getOffsets
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
	svc.Stop() // Have to stop before getOffsets

	// Then
	offsetsAfter := s.tkc.getOffsets("test.4")
	msgs := s.tkc.getMessages("test.4", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), {"Превед Медвед"}, []string(nil), []string(nil)})
}

// TCP API is not started by default.
func (s *ServiceSuite) TestTCPDoesNotWork(c *C) {
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.tcpClient.Post("http://localhost:55501/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Hello Kitty"))

	// Then
	c.Assert(err.Error(), Matches,
		"Post http://localhost:55501/topics/test.4/messages\\?key=foo: .* connection refused")
	c.Assert(r, IsNil)
}

// API is served on a TCP socket if it is explicitly configured.
func (s *ServiceSuite) TestBothAPI(c *C) {
	offsetsBefore := s.tkc.getOffsets("test.4")
	s.config.TCPAddr = "127.0.0.1:55501"
	svc, err := SpawnService(s.config)
	c.Assert(err, IsNil)

	// When
	_, err1 := s.tcpClient.Post("http://localhost:55501/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Превед"))
	_, err2 := s.unixClient.Post("http://_/topics/test.4/messages?key=foo",
		"text/plain", strings.NewReader("Kitty"))

	// Then
	svc.Stop() // Have to stop before getOffsets
	c.Assert(err1, IsNil)
	c.Assert(err2, IsNil)
	offsetsAfter := s.tkc.getOffsets("test.4")
	msgs := s.tkc.getMessages("test.4", offsetsBefore, offsetsAfter)
	c.Assert(msgs, DeepEquals,
		[][]string{[]string(nil), {"Превед", "Kitty"}, []string(nil), []string(nil)})
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
	c.Assert(err.Error(), Matches, "Post http://_/topics/test\\.4/messages\\?key=foo: dial unix .* no such file or directory")
	c.Assert(r, IsNil)
}

// If the TCP API Server crashes then the service terminates gracefully.
func (s *ServiceSuite) TestTCPServerCrash(c *C) {
	s.config.TCPAddr = "127.0.0.1:55501"
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
	s.config.TCPAddr = "127.0.0.1:55501"
	svc, _ := SpawnService(s.config)

	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55501/topics/test.4/messages?key=foo", msg)
	svc.Stop() // Have to stop before getOffsets

	// Then
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(ParseJSONBody(c, r), DeepEquals, map[string]interface{}{})
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
	s.config.TCPAddr = "127.0.0.1:55501"
	svc, _ := SpawnService(s.config)

	// When
	r := PostChunked(s.tcpClient, "http://127.0.0.1:55501/topics/test.4/messages?key=foo", msg)
	svc.Stop() // Have to stop before getOffsets

	// Then
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(ParseJSONBody(c, r), DeepEquals, map[string]interface{}{})
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
	svc.Stop() // Have to stop before getOffsets
	offsetsAfter := s.tkc.getOffsets("test.4")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(int(body["partition"].(float64)), Equals, 0)
	c.Assert(int64(body["offset"].(float64)), Equals, offsetsBefore[0])
	c.Assert(offsetsAfter[0], Equals, offsetsBefore[0]+1)
}

func (s *ServiceSuite) TestSyncProduceInvalidTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/no-such-topic/messages?sync=true",
		"text/plain", strings.NewReader("Foo"))

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusNotFound)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, sarama.ErrUnknownTopicOrPartition.Error())
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
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "One consumer group is expected, but 0 provided")
}

func (s *ServiceSuite) TestConsumeManyGroups(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=a&group=b")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "One consumer group is expected, but 2 provided")
}

func (s *ServiceSuite) TestConsumeInvalidTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/no-such-topic/messages?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusRequestTimeout)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "long polling timeout")
}

func (s *ServiceSuite) TestConsumeSingleMessage(c *C) {
	// Given
	ResetOffsets(c, "foo", "test.4")
	produced := GenMessages(c, "service.consume", "test.4", map[string]int{"B": 1})
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/messages?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(ParseBase64(c, body["key"].(string)), Equals, "B")
	c.Assert(ParseBase64(c, body["value"].(string)), Equals, ProdMsgVal(produced["B"][0]))
	c.Assert(int(body["partition"].(float64)), Equals, 3)
	c.Assert(int64(body["offset"].(float64)), Equals, produced["B"][0].Offset)
}

// If offsets for a group that does not exist are requested then -1 is returned
// as the next offset to be consumed for all topic partitions.
func (s *ServiceSuite) TestGetOffsetsNoSuchGroup(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/offsets?group=no_such_group")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).([]interface{})
	for i := 0; i < 4; i++ {
		partitionView := body[i].(map[string]interface{})
		c.Assert(partitionView["partition"].(float64), Equals, float64(i))
		c.Assert(partitionView["offset"].(float64), Equals, float64(-1))
	}
}

// An attempt to retrieve offsets for a topic that does not exist fails with 404.
func (s *ServiceSuite) TestGetOffsetsNoSuchTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/no_such_topic/offsets?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusNotFound)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "Unknown topic")
}

// Committed offsets are returned in a following GET request.
func (s *ServiceSuite) TestSetOffsets(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(
			`[{"partition": 0, "offset": 1100, "metadata": "A100"},
			  {"partition": 1, "offset": 1101, "metadata": "A101"},
			  {"partition": 2, "offset": 1102, "metadata": "A102"},
			  {"partition": 3, "offset": 1103, "metadata": "A103"}]`))

	// Then
	c.Assert(err, IsNil)
	c.Assert(ParseJSONBody(c, r), DeepEquals, EmptyResponse)
	c.Assert(r.StatusCode, Equals, http.StatusOK)

	r, err = s.unixClient.Get("http://_/topics/test.4/offsets?group=foo")
	c.Assert(err, IsNil)
	body := ParseJSONBody(c, r).([]interface{})
	for i := 0; i < 4; i++ {
		partitionView := body[i].(map[string]interface{})
		c.Assert(partitionView["partition"].(float64), Equals, float64(i))
		c.Assert(partitionView["offset"].(float64), Equals, float64(1100+i))
		c.Assert(partitionView["metadata"].(string), Equals, fmt.Sprintf("A10%d", i))
		c.Assert(partitionView["count"], Equals, partitionView["end"].(float64)-partitionView["begin"].(float64))
	}
}

// It is not an error to set offsets for a topic that does not exist.
func (s *ServiceSuite) TestSetOffsetsNoSuchTopic(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/no_such_topic/offsets?group=foo",
		"application/json", strings.NewReader(`[{"partition": 0, "offset": 1100, "metadata": "A100"}]`))

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(ParseJSONBody(c, r), DeepEquals, EmptyResponse)
}

// Invalid body is detected and properly reported.
func (s *ServiceSuite) TestSetOffsetsInvalidBody(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(`garbage`))

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "Failed to parse the request: err=(invalid character 'g' looking for beginning of value)")
}

// It is not an error to set an offset for a missing partition.
func (s *ServiceSuite) TestSetOffsetsInvalidPartition(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(`[{"partition": 5}]`))

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	c.Assert(ParseJSONBody(c, r), DeepEquals, EmptyResponse)
}

// Reported partition lags are correct, including those corresponding to -1 and
// -2 special case offset values.
func (s *ServiceSuite) TestGetOffsetsLag(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()
	r, err := s.unixClient.Post("http://_/topics/test.4/offsets?group=foo",
		"application/json", strings.NewReader(
			`[{"partition": 0, "offset": -1},
			  {"partition": 1, "offset": -2},
			  {"partition": 2, "offset": 1}]`))
	c.Assert(err, IsNil)

	// When
	r, err = s.unixClient.Get("http://_/topics/test.4/offsets?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	body := ParseJSONBody(c, r).([]interface{})
	partition0View := body[0].(map[string]interface{})
	c.Assert(partition0View["lag"], Equals, float64(0))
	partition1View := body[1].(map[string]interface{})
	c.Assert(partition1View["lag"], Equals, partition1View["end"].(float64)-partition1View["begin"].(float64))
	partition2View := body[2].(map[string]interface{})
	c.Assert(partition2View["lag"], Equals, partition2View["end"].(float64)-partition2View["offset"].(float64))
}

// If a topic is not consumed by any member of a group at the moment then
// empty consumer map is returned.
func (s *ServiceSuite) TestGetTopicConsumersNone(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/consumers?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	consumers := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(len(consumers), Equals, 0)
}

func (s *ServiceSuite) TestGetTopicConsumersInvalid(c *C) {
	// Given
	svc, _ := SpawnService(s.config)
	defer svc.Stop()

	// When
	r, err := s.unixClient.Get("http://_/topics/test.5/consumers?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusBadRequest)
	body := ParseJSONBody(c, r).(map[string]interface{})
	c.Assert(body["error"], Equals, "either group or topic is incorrect")
}

func (s *ServiceSuite) TestGetTopicConsumersOne(c *C) {
	// Given
	ResetOffsets(c, "foo", "test.4")
	GenMessages(c, "get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1, "D": 1})
	svc, _ := SpawnService(NewTestConfig("C1"))
	defer svc.Stop()
	for i := 0; i < 4; i++ {
		s.unixClient.Get("http://_/topics/test.4/messages?group=foo")
	}

	// When
	r, err := s.unixClient.Get("http://_/topics/test.4/consumers?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	consumers := ParseJSONBody(c, r).(map[string]interface{})
	assertConsumedPartitions(c, consumers, map[string]map[string][]int32{
		"foo": {
			"C1": {0, 1, 2, 3}},
	})
}

// If `group` parameter is not passed to `GET /topics/{}/consumers` then
// a topic consumer report includes members of all consumer groups consuming
// the topic.
func (s *ServiceSuite) TestGetAllTopicConsumers(c *C) {
	// Given
	ResetOffsets(c, "foo", "test.4")
	ResetOffsets(c, "bar", "test.1")
	ResetOffsets(c, "bazz", "test.4")
	GenMessages(c, "get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1})
	GenMessages(c, "get.consumers", "test.1", map[string]int{"D": 1})

	svc1 := spawnTestService(c, 55501)
	defer svc1.Stop()
	svc2 := spawnTestService(c, 55502)
	defer svc2.Stop()
	svc3 := spawnTestService(c, 55503)
	defer svc3.Stop()

	_, err := s.tcpClient.Get("http://127.0.0.1:55501/topics/test.4/messages?group=foo")
	c.Assert(err, IsNil)
	_, err = s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/messages?group=foo")
	c.Assert(err, IsNil)
	_, err = s.tcpClient.Get("http://127.0.0.1:55503/topics/test.4/messages?group=foo")
	c.Assert(err, IsNil)

	_, err = s.tcpClient.Get("http://127.0.0.1:55501/topics/test.1/messages?group=bar")
	c.Assert(err, IsNil)

	_, err = s.tcpClient.Get("http://127.0.0.1:55502/topics/test.1/messages?group=bar")
	c.Assert(err, IsNil)
	for i := 0; i < 3; i++ {
		_, err = s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/messages?group=bazz")
		c.Assert(err, IsNil)
	}

	// When
	r, err := s.tcpClient.Get("http://127.0.0.1:55501/topics/test.4/consumers")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)

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
func (s *ServiceSuite) TestGetTopicConsumers(c *C) {
	// Given
	ResetOffsets(c, "foo", "test.4")
	ResetOffsets(c, "bar", "test.4")
	GenMessages(c, "get.consumers", "test.4", map[string]int{"A": 1, "B": 1, "C": 1})

	svc1 := spawnTestService(c, 55501)
	defer svc1.Stop()
	svc2 := spawnTestService(c, 55502)
	defer svc2.Stop()

	_, err := s.tcpClient.Get("http://127.0.0.1:55501/topics/test.4/messages?group=foo")
	c.Assert(err, IsNil)
	_, err = s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/messages?group=foo")
	c.Assert(err, IsNil)

	_, err = s.tcpClient.Get("http://127.0.0.1:55501/topics/test.1/messages?group=bar")
	c.Assert(err, IsNil)

	// When
	r, err := s.tcpClient.Get("http://127.0.0.1:55502/topics/test.4/consumers?group=foo")

	// Then
	c.Assert(err, IsNil)
	c.Assert(r.StatusCode, Equals, http.StatusOK)
	consumers := ParseJSONBody(c, r).(map[string]interface{})
	assertConsumedPartitions(c, consumers, map[string]map[string][]int32{
		"foo": {
			"C55501": {0, 1},
			"C55502": {2, 3}},
	})
}

func spawnTestService(c *C, port int) *Service {
	config := NewTestConfig(fmt.Sprintf("C%d", port))
	config.UnixAddr = fmt.Sprintf("%s.%d", config.UnixAddr, port)
	os.Remove(config.UnixAddr)
	config.TCPAddr = fmt.Sprintf("127.0.0.1:%d", port)
	svc, err := SpawnService(config)
	c.Assert(err, IsNil)
	return svc
}

func assertConsumedPartitions(c *C, consumers map[string]interface{}, expected map[string]map[string][]int32) {
	c.Assert(len(consumers), Equals, len(expected))
	for group, expectedClients := range expected {
		groupConsumers := consumers[group].(map[string]interface{})
		c.Assert(len(groupConsumers), Equals, len(expectedClients))
		for clientID, expectedPartitions := range expectedClients {
			clientPartitions := groupConsumers[clientID].([]interface{})
			partitions := make([]int32, len(clientPartitions))
			for i, p := range clientPartitions {
				partitions[i] = int32(p.(float64))
			}
			c.Assert(partitions, DeepEquals, expectedPartitions)
		}
	}
}
