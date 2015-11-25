package pixy

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
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
	"github.com/mailgun/kafka-pixy/config"
)

const (
	// Use Shopify/sarama Vagrant box (copied over from https://github.com/Shopify/sarama/blob/master/functional_test.go#L18)
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	testKafkaPeers     []string
	testZookeeperPeers []string

	initLogOnce = sync.Once{}
)

func init() {
	kafkaPeersStr := os.Getenv("KAFKA_PEERS")
	if kafkaPeersStr == "" {
		kafkaPeersStr = VagrantKafkaPeers
	}
	testKafkaPeers = strings.Split(kafkaPeersStr, ",")

	zookeeperPeersStr := os.Getenv("ZOOKEEPER_PEERS")
	if zookeeperPeersStr == "" {
		zookeeperPeersStr = VagrantZookeeperPeers
	}
	testZookeeperPeers = strings.Split(zookeeperPeersStr, ",")
}

func Test(t *testing.T) {
	TestingT(t)
}

func InitTestLog() {
	initLogOnce.Do(func() {
		consoleLogger, _ := log.NewConsoleLogger(log.Config{Severity: "info"})
		log.Init(consoleLogger)
		InitLibraryLoggers()
	})
}

// NewUDSHTTPClient creates an HTTP client that always connects to the
// specified unix domain socket ignoring the host part of requested HTTP URLs.
func NewUDSHTTPClient(unixSockAddr string) *http.Client {
	dial := func(proto, addr string) (net.Conn, error) {
		return net.Dial("unix", unixSockAddr)
	}
	tr := &http.Transport{Dial: dial}
	return &http.Client{Transport: tr}
}

type TestKafkaClient struct {
	client   sarama.Client
	consumer sarama.Consumer
}

func NewTestKafkaClient(brokers []string) *TestKafkaClient {
	tkc := &TestKafkaClient{}
	clientCfg := sarama.NewConfig()
	clientCfg.ClientID = "unittest-runner"
	err := error(nil)
	if tkc.client, err = sarama.NewClient(brokers, clientCfg); err != nil {
		panic(err)
	}
	if tkc.consumer, err = sarama.NewConsumerFromClient(tkc.client); err != nil {
		panic(err)
	}
	return tkc
}

func (tkc *TestKafkaClient) Close() {
	tkc.consumer.Close()
	tkc.client.Close()
}

func (tkc *TestKafkaClient) getOffsets(topic string) []int64 {
	offsets := []int64{}
	partitions, err := tkc.client.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for _, p := range partitions {
		offset, err := tkc.client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		offsets = append(offsets, offset)
	}
	return offsets
}

func (tkc *TestKafkaClient) getMessages(topic string, begin, end []int64) [][]string {
	writtenMsgs := make([][]string, len(begin))
	for i := range begin {
		p, _, err := tkc.consumer.ConsumePartition(topic, int32(i), begin[i])
		if err != nil {
			panic(err)
		}
		writtenMsgCount := int(end[i] - begin[i])
		for j := 0; j < writtenMsgCount; j++ {
			connMsg := <-p.Messages()
			writtenMsgs[i] = append(writtenMsgs[i], string(connMsg.Value))
		}
		p.Close()
	}
	return writtenMsgs
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

func (cr *ChunkReader) Read(b []byte) (n int, err error) {
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

func NewTestConfig(clientID string) *config.T {
	config := config.Default()
	config.UnixAddr = path.Join(os.TempDir(), "kafka-pixy.sock")
	config.ClientID = clientID
	config.Kafka.SeedPeers = testKafkaPeers
	config.ZooKeeper.SeedPeers = testZookeeperPeers
	config.Consumer.LongPollingTimeout = 3000 * time.Millisecond
	config.Consumer.BackOffTimeout = 100 * time.Millisecond
	config.Consumer.RebalanceDelay = 100 * time.Millisecond
	return config
}

func ResetOffsets(c *C, group, topic string) {
	config := config.Default()
	config.Kafka.SeedPeers = testKafkaPeers
	config.ZooKeeper.SeedPeers = testZookeeperPeers

	kafkaClient, err := sarama.NewClient(config.Kafka.SeedPeers, config.SaramaConfig())
	c.Assert(err, IsNil)
	defer kafkaClient.Close()

	offsetManager, err := sarama.NewOffsetManagerFromClient(kafkaClient)
	c.Assert(err, IsNil)
	partitions, err := kafkaClient.Partitions(topic)
	c.Assert(err, IsNil)
	for _, p := range partitions {
		offset, err := kafkaClient.GetOffset(topic, p, sarama.OffsetNewest)
		c.Assert(err, IsNil)
		pom, err := offsetManager.ManagePartition(group, topic, p)
		c.Assert(err, IsNil)
		pom.SubmitOffset(offset, "dummy")
		log.Infof("Set initial offset %s/%s/%d=%d", group, topic, p, offset)
		pom.Close()
	}
	offsetManager.Close()
}

func GenMessages(c *C, prefix, topic string, keys map[string]int) map[string][]*sarama.ProducerMessage {
	config := config.Default()
	config.ClientID = "producer"
	config.Kafka.SeedPeers = testKafkaPeers
	producer, err := SpawnGracefulProducer(config)
	c.Assert(err, IsNil)

	messages := make(map[string][]*sarama.ProducerMessage)
	var wg sync.WaitGroup
	var lock sync.Mutex
	for key, count := range keys {
		for i := 0; i < count; i++ {
			key := key
			message := fmt.Sprintf("%s:%s:%d", prefix, key, i)
			spawn(&wg, func() {
				keyEncoder := sarama.StringEncoder(key)
				msgEncoder := sarama.StringEncoder(message)
				prodMsg, err := producer.Produce(topic, keyEncoder, msgEncoder)
				c.Assert(err, IsNil)
				log.Infof("*** produced: topic=%s, partition=%d, offset=%d, message=%s",
					topic, prodMsg.Partition, prodMsg.Offset, message)
				lock.Lock()
				messages[key] = append(messages[key], prodMsg)
				lock.Unlock()
			})
		}
	}
	wg.Wait()
	// Sort the produced messages in ascending order of their offsets.
	for _, keyMessages := range messages {
		sort.Sort(MessageSlice(keyMessages))
	}
	return messages
}

type MessageSlice []*sarama.ProducerMessage

func (p MessageSlice) Len() int           { return len(p) }
func (p MessageSlice) Less(i, j int) bool { return p[i].Offset < p[j].Offset }
func (p MessageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

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

func ParseBase64(c *C, encoded string) string {
	decoder := base64.NewDecoder(base64.StdEncoding, strings.NewReader(encoded))
	decoded, err := ioutil.ReadAll(decoder)
	if err != nil {
		c.Error(err)
		return ""
	}
	return string(decoded)
}

func ProdMsgVal(prodMsg *sarama.ProducerMessage) string {
	return string(prodMsg.Value.(sarama.StringEncoder))
}
