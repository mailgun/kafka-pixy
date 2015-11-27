package pixy

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/mailgun/kafka-pixy/testhelpers"
)

func Test(t *testing.T) {
	TestingT(t)
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
	config.Kafka.SeedPeers = testhelpers.KafkaPeers
	config.ZooKeeper.SeedPeers = testhelpers.ZookeeperPeers
	config.Consumer.LongPollingTimeout = 3000 * time.Millisecond
	config.Consumer.BackOffTimeout = 100 * time.Millisecond
	config.Consumer.RebalanceDelay = 100 * time.Millisecond
	return config
}

func ResetOffsets(c *C, group, topic string) {
	config := config.Default()
	config.Kafka.SeedPeers = testhelpers.KafkaPeers
	config.ZooKeeper.SeedPeers = testhelpers.ZookeeperPeers

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
