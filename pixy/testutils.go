package pixy

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

var initLogOnce = sync.Once{}

func InitTestLog() {
	initLogOnce.Do(func() {
		consoleLogger, _ := log.NewConsoleLogger(log.Config{Severity: "info"})
		log.Init(consoleLogger)
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

// ResponseBody returns the content of an HTTP response as a string.
func ResponseBody(r *http.Response) string {
	if r == nil || r.Body == nil {
		return ""
	}
	defer r.Body.Close()
	size, _ := strconv.Atoi(r.Header.Get(HeaderContentLength))
	body := make([]byte, size)
	r.Body.Read(body)
	return string(body)
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
		p, err := tkc.consumer.ConsumePartition(topic, int32(i), begin[i])
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
	chunks   []string
	chunk    string
	pause    time.Duration
	chunkDue time.Time
}

func NewChunkReader(s string, count int, pause time.Duration) *ChunkReader {
	chunkSize := len(s) / count
	chunks := make([]string, count, count+1)
	for i := 0; i < count; i++ {
		begin := chunkSize * i
		end := begin + chunkSize
		chunks[i] = s[begin:end]
	}
	if count*chunkSize != len(s) {
		chunks = append(chunks, s[chunkSize*count:])
	}
	return &ChunkReader{
		chunks:   chunks,
		pause:    pause,
		chunkDue: time.Now().Add(pause),
	}
}

func (cr *ChunkReader) Read(b []byte) (n int, err error) {
	if len(cr.chunk) == 0 {
		if len(cr.chunks) == 0 {
			return 0, io.EOF
		}
		cr.chunk = cr.chunks[0]
		cr.chunks = cr.chunks[1:]
		cr.chunkDue = time.Now().Add(cr.pause)
	}

	waitFor := time.Now().Sub(cr.chunkDue)
	if waitFor > 0 {
		time.Sleep(waitFor)
		return 0, nil
	}

	copied := copy(b, cr.chunk)
	cr.chunk = cr.chunk[copied:]
	return copied, nil
}

func PostChunked(clt *http.Client, url, msg string) *http.Response {
	req, err := http.NewRequest("POST", url, NewChunkReader(msg, 1, 10*time.Millisecond))
	if err != nil {
		panic(fmt.Sprintf("Failed to make a request object: cause=(%v)", err))
	}
	req.Header.Add("Content-Type", "text/plain")
	req.ContentLength = int64(len(msg))
	resp, err := clt.Do(req)
	if err != nil {
		panic(fmt.Sprintf("Failed to do a request: cause=(%v)", err))
	}
	return resp
}

func AssertHTTPResp(c *C, resp *http.Response, expectedStatusCode int, expectedBody string) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
	c.Assert(string(body), DeepEquals, expectedBody)
	c.Assert(resp.StatusCode, Equals, expectedStatusCode)
}

func ProdMsgMetadataSize(key []byte) int {
	size := 26 // the metadata overhead of CRC, flags, etc.
	if key != nil {
		size += sarama.ByteEncoder(key).Length()
	}
	return size
}
