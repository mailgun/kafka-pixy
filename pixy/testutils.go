package pixy

import (
	"net"
	"net/http"
	"strconv"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
)

var initLogOnce = sync.Once{}

func InitTestLog() {
	initLogOnce.Do(func() {
		log.Init([]*log.LogConfig{&log.LogConfig{Name: "console"}})
		log.SetSeverity(log.SeverityInfo)
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
	size, _ := strconv.Atoi(r.Header.Get(contentLength))
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

func (tkc *TestKafkaClient) getMessages(topic string, before, after []int64) [][]string {
	writtenMsgs := make([][]string, len(before))
	for i := range before {
		p, err := tkc.consumer.ConsumePartition(topic, int32(i), before[i])
		if err != nil {
			panic(err)
		}
		writtenMsgCount := int(after[i] - before[i])
		for j := 0; j < writtenMsgCount; j++ {
			connMsg := <-p.Messages()
			writtenMsgs[i] = append(writtenMsgs[i], string(connMsg.Value))
		}
		p.Close()
	}
	return writtenMsgs
}
