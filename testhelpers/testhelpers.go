package testhelpers

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/log"
	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
	. "github.com/mailgun/kafka-pixy/Godeps/_workspace/src/gopkg.in/check.v1"
)

const (
	// Use Shopify/sarama Vagrant box (copied over from https://github.com/Shopify/sarama/blob/master/functional_test.go#L18)
	VagrantKafkaPeers     = "192.168.100.67:9091,192.168.100.67:9092,192.168.100.67:9093,192.168.100.67:9094,192.168.100.67:9095"
	VagrantZookeeperPeers = "192.168.100.67:2181,192.168.100.67:2182,192.168.100.67:2183,192.168.100.67:2184,192.168.100.67:2185"
)

var (
	KafkaPeers     []string
	ZookeeperPeers []string
)

func init() {
	kafkaPeersStr := os.Getenv("KAFKA_PEERS")
	if kafkaPeersStr == "" {
		kafkaPeersStr = VagrantKafkaPeers
	}
	KafkaPeers = strings.Split(kafkaPeersStr, ",")

	zookeeperPeersStr := os.Getenv("ZOOKEEPER_PEERS")
	if zookeeperPeersStr == "" {
		zookeeperPeersStr = VagrantZookeeperPeers
	}
	ZookeeperPeers = strings.Split(zookeeperPeersStr, ",")
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

type KafkaHelper struct {
	client   sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.Consumer
}

func NewKafkaHelper(brokers []string) *KafkaHelper {
	kh := &KafkaHelper{}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.ClientID = "unittest-runner"
	err := error(nil)
	if kh.client, err = sarama.NewClient(brokers, cfg); err != nil {
		panic(err)
	}
	if kh.consumer, err = sarama.NewConsumerFromClient(kh.client); err != nil {
		panic(err)
	}
	if kh.producer, err = sarama.NewAsyncProducerFromClient(kh.client); err != nil {
		panic(err)
	}
	return kh
}

func (kh *KafkaHelper) Close() {
	kh.producer.Close()
	kh.consumer.Close()
	kh.client.Close()
}

func (kh *KafkaHelper) GetOffsets(topic string) []int64 {
	offsets := []int64{}
	partitions, err := kh.client.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for _, p := range partitions {
		offset, err := kh.client.GetOffset(topic, p, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}
		offsets = append(offsets, offset)
	}
	return offsets
}

func (kh *KafkaHelper) GetMessages(topic string, begin, end []int64) [][]string {
	writtenMsgs := make([][]string, len(begin))
	for i := range begin {
		p, _, err := kh.consumer.ConsumePartition(topic, int32(i), begin[i])
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

func (kh *KafkaHelper) PutMessages(c *C, prefix, topic string, keys map[string]int) map[string][]*sarama.ProducerMessage {
	messages := make(map[string][]*sarama.ProducerMessage)
	var wg sync.WaitGroup
	total := 0
	for key, count := range keys {
		total += count
		for i := 0; i < count; i++ {
			key := key
			message := fmt.Sprintf("%s:%s:%d", prefix, key, i)
			wg.Add(1)
			go func() {
				defer wg.Done()
				keyEncoder := sarama.StringEncoder(key)
				msgEncoder := sarama.StringEncoder(message)
				prodMsg := &sarama.ProducerMessage{
					Topic: topic,
					Key:   keyEncoder,
					Value: msgEncoder,
				}
				kh.producer.Input() <- prodMsg
			}()
		}
	}
	for i := 0; i < total; i++ {
		select {
		case prodMsg := <-kh.producer.Successes():
			key := string(prodMsg.Key.(sarama.StringEncoder))
			messages[key] = append(messages[key], prodMsg)
			log.Infof("*** produced: topic=%s, partition=%d, offset=%d, message=%s",
				topic, prodMsg.Partition, prodMsg.Offset, prodMsg.Value)
		case prodErr := <-kh.producer.Errors():
			c.Error(prodErr)
		}
	}
	// Sort the produced messages in ascending order of their offsets.
	for _, keyMessages := range messages {
		sort.Sort(messageSlice(keyMessages))
	}
	wg.Wait()
	return messages
}

type messageSlice []*sarama.ProducerMessage

func (p messageSlice) Len() int           { return len(p) }
func (p messageSlice) Less(i, j int) bool { return p[i].Offset < p[j].Offset }
func (p messageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
