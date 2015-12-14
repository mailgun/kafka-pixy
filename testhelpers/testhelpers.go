package testhelpers

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"path"
	"sort"
	"strings"
	"sync"
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
	KafkaPeers     []string
	ZookeeperPeers []string

	initTestOnce = sync.Once{}
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

func NewTestConfig(clientID string) *config.T {
	cfg := config.Default()
	cfg.UnixAddr = path.Join(os.TempDir(), "kafka-pixy.sock")
	cfg.ClientID = clientID
	cfg.Kafka.SeedPeers = KafkaPeers
	cfg.ZooKeeper.SeedPeers = ZookeeperPeers
	cfg.Consumer.LongPollingTimeout = 3000 * time.Millisecond
	cfg.Consumer.BackOffTimeout = 100 * time.Millisecond
	cfg.Consumer.RebalanceDelay = 100 * time.Millisecond
	return cfg
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
	c         *C
	client    sarama.Client
	producer  sarama.AsyncProducer
	consumer  sarama.Consumer
	offsetMgr sarama.OffsetManager
}

func NewKafkaHelper(c *C) *KafkaHelper {
	kh := &KafkaHelper{c: c}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	cfg.ClientID = "unittest-runner"
	err := error(nil)
	if kh.client, err = sarama.NewClient(KafkaPeers, cfg); err != nil {
		panic(err)
	}
	if kh.consumer, err = sarama.NewConsumerFromClient(kh.client); err != nil {
		panic(err)
	}
	if kh.producer, err = sarama.NewAsyncProducerFromClient(kh.client); err != nil {
		panic(err)
	}
	if kh.offsetMgr, err = sarama.NewOffsetManagerFromClient(kh.client); err != nil {
		panic(err)
	}
	return kh
}

func (kh *KafkaHelper) Close() {
	kh.offsetMgr.Close()
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

func (kh *KafkaHelper) PutMessages(prefix, topic string, keys map[string]int) map[string][]*sarama.ProducerMessage {
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
			kh.c.Error(prodErr)
		}
	}
	// Sort the produced messages in ascending order of their offsets.
	for _, keyMessages := range messages {
		sort.Sort(messageSlice(keyMessages))
	}
	wg.Wait()
	return messages
}

func (kh *KafkaHelper) ResetOffsets(group, topic string) {
	partitions, err := kh.client.Partitions(topic)
	kh.c.Assert(err, IsNil)
	for _, p := range partitions {
		offset, err := kh.client.GetOffset(topic, p, sarama.OffsetNewest)
		kh.c.Assert(err, IsNil)
		pom, err := kh.offsetMgr.ManagePartition(group, topic, p)
		kh.c.Assert(err, IsNil)
		pom.SubmitOffset(offset, "dummy")
		log.Infof("Set initial offset %s/%s/%d=%d", group, topic, p, offset)
		pom.Close()
	}
}

type messageSlice []*sarama.ProducerMessage

func (p messageSlice) Len() int           { return len(p) }
func (p messageSlice) Less(i, j int) bool { return p[i].Offset < p[j].Offset }
func (p messageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
