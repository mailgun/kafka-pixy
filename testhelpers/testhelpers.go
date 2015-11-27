package testhelpers

import (
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
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
	consumer sarama.Consumer
}

func NewKafkaHelper(brokers []string) *KafkaHelper {
	tkc := &KafkaHelper{}
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

func (kh *KafkaHelper) Close() {
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
