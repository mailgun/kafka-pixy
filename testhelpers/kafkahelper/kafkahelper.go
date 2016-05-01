package kafkahelper

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/consumer/offsetmgr"
	"github.com/mailgun/kafka-pixy/testhelpers"
	"github.com/mailgun/log"
	. "gopkg.in/check.v1"
)

type T struct {
	ns       *actor.ID
	c        *C
	client   sarama.Client
	producer sarama.AsyncProducer
	consumer sarama.Consumer
}

func New(c *C) *T {
	kh := &T{ns: actor.RootID.NewChild("kafka_helper"), c: c}
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	cfg.Consumer.Offsets.CommitInterval = 50 * time.Millisecond
	cfg.ClientID = "unittest-runner"
	err := error(nil)
	if kh.client, err = sarama.NewClient(testhelpers.KafkaPeers, cfg); err != nil {
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

func (kh *T) Client() sarama.Client {
	return kh.client
}

func (kh *T) Close() {
	kh.producer.Close()
	kh.consumer.Close()
	kh.client.Close()
}

func (kh *T) GetNewestOffsets(topic string) []int64 {
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

func (kh *T) GetOldestOffsets(topic string) []int64 {
	offsets := []int64{}
	partitions, err := kh.client.Partitions(topic)
	if err != nil {
		panic(err)
	}
	for _, p := range partitions {
		offset, err := kh.client.GetOffset(topic, p, sarama.OffsetOldest)
		if err != nil {
			panic(err)
		}
		offsets = append(offsets, offset)
	}
	return offsets
}

func (kh *T) GetMessages(topic string, begin, end []int64) [][]string {
	writtenMsgs := make([][]string, len(begin))
	for i := range begin {
		p, err := kh.consumer.ConsumePartition(topic, int32(i), begin[i])
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

func (kh *T) PutMessages(prefix, topic string, keys map[string]int) map[string][]*sarama.ProducerMessage {
	messages := make(map[string][]*sarama.ProducerMessage)
	var wg sync.WaitGroup
	total := 0
	for key, count := range keys {
		total += count
		wg.Add(1)
		go func(key string, count int) {
			defer wg.Done()
			for i := 0; i < count; i++ {
				message := fmt.Sprintf("%s:%s:%d", prefix, key, i)
				keyEncoder := sarama.StringEncoder(key)
				msgEncoder := sarama.StringEncoder(message)
				prodMsg := &sarama.ProducerMessage{
					Topic: topic,
					Key:   keyEncoder,
					Value: msgEncoder,
				}
				kh.producer.Input() <- prodMsg
			}
		}(key, count)
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

func (kh *T) ResetOffsets(group, topic string) {
	omf := offsetmgr.SpawnFactory(kh.ns, kh.client)
	defer omf.Stop()
	partitions, err := kh.client.Partitions(topic)
	kh.c.Assert(err, IsNil)
	for _, p := range partitions {
		offset, err := kh.client.GetOffset(topic, p, sarama.OffsetNewest)
		kh.c.Assert(err, IsNil)
		om, err := omf.SpawnOffsetManager(kh.ns, group, topic, p)
		kh.c.Assert(err, IsNil)
		om.SubmitOffset(offset, "dummy")
		log.Infof("Set initial offset %s/%s/%d=%d", group, topic, p, offset)
		om.Stop()
	}
}

type messageSlice []*sarama.ProducerMessage

func (p messageSlice) Len() int           { return len(p) }
func (p messageSlice) Less(i, j int) bool { return p[i].Offset < p[j].Offset }
func (p messageSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
