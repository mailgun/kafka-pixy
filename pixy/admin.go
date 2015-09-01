package pixy

import (
	"fmt"
	"sync"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/mailgun/sarama"
)

type (
	ErrAdminSetup    error
	ErrAdminKafkaReq struct {
		err  error
		desc string
	}
)

func NewErrAdminKafkaReq(err error, format string, v ...interface{}) ErrAdminKafkaReq {
	return ErrAdminKafkaReq{err, fmt.Sprintf(format, v...)}
}

func (e ErrAdminKafkaReq) Error() string {
	return fmt.Sprintf("%s, err=(%s)", e.desc, e.err)
}

func (e ErrAdminKafkaReq) Cause() error {
	return e.err
}

const (
	ProtocolVer1 = 1 // Supported by Kafka v0.8.2 and later
)

// Admin provides methods to perform miscellaneous administrative operations
// on the Kafka cluster.
type Admin struct {
	kafkaClient sarama.Client
}

// SpawnAdmin creates an `Admin` instance with the specified configuration and
// starts internal goroutines to support its operation.
func SpawnAdmin(config *Config) (*Admin, error) {
	kafkaClient, err := sarama.NewClient(config.Kafka.SeedPeers, config.saramaConfig())
	if err != nil {
		return nil, ErrAdminSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	a := Admin{kafkaClient: kafkaClient}
	return &a, nil
}

// Stop gracefully terminates internal goroutines.
func (a *Admin) Stop() {
	a.kafkaClient.Close()
}

type PartitionOffset struct {
	Partition int32
	Range     struct {
		Begin int64
		End   int64
	}
	Offset   int64
	Metadata string
}

type indexedPartition struct {
	index     int
	partition int32
}

// GetGroupOffsets for every partition of the specified topic it returns the
// current offset range along with the latest offset and metadata committed by
// the specified consumer group.
func (a *Admin) GetGroupOffsets(group, topic string) ([]PartitionOffset, error) {
	var err error
	partitions, err := a.kafkaClient.Partitions(topic)
	if err != nil {
		return nil, NewErrAdminKafkaReq(err, "failed to get topic partitions")
	}

	// Figure out distribution of partitions among brokers.
	brokerToPartitions := make(map[*sarama.Broker][]indexedPartition)
	for i, p := range partitions {
		broker, err := a.kafkaClient.Leader(topic, p)
		if err != nil {
			return nil, NewErrAdminKafkaReq(err, "failed to get partition leader: partition=%d", p)
		}
		brokerToPartitions[broker] = append(brokerToPartitions[broker], indexedPartition{i, p})
	}

	// Query brokers for the oldest and newest offsets of the partitions that
	// they are leaders for.
	offsets := make([]PartitionOffset, len(partitions))
	var wg sync.WaitGroup
	errorsCh := make(chan ErrAdminKafkaReq, len(brokerToPartitions))
	for broker, brokerPartitions := range brokerToPartitions {
		broker, brokerPartitions := broker, brokerPartitions
		var reqNewest sarama.OffsetRequest
		var reqOldest sarama.OffsetRequest
		for _, p := range brokerPartitions {
			reqNewest.AddBlock(topic, p.partition, sarama.OffsetNewest, 1)
			reqOldest.AddBlock(topic, p.partition, sarama.OffsetOldest, 1)
		}
		spawn(&wg, func() {
			resOldest, err := broker.GetAvailableOffsets(&reqOldest)
			if err != nil {
				errorsCh <- NewErrAdminKafkaReq(err, "failed to fetch oldest offset: broker=%v", broker.ID())
				return
			}
			resNewest, err := broker.GetAvailableOffsets(&reqNewest)
			if err != nil {
				errorsCh <- NewErrAdminKafkaReq(err, "failed to fetch newest offset: broker=%v", broker.ID())
				return
			}
			for _, xp := range brokerPartitions {
				begin, err := getOffsetResult(resOldest, topic, xp.partition)
				if err != nil {
					errorsCh <- NewErrAdminKafkaReq(err, "failed to fetch oldest offset: broker=%v", broker.ID())
					return
				}
				end, err := getOffsetResult(resNewest, topic, xp.partition)
				if err != nil {
					errorsCh <- NewErrAdminKafkaReq(err, "failed to fetch newest offset: broker=%v", broker.ID())
					return
				}
				offsets[xp.index].Partition = xp.partition
				offsets[xp.index].Range.Begin = begin
				offsets[xp.index].Range.End = end
			}
		})
	}
	wg.Wait()
	// If we failed to get offset range for at least one of the partitions then
	// return the first error that was reported.
	close(errorsCh)
	if err, ok := <-errorsCh; ok {
		return nil, err
	}

	// Fetch the last committed offsets for all partitions of the group/topic.
	coordinator, err := a.kafkaClient.Coordinator(group)
	if err != nil {
		return nil, NewErrAdminKafkaReq(err, "failed to get coordinator")
	}
	req := sarama.OffsetFetchRequest{ConsumerGroup: group, Version: ProtocolVer1}
	for _, p := range partitions {
		req.AddPartition(topic, p)
	}
	res, err := coordinator.FetchOffset(&req)
	if err != nil {
		return nil, NewErrAdminKafkaReq(err, "failed to fetch offsets")
	}
	for i, p := range partitions {
		block := res.GetBlock(topic, p)
		if block == nil {
			return nil, NewErrAdminKafkaReq(nil, "offset block is missing: partition=%d", p)
		}
		offsets[i].Offset = block.Offset
		offsets[i].Metadata = block.Metadata
	}

	return offsets, nil
}

// SetGroupOffsets commits specific offset values along with metadata for a list
// of partitions of a particular topic on behalf of the specified group.
func (a *Admin) SetGroupOffsets(group, topic string, offsets []PartitionOffset) error {
	coordinator, err := a.kafkaClient.Coordinator(group)
	if err != nil {
		return NewErrAdminKafkaReq(err, "failed to get coordinator")
	}

	req := sarama.OffsetCommitRequest{ConsumerGroup: group, Version: ProtocolVer1}
	for _, po := range offsets {
		req.AddBlock(topic, po.Partition, po.Offset, sarama.ReceiveTime, po.Metadata)
	}
	res, err := coordinator.CommitOffset(&req)
	if err != nil {
		return NewErrAdminKafkaReq(err, "failed to commit offsets")
	}
	for p, err := range res.Errors[topic] {
		if err != sarama.ErrNoError {
			return NewErrAdminKafkaReq(err, "failed to commit offset: partition=%d", p)
		}
	}
	return nil
}

func getOffsetResult(res *sarama.OffsetResponse, topic string, partition int32) (int64, error) {
	block := res.GetBlock(topic, partition)
	if block == nil {
		return 0, fmt.Errorf("%s/%d: no data", topic, partition)
	}
	if block.Err != sarama.ErrNoError {
		return 0, fmt.Errorf("%s/%d: fetch error: (%v)", topic, partition, block.Err)
	}
	if len(block.Offsets) < 1 {
		return 0, fmt.Errorf("%s/%d: no offset", topic, partition)
	}
	return block.Offsets[0], nil
}
