package admin

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/mailgun/kafka-pixy/actor"
	"github.com/mailgun/kafka-pixy/config"
	"github.com/samuel/go-zookeeper/zk"
)

type (
	ErrSetup        error
	ErrInvalidParam error
	ErrQuery        struct {
		err  error
		desc string
	}
)

func NewErrQuery(err error, format string, v ...interface{}) ErrQuery {
	return ErrQuery{err, fmt.Sprintf(format, v...)}
}

func (e ErrQuery) Error() string {
	return fmt.Sprintf("%s, err=(%s)", e.desc, e.err)
}

func (e ErrQuery) Cause() error {
	return e.err
}

const (
	ProtocolVer1 = 1 // Supported by Kafka v0.8.2 and later
)

// T provides methods to perform administrative operations on a Kafka cluster.
type T struct {
	namespace *actor.ID
	cfg       *config.T
}

// Spawn creates an admin instance with the specified configuration and starts
// internal goroutines to support its operation.
func Spawn(namespace *actor.ID, config *config.T) (*T, error) {
	a := T{
		namespace: namespace,
		cfg:       config,
	}
	return &a, nil
}

// Stop gracefully terminates internal goroutines.
func (a *T) Stop() {
}

type PartitionOffset struct {
	Partition int32
	Begin     int64
	End       int64
	Offset    int64
	Metadata  string
}

type indexedPartition struct {
	index     int
	partition int32
}

// GetGroupOffsets for every partition of the specified topic it returns the
// current offset range along with the latest offset and metadata committed by
// the specified consumer group.
func (a *T) GetGroupOffsets(group, topic string) ([]PartitionOffset, error) {
	kafkaClt, err := sarama.NewClient(a.cfg.Kafka.SeedPeers, a.saramaConfig())
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	defer kafkaClt.Close()

	partitions, err := kafkaClt.Partitions(topic)
	if err != nil {
		return nil, NewErrQuery(err, "failed to get topic partitions")
	}

	// Figure out distribution of partitions among brokers.
	brokerToPartitions := make(map[*sarama.Broker][]indexedPartition)
	for i, p := range partitions {
		broker, err := kafkaClt.Leader(topic, p)
		if err != nil {
			return nil, NewErrQuery(err, "failed to get partition leader: partition=%d", p)
		}
		brokerToPartitions[broker] = append(brokerToPartitions[broker], indexedPartition{i, p})
	}

	// Query brokers for the oldest and newest offsets of the partitions that
	// they are leaders for.
	offsets := make([]PartitionOffset, len(partitions))
	var wg sync.WaitGroup
	errorsCh := make(chan ErrQuery, len(brokerToPartitions))
	for broker, brokerPartitions := range brokerToPartitions {
		broker, brokerPartitions := broker, brokerPartitions
		var reqNewest sarama.OffsetRequest
		var reqOldest sarama.OffsetRequest
		for _, p := range brokerPartitions {
			reqNewest.AddBlock(topic, p.partition, sarama.OffsetNewest, 1)
			reqOldest.AddBlock(topic, p.partition, sarama.OffsetOldest, 1)
		}
		actorID := actor.RootID.NewChild("adminOffsetFetcher")
		actor.Spawn(actorID, &wg, func() {
			resOldest, err := broker.GetAvailableOffsets(&reqOldest)
			if err != nil {
				errorsCh <- NewErrQuery(err, "failed to fetch oldest offset: broker=%v", broker.ID())
				return
			}
			resNewest, err := broker.GetAvailableOffsets(&reqNewest)
			if err != nil {
				errorsCh <- NewErrQuery(err, "failed to fetch newest offset: broker=%v", broker.ID())
				return
			}
			for _, xp := range brokerPartitions {
				begin, err := getOffsetResult(resOldest, topic, xp.partition)
				if err != nil {
					errorsCh <- NewErrQuery(err, "failed to fetch oldest offset: broker=%v", broker.ID())
					return
				}
				end, err := getOffsetResult(resNewest, topic, xp.partition)
				if err != nil {
					errorsCh <- NewErrQuery(err, "failed to fetch newest offset: broker=%v", broker.ID())
					return
				}
				offsets[xp.index].Partition = xp.partition
				offsets[xp.index].Begin = begin
				offsets[xp.index].End = end
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
	coordinator, err := kafkaClt.Coordinator(group)
	if err != nil {
		return nil, NewErrQuery(err, "failed to get coordinator")
	}
	req := sarama.OffsetFetchRequest{ConsumerGroup: group, Version: ProtocolVer1}
	for _, p := range partitions {
		req.AddPartition(topic, p)
	}
	res, err := coordinator.FetchOffset(&req)
	if err != nil {
		return nil, NewErrQuery(err, "failed to fetch offsets")
	}
	for i, p := range partitions {
		block := res.GetBlock(topic, p)
		if block == nil {
			return nil, NewErrQuery(nil, "offset block is missing: partition=%d", p)
		}
		offsets[i].Offset = block.Offset
		offsets[i].Metadata = block.Metadata
	}

	return offsets, nil
}

// SetGroupOffsets commits specific offset values along with metadata for a list
// of partitions of a particular topic on behalf of the specified group.
func (a *T) SetGroupOffsets(group, topic string, offsets []PartitionOffset) error {
	kafkaClt, err := sarama.NewClient(a.cfg.Kafka.SeedPeers, a.saramaConfig())
	if err != nil {
		return ErrSetup(fmt.Errorf("failed to create sarama.Client: err=(%v)", err))
	}
	defer kafkaClt.Close()

	coordinator, err := kafkaClt.Coordinator(group)
	if err != nil {
		return NewErrQuery(err, "failed to get coordinator")
	}

	req := sarama.OffsetCommitRequest{
		Version:                 ProtocolVer1,
		ConsumerGroup:           group,
		ConsumerGroupGeneration: sarama.GroupGenerationUndefined,
	}
	for _, po := range offsets {
		req.AddBlock(topic, po.Partition, po.Offset, sarama.ReceiveTime, po.Metadata)
	}
	res, err := coordinator.CommitOffset(&req)
	if err != nil {
		return NewErrQuery(err, "failed to commit offsets")
	}
	for p, err := range res.Errors[topic] {
		if err != sarama.ErrNoError {
			return NewErrQuery(err, "failed to commit offset: partition=%d", p)
		}
	}
	return nil
}

// GetTopicConsumers returns client-id -> consumed-partitions-list mapping
// for a clients from a particular consumer group and a particular topic.
func (a *T) GetTopicConsumers(group, topic string) (map[string][]int32, error) {
	zookeeperClt, _, err := zk.Connect(a.cfg.ZooKeeper.SeedPeers, 1*time.Second)
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create zk.Conn: err=(%v)", err))
	}
	defer zookeeperClt.Close()

	consumedPartitionsPath := fmt.Sprintf("%s/consumers/%s/owners/%s",
		a.cfg.ZooKeeper.Chroot, group, topic)
	partitionNodes, _, err := zookeeperClt.Children(consumedPartitionsPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return nil, ErrInvalidParam(errors.New("either group or topic is incorrect"))
		}
		return nil, NewErrQuery(err, "failed to fetch partition owners data")
	}

	consumers := make(map[string][]int32)
	for _, partitionNode := range partitionNodes {
		partition, err := strconv.Atoi(partitionNode)
		if err != nil {
			return nil, NewErrQuery(err, "invalid partition id: %s", partitionNode)
		}
		partitionPath := fmt.Sprintf("%s/%s", consumedPartitionsPath, partitionNode)
		partitionNodeData, _, err := zookeeperClt.Get(partitionPath)
		if err != nil {
			return nil, NewErrQuery(err, "failed to fetch partition owner")
		}
		clientID := string(partitionNodeData)
		consumers[clientID] = append(consumers[clientID], int32(partition))
	}

	for _, partitions := range consumers {
		sort.Sort(int32Slice(partitions))
	}

	return consumers, nil
}

// GetAllTopicConsumers returns group -> client-id -> consumed-partitions-list
// mapping for a particular topic. Warning, the function performs scan of all
// consumer groups registered in ZooKeeper and therefore can take a lot of time.
func (a *T) GetAllTopicConsumers(topic string) (map[string]map[string][]int32, error) {
	zookeeperClt, _, err := zk.Connect(a.cfg.ZooKeeper.SeedPeers, 1*time.Second)
	if err != nil {
		return nil, ErrSetup(fmt.Errorf("failed to create zk.Conn: err=(%v)", err))
	}
	defer zookeeperClt.Close()

	groupsPath := fmt.Sprintf("%s/consumers", a.cfg.ZooKeeper.Chroot)
	groups, _, err := zookeeperClt.Children(groupsPath)
	if err != nil {
		return nil, NewErrQuery(err, "failed to fetch consumer groups")
	}

	consumers := make(map[string]map[string][]int32)
	for _, group := range groups {
		groupConsumers, err := a.GetTopicConsumers(group, topic)
		if err != nil {
			if _, ok := err.(ErrInvalidParam); ok {
				continue
			}
			return nil, NewErrQuery(err, "failed to fetch group `%s` data", group)
		}
		if len(groupConsumers) > 0 {
			consumers[group] = groupConsumers
		}
	}
	return consumers, nil
}

// saramaConfig generates a `Shopify/sarama` library config.
func (a *T) saramaConfig() *sarama.Config {
	saramaConfig := sarama.NewConfig()
	saramaConfig.ClientID = a.cfg.ClientID
	return saramaConfig
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

type int32Slice []int32

func (p int32Slice) Len() int           { return len(p) }
func (p int32Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p int32Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
