package kazoo

import (
	"sort"
	"testing"
)

func TestPartition(t *testing.T) {
	topic := &Topic{Name: "test"}
	partition := topic.Partition(1, []int32{1, 2, 3})

	if key := partition.Key(); key != "test/1" {
		t.Error("Unexpected partition key", key)
	}

	if partition.Topic() != topic {
		t.Error("Expected Topic() to return the topic the partition was created from.")
	}

	if pr := partition.PreferredReplica(); pr != 1 {
		t.Error("Expected 1 to be the preferred replica, but found", pr)
	}

	partitionWithoutReplicas := topic.Partition(1, nil)
	if pr := partitionWithoutReplicas.PreferredReplica(); pr != -1 {
		t.Error("Expected -1 to be returned if the partition does not have replicas, but found", pr)
	}
}

func TestTopicList(t *testing.T) {
	topics := TopicList{
		&Topic{Name: "foo"},
		&Topic{Name: "bar"},
		&Topic{Name: "baz"},
	}

	sort.Sort(topics)

	if topics[0].Name != "bar" || topics[1].Name != "baz" || topics[2].Name != "foo" {
		t.Error("Unexpected order after sorting topic list", topics)
	}

	topic := topics.Find("foo")
	if topic != topics[2] {
		t.Error("Should have found foo topic from the list")
	}
}

func TestPartitionList(t *testing.T) {
	var (
		topic1 = &Topic{Name: "1"}
		topic2 = &Topic{Name: "2"}
	)

	var (
		partition21 = topic2.Partition(1, nil)
		partition12 = topic1.Partition(2, nil)
		partition11 = topic1.Partition(1, nil)
	)

	partitions := PartitionList{partition21, partition12, partition11}
	sort.Sort(partitions)

	if partitions[0] != partition11 || partitions[1] != partition12 || partitions[2] != partition21 {
		t.Error("Unexpected order after sorting topic list", partitions)
	}
}
