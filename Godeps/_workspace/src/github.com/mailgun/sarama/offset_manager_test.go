package sarama

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mailgun/kafka-pixy/Godeps/_workspace/src/github.com/davecgh/go-spew/spew"
)

// When a partition consumer is created, then an initial offset is sent down
// the InitialOffset() channel.
func TestOffsetManagerInitialOffset(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 1000, "foo", ErrNoError).
			SetOffset("group-1", "topic-1", 8, 2000, "bar", ErrNoError).
			SetOffset("group-1", "topic-2", 9, 3000, "bazz", ErrNoError),
	})

	client, err := NewClient([]string{broker1.Addr()}, nil)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pom, err := om.ManagePartition("group-1", "topic-1", 8)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	fo := <-pom.InitialOffset()
	if !reflect.DeepEqual(fo, FetchedOffset{2000, "bar"}) {
		t.Errorf("Unexpected initial offset: %#v", fo)
	}

	om.Close()
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func TestOffsetManagerInitialNoCoordinator(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetError("group-1", ErrOffsetsLoadInProgress),
	})

	config := NewConfig()
	config.Consumer.Retry.Backoff = 50 * time.Millisecond
	config.Consumer.Return.Errors = true
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pom, err := om.ManagePartition("group-1", "topic-1", 8)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	oce := <-pom.Errors()
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 8, ErrOffsetMgrNoCoordinator}) {
		t.Errorf("Unexpected error: %v", oce)
	}

	om.Close()
}

// A partition offset manager can be closed even while it keeps trying to
// resolve the coordinator for the broker.
func TestOffsetManagerInitialFetchError(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()
	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 0, "", ErrNotLeaderForPartition),
	})

	config := NewConfig()
	config.Consumer.Retry.Backoff = 50 * time.Millisecond
	config.Consumer.Return.Errors = true
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pom, err := om.ManagePartition("group-1", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}

	// Then
	oce := <-pom.Errors()
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrNotLeaderForPartition}) {
		t.Errorf("Unexpected error: %v", oce)
	}

	om.Close()
}

// If offset commit fails then the corresponding error is sent down to the
// errors channel, but the partition offset manager keeps retrying until it
// succeeds.
func TestOffsetManagerCommitError(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 1234, "foo", ErrNoError),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrNotLeaderForPartition),
	})

	config := NewConfig()
	config.Consumer.Retry.Backoff = 1000 * time.Millisecond
	config.Consumer.Return.Errors = true
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	pom, err := om.ManagePartition("group-1", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pom.CommitOffset(1000, "foo")
	var wg sync.WaitGroup
	spawn(&wg, pom.Close)

	// Then
	oce := <-pom.Errors()
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrNotLeaderForPartition}) {
		t.Errorf("Unexpected error: %v", oce)
	}

	broker1.SetHandlerByMap(map[string]MockResponse{
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrNoError),
	})

	wg.Wait()
	committedOffset := lastCommittedOffset(broker1, "group-1", "topic-1", 7)
	if committedOffset != (FetchedOffset{1000, "foo"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset))
	}
	om.Close()
}

// It is guaranteed that a partition offset manager commits all pending offsets
// before it terminates. Note that it will try indefinitely by design.
func TestOffsetManagerCommitBeforeClose(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
	})

	config := NewConfig()
	config.Net.ReadTimeout = 10 * time.Millisecond
	config.Consumer.Retry.Backoff = 25 * time.Millisecond
	config.Consumer.Return.Errors = true
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	pom, err := om.ManagePartition("group-1", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}

	// When: a partition offset manager is closed while there is a pending commit.
	pom.CommitOffset(1001, "foo")
	go pom.Close()

	// Then: the partition offset manager terminates only after it has
	// successfully committed the offset.

	// STAGE 1: Requests for coordinator time out.
	Logger.Printf("    STAGE 1")
	oce := <-pom.Errors()
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrOffsetMgrNoCoordinator}) {
		t.Fatalf("Unexpected error: %v", oce)
	}

	// STAGE 2: Requests for initial offset return errors
	Logger.Printf("    STAGE 2")
	broker1.SetHandlerByMap(map[string]MockResponse{
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 0, "", ErrNotLeaderForPartition),
	})
	for oce = range pom.Errors() {
		if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrOffsetMgrNoCoordinator}) {
			break
		}
	}
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrNotLeaderForPartition}) {
		t.Fatalf("Unexpected error: %v", oce)
	}

	// STAGE 3: Offset commit requests fail
	Logger.Printf("    STAGE 3")
	broker1.SetHandlerByMap(map[string]MockResponse{
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 1234, "foo", ErrNoError),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrOffsetMetadataTooLarge),
	})
	for oce = range pom.Errors() {
		if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrNotLeaderForPartition}) {
			break
		}
	}
	if !reflect.DeepEqual(oce, &OffsetCommitError{"group-1", "topic-1", 7, ErrOffsetMetadataTooLarge}) {
		t.Fatalf("Unexpected error: %v", oce)
	}

	// STAGE 4: Finally everything is fine
	Logger.Printf("    STAGE 4")
	broker1.SetHandlerByMap(map[string]MockResponse{
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 0, "", ErrNoError),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrNoError),
	})
	// The errors channel is closed when the partition offset manager has
	// terminated.
	for oce := range pom.Errors() {
		Logger.Printf("Drain error: %v", oce)
	}

	committedOffset := lastCommittedOffset(broker1, "group-1", "topic-1", 7)
	if committedOffset != (FetchedOffset{1001, "foo"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset))
	}
	om.Close()
}

// Different consumer groups can keep different offsets for the same
// topic/partition, even where they have the same broker as a coordinator.
func TestOffsetManagerCommitDifferentGroups(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1).
			SetCoordinator("group-2", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 1000, "foo", ErrNoError).
			SetOffset("group-2", "topic-1", 7, 2000, "bar", ErrNoError),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrNoError).
			SetError("group-2", "topic-1", 7, ErrNoError),
	})

	config := NewConfig()
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	pom1, err := om.ManagePartition("group-1", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	pom2, err := om.ManagePartition("group-2", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}

	// When
	pom1.CommitOffset(1009, "foo1")
	pom1.CommitOffset(1010, "foo2")
	pom2.CommitOffset(2010, "bar1")
	pom2.CommitOffset(2011, "bar2")
	pom1.CommitOffset(1017, "foo3")
	pom2.CommitOffset(2019, "bar3")
	var wg sync.WaitGroup
	spawn(&wg, pom1.Close)
	spawn(&wg, pom2.Close)

	// Then
	wg.Wait()

	committedOffset1 := lastCommittedOffset(broker1, "group-1", "topic-1", 7)
	if committedOffset1 != (FetchedOffset{1017, "foo3"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset1))
	}
	committedOffset2 := lastCommittedOffset(broker1, "group-2", "topic-1", 7)
	if committedOffset2 != (FetchedOffset{2019, "bar3"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset2))
	}
	om.Close()
}

func TestOffsetManagerCommitNetworkError(t *testing.T) {
	// Given
	broker1 := newMockBroker(t, 101)
	defer broker1.Close()

	broker1.SetHandlerByMap(map[string]MockResponse{
		"MetadataRequest": newMockMetadataResponse(t).
			SetBroker(broker1.Addr(), broker1.BrokerID()),
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1).
			SetCoordinator("group-2", broker1),
		"OffsetFetchRequest": newMockOffsetFetchResponse(t).
			SetOffset("group-1", "topic-1", 7, 1000, "foo1", ErrNoError).
			SetOffset("group-1", "topic-1", 8, 2000, "foo2", ErrNoError).
			SetOffset("group-2", "topic-1", 7, 3000, "foo3", ErrNoError),
	})

	config := NewConfig()
	config.Net.ReadTimeout = 50 * time.Millisecond
	config.Consumer.Return.Errors = true
	client, err := NewClient([]string{broker1.Addr()}, config)
	if err != nil {
		t.Fatal(err)
	}
	om, err := NewOffsetManagerFromClient(client)
	if err != nil {
		t.Fatal(err)
	}
	pom1, err := om.ManagePartition("group-1", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	pom2, err := om.ManagePartition("group-1", "topic-1", 8)
	if err != nil {
		t.Fatal(err)
	}
	pom3, err := om.ManagePartition("group-2", "topic-1", 7)
	if err != nil {
		t.Fatal(err)
	}
	pom1.CommitOffset(1001, "bar1")
	pom2.CommitOffset(2001, "bar2")
	pom3.CommitOffset(3001, "bar3")

	<-pom1.Errors()
	<-pom2.Errors()
	<-pom3.Errors()

	// When: network recovers
	broker1.SetHandlerByMap(map[string]MockResponse{
		"ConsumerMetadataRequest": newMockConsumerMetadataResponse(t).
			SetCoordinator("group-1", broker1).
			SetCoordinator("group-2", broker1),
		"OffsetCommitRequest": newMockOffsetCommitResponse(t).
			SetError("group-1", "topic-1", 7, ErrNoError).
			SetError("group-1", "topic-1", 8, ErrNoError).
			SetError("group-2", "topic-1", 7, ErrNoError),
	})
	pom1.Close()
	pom2.Close()
	pom3.Close()

	// Then: offset managers are able to commit offsets and terminate.
	committedOffset1 := lastCommittedOffset(broker1, "group-1", "topic-1", 7)
	if committedOffset1 != (FetchedOffset{1001, "bar1"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset1))
	}
	committedOffset2 := lastCommittedOffset(broker1, "group-1", "topic-1", 8)
	if committedOffset2 != (FetchedOffset{2001, "bar2"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset2))
	}
	committedOffset3 := lastCommittedOffset(broker1, "group-2", "topic-1", 7)
	if committedOffset3 != (FetchedOffset{3001, "bar3"}) {
		t.Errorf("Unexpected commit request: %v", spew.Sdump(committedOffset3))
	}
	om.Close()
}

// lastCommittedOffset traverses the mock broker history backwards searching
// for the OffsetCommitRequest coming from the specified consumer group that
// commits an offset of the specified topic/partition.
func lastCommittedOffset(mb *mockBroker, group, topic string, partition int32) FetchedOffset {
	for i := len(mb.History()) - 1; i >= 0; i-- {
		res, ok := mb.History()[i].Request.(*OffsetCommitRequest)
		if !ok || res.ConsumerGroup != group {
			continue
		}
		partitions := res.blocks[topic]
		if partitions == nil {
			continue
		}
		block := partitions[partition]
		if block == nil {
			continue
		}
		return FetchedOffset{block.offset, block.metadata}
	}
	return FetchedOffset{}
}
