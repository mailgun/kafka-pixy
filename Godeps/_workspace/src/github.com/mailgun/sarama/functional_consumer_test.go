package sarama

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestFuncConsumerOffsetOutOfRange(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	consumer, err := NewConsumer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := consumer.ConsumePartition("test.1", 0, -10); err != ErrOffsetOutOfRange {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	if _, _, err := consumer.ConsumePartition("test.1", 0, math.MaxInt64); err != ErrOffsetOutOfRange {
		t.Error("Expected ErrOffsetOutOfRange, got:", err)
	}

	safeClose(t, consumer)
}

func TestConsumerHighWaterMarkOffset(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	p, err := NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, p)

	_, offset, err := p.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("Test")})
	if err != nil {
		t.Fatal(err)
	}

	c, err := NewConsumer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, c)

	pc, _, err := c.ConsumePartition("test.1", 0, OffsetOldest)
	if err != nil {
		t.Fatal(err)
	}

	<-pc.Messages()

	if hwmo := pc.HighWaterMarkOffset(); hwmo != offset+1 {
		t.Logf("Last produced offset %d; high water mark should be one higher but found %d.", offset, hwmo)
	}

	safeClose(t, pc)
}

// BrokerConsumer used to be implemented so that if the message channel of one
// of the associated PartitionConsumers got full, that prevented ALL the rest
// PartitionConsumers from receiving messages from the BrokerConsumer.
//
// This test makes sure that if the queue of one PartitionConsumer gets full,
// the other keep getting new messages.
//
// IMPORTANT: The topic/key of the two sets of the generated messages had been
// selected so that both sets end up in partitions that has the same leader.
func TestConsumerPartitionConsumerSlacker(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	// {topic: "test.1", key: "foo"} and {topic: "test.4": key: "bar"} have
	// the same broker #9093 as a leader.
	startOffsetA := generateTestMessages(t, "test.1", StringEncoder("foo"), 11)
	startOffsetB := generateTestMessages(t, "test.4", StringEncoder("bar"), 1000)

	config := NewConfig()
	// The channel buffer size is selected to be one short of the number of
	// messages in the `test.1` topic. So that an attempt to write the
	// (buffer size + 1)th message to the respective PartitionConsumer message
	// channel will block, given that nobody is reading from the channel.
	config.ChannelBufferSize = 10
	c, err := NewConsumer(kafkaBrokers, config)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, c)

	pcA, _, err := c.ConsumePartition("test.1", 0, startOffsetA)
	if err != nil {
		t.Fatal(err)
	}

	pcB, _, err := c.ConsumePartition("test.4", 2, startOffsetB)
	if err != nil {
		t.Fatal(err)
	}

	timeoutCh := time.After(1 * time.Second)
	for i := 0; i < 1000; i++ {
		select {
		case <-pcB.Messages():
			break
		case <-timeoutCh:
			// Both queues should be drained in parallel, otherwise the old
			// BrokerConsumer implementation would get into a deadlock here.
			go pcA.Close()
			messagesA := pcA.Messages()
			go pcB.Close()
			messagesB := pcB.Messages()
		drainLoop:
			for messagesA != nil || messagesB != nil {
				select {
				case _, ok := <-messagesA:
					if !ok {
						messagesA = nil
					}
				case _, ok := <-messagesB:
					if !ok {
						messagesB = nil
					}
				default:
					break drainLoop
				}
			}
			t.Fatalf("Failed to read all messages from pcB within 1 seconds: read=%v", i)
		}
	}
	safeClose(t, pcA)
	safeClose(t, pcB)
}

func generateTestMessages(t *testing.T, topic string, key Encoder, count int) int64 {
	p, err := NewSyncProducer(kafkaBrokers, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer safeClose(t, p)

	startOffset := int64(-42)
	for i := 0; i < count; i++ {
		_, offset, err := p.SendMessage(
			&ProducerMessage{
				Topic: topic,
				Key:   key,
				Value: StringEncoder(fmt.Sprintf("Test%v", i))})
		if err != nil {
			t.Fatal(err)
		}

		if startOffset == int64(-42) {
			startOffset = offset
		}
	}
	return startOffset
}
