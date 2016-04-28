package consumermsg

import "fmt"

// ConsumerMessage encapsulates a Kafka message returned by the consumer.
type ConsumerMessage struct {
	Key, Value    []byte
	Topic         string
	Partition     int32
	Offset        int64
	HighWaterMark int64
}

// ConsumerError is what is provided to the user when an error occurs.
// It wraps an error and includes the topic and partition.
type ConsumerError struct {
	Topic     string
	Partition int32
	Err       error
}

func (ce ConsumerError) Error() string {
	return fmt.Sprintf("kafka: error while consuming %s/%d: %s", ce.Topic, ce.Partition, ce.Err)
}

type (
	ErrSetup          error
	ErrBufferOverflow error
	ErrRequestTimeout error
)
