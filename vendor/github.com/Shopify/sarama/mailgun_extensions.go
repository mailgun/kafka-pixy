package sarama

import (
	"github.com/pkg/errors"
)

func (r *OffsetCommitRequest) Offset(topic string, partitionID int32) (int64, string, error) {
	partitions := r.blocks[topic]
	if partitions == nil {
		return 0, "", errors.New("No such offset")
	}
	block := partitions[partitionID]
	if block == nil {
		return 0, "", errors.New("No such offset")
	}
	return block.offset, block.metadata, nil
}


func (b *FetchResponseBlock) RecordBatch() *RecordBatch { return b.Records.recordBatch }
func (b *FetchResponseBlock) MessageSet() *MessageSet { return b.Records.msgSet }
