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


func (r *Records) RecordBatch() *RecordBatch { return r.recordBatch }
func (r *Records) MessageSet() *MessageSet { return r.msgSet }
