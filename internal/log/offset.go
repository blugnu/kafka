package log

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type offset struct {
	Topic     *string `json:"topic"`
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
}

func (off *offset) String() string {
	t := "<nil>"
	p := "<nil>"
	o := "<nil>"

	if off.Topic != nil {
		t = *off.Topic
	}
	if off.Partition != nil {
		p = fmt.Sprintf("%d", *off.Partition)
	}
	if off.Offset != nil {
		o = fmt.Sprintf("%d", *off.Offset)
	}

	return fmt.Sprintf("[%s:%s:%s]", t, p, o)
}

func Offset(tp *kafka.TopicPartition) *offset {
	offset := &offset{
		Topic: tp.Topic,
	}

	if tp.Partition != kafka.PartitionAny {
		off := int64(tp.Offset)
		offset.Partition = &tp.Partition
		offset.Offset = &off
	}

	return offset
}
