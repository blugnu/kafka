package log

import (
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type offset struct {
	Name      *string `json:"name"`
	Partition *int32  `json:"partition,omitempty"`
	Offset    *int64  `json:"offset,omitempty"`
}

func (off *offset) String() string {
	t := "<nil>"
	p := "<nil>"
	o := "<nil>"

	if off.Name != nil {
		t = *off.Name
	}
	if off.Partition != nil {
		p = strconv.Itoa(int(*off.Partition))
	}
	if off.Offset != nil {
		o = strconv.Itoa(int(*off.Offset))
	}

	return "[" + t + "/" + p + ":" + o + "]"
}

func Offset(tp *kafka.TopicPartition) *offset {
	offset := &offset{
		Name: tp.Topic,
	}

	if tp.Partition != kafka.PartitionAny {
		off := int64(tp.Offset)
		offset.Partition = &tp.Partition
		offset.Offset = &off
	}

	return offset
}
