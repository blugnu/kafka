package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type OffsetInfo kafka.TopicPartition

func (tp OffsetInfo) String() string {
	return fmt.Sprintf("%s/%d:%d", *tp.Topic, tp.Partition, tp.Offset)
}
