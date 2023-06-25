package kafka

import (
	"encoding/json"

	"github.com/blugnu/kafka/context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func UnmarshalJson[T any](ctx context.Context, msg *kafka.Message) (*T, error) {
	result := new(T)
	if err := json.Unmarshal(msg.Value, result); err != nil {
		return nil, err
	}
	return result, nil
}
