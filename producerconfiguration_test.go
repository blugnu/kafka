package kafka

import (
	"reflect"
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func TestProducerConfiguration(t *testing.T) {
	// ARRANGE
	type result struct {
		*Config
		*producer
		error
	}

	testcases := []struct {
		name string
		sut  ProducerConfiguration
		result
	}{
		{name: "encryption", sut: Encryption(&encryptionhandler{}), result: result{
			Config: &Config{ConfigMap: kafka.ConfigMap{}},
			producer: &producer{
				EncryptionHandler: &encryptionhandler{},
			},
		}},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// ARRANGE
			cfg := &Config{ConfigMap: kafka.ConfigMap{}}
			prd := &producer{}

			// ACT
			err := tc.sut(cfg, prd)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// ASSERT
			wanted := tc.result
			got := result{Config: cfg, producer: prd}
			if !reflect.DeepEqual(wanted, got) {
				t.Errorf("\nwanted %#v\ngot    %#v", wanted, got)
			}
		})
	}
}
