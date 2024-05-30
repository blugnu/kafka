package mock

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type expectheaders struct {
	value map[string]string
}

func (e *expectheaders) Equal(v []kafka.Header) bool {
	if e == nil {
		return true
	}

	mv := make(map[string]string)
	for _, h := range v {
		mv[h.Key] = string(h.Value)
	}
	return maps.Equal(e.value, mv)
}

func (e *expectheaders) String() string {
	if e == nil {
		return ""
	}

	keys := []string{}
	for k := range e.value {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	s := "=["
	for _, k := range keys {
		s += fmt.Sprintf(fmtheaderkv, k, e.value[k])
	}
	s = strings.TrimRight(s, ", ")
	s += "]"

	return s
}
