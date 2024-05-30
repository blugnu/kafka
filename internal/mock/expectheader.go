package mock

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type expectheader struct {
	entries []kafka.Header
}

func (e *expectheader) In(v []kafka.Header) bool {
	if e == nil {
		return true
	}

	found := 0
	for _, h := range e.entries {
		for _, hv := range v {
			if h.Key == hv.Key && bytes.Equal(h.Value, hv.Value) {
				found++
				break
			}
		}
	}
	return found == len(e.entries)
}

func (e *expectheader) String() string {
	if e == nil {
		return ""
	}

	s := " contain:["
	for _, h := range e.entries {
		s += fmt.Sprintf(fmtheaderkv, h.Key, string(h.Value))
	}
	s = strings.TrimRight(s, ", ")
	s += "]"

	return s
}
