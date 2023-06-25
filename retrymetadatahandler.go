package kafka

import (
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type RetryMetadataHandler interface {
	Get(*kafka.Message) *RetryMetadata
	Set(*kafka.Message, *RetryMetadata)
}

type noretrymetadata struct{}

func (*noretrymetadata) Get(*kafka.Message) *RetryMetadata  { return nil }
func (*noretrymetadata) Set(*kafka.Message, *RetryMetadata) { panic("retry metadata handler required") }

var _noretrymetadata = &noretrymetadata{}

func NoRetryMetadata() RetryMetadataHandler { return _noretrymetadata }

type retrymetadatahandler struct{}

func (*retrymetadatahandler) Get(msg *kafka.Message) *RetryMetadata {
	if msg == nil || len(msg.Headers) == 0 {
		return nil
	}

	z := RetryMetadata{}

	get := func(k string) string {
		for _, h := range msg.Headers {
			if h.Key == k {
				return string(h.Value)
			}
		}
		return ""
	}
	getInt := func(k string) int {
		s := get(k)
		i, _ := strconv.Atoi(s)
		return i
	}
	getDuration := func(k string) time.Duration {
		s := get(k)
		d, _ := time.ParseDuration(s)
		return d
	}

	md := &RetryMetadata{
		Seq:      getInt("retry-seq"),
		Num:      getInt("retry-num"),
		Max:      getInt("retry-max"),
		Interval: getDuration("retry-interval"),
		Reason:   get("retry-reason"),
	}

	if *md == z {
		return nil
	}

	return md
}

func (*retrymetadatahandler) Set(msg *kafka.Message, md *RetryMetadata) {
	if md == nil {
		return
	}

	set := func(k string, v any) {
		s := ""
		switch v := v.(type) {
		case int:
			if v != 0 {
				s = strconv.Itoa(v)
			}
		case time.Duration:
			if v != 0 {
				s = v.String()
			}
		case string:
			if v != "" {
				s = v
			}
		}
		if s != "" {
			msg.Headers = append(msg.Headers, kafka.Header{Key: k, Value: []byte(s)})
		}
	}
	set("retry-seq", md.Seq)
	set("retry-num", md.Num)
	set("retry-max", md.Max)
	set("retry-interval", md.Interval)
	set("retry-reason", md.Reason)
}
