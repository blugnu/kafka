package kafka

import (
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"context"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type LogInfo struct {
	Consumer  *string `json:"consumer,omitempty"`
	Error     error   `json:"error,omitempty"`
	*Message  `json:"message,omitempty"`
	Offset    *kafka.TopicPartition `json:"offset,omitempty"`
	Reason    *string               `json:"reason,omitempty"`
	Recovered *any                  `json:"recovered,omitempty"`
	Topic     *string               `json:"topic,omitempty"`
	Topics    *[]string             `json:"topics,omitempty"`
}

func (LogInfo) messageString(m Message) string {
	e := []string{fmt.Sprintf("offset=%s", OffsetInfo(m.TopicPartition))}

	if len(m.Key) > 0 {
		e = append(e, fmt.Sprintf("key=[%s]", m.Key))
	}

	if len(m.Headers) > 0 {
		h := []string{}
		for _, header := range m.Headers {
			h = append(h, fmt.Sprintf("%s:[%s]", header.Key, header.Value))
		}
		e = append(e, fmt.Sprintf("headers={%s}", strings.Join(h, " ")))
	}

	if !m.Timestamp.IsZero() {
		e = append(e, "timestamp="+m.Timestamp.Format(time.RFC3339))
	}

	return "{" + strings.Join(e, " ") + "}"
}

func (l LogInfo) String() string {
	e := []string{}

	add := func(name string, value any, fn func(any) string) {
		if value == nil || reflect.ValueOf(value).IsZero() {
			return
		}
		refv := reflect.ValueOf(value).Elem()
		if !refv.IsZero() {
			value = refv.Interface()
		}
		e = append(e, fmt.Sprintf("%s=%s", name, fn(value)))
	}

	add("consumer", l.Consumer, func(a any) string { return a.(string) })
	add("offset", l.Offset, func(a any) string { return OffsetInfo(a.(kafka.TopicPartition)).String() })
	add("message", l.Message, func(a any) string { return l.messageString(a.(kafka.Message)) })
	if l.Error != nil {
		e = append(e, fmt.Sprintf("error=%q", l.Error))
	}
	add("reason", l.Reason, func(a any) string { return a.(string) })
	add("recovered", l.Recovered, func(a any) string { return fmt.Sprintf("%q", a) })
	add("topic", l.Topic, func(a any) string { return a.(string) })
	if l.Topics != nil {
		e = append(e, fmt.Sprintf("topics=%v", *l.Topics))
	}
	return strings.Join(e, " ")
}

type Loggers struct {
	Debug func(context.Context, string, LogInfo)
	Info  func(context.Context, string, LogInfo)
	Error func(context.Context, string, LogInfo)
}

func (l *Loggers) useDefault() bool {
	return l == nil ||
		(l.Debug == nil && l.Info == nil && l.Error == nil)
}

func noLog(context.Context, string, LogInfo) { /* NO-OP */ }

var logs = Loggers{
	Debug: noLog,
	Info:  noLog,
	Error: noLog,
}

func EnableLogs(fns *Loggers) {
	type logfn func(context.Context, string, LogInfo)

	ifNotNil := func(fn logfn) logfn {
		if fn == nil {
			return noLog
		}
		return fn
	}
	makeFunc := func(name string) logfn {
		return func(ctx context.Context, s string, i LogInfo) {
			log.Println(name, s, i)
		}
	}

	if fns.useDefault() {
		logs.Debug = makeFunc("KAFKA:DEBUG")
		logs.Info = makeFunc("KAFKA:INFO ")
		logs.Error = makeFunc("KAFKA:ERROR")
		return
	}

	logs.Debug = ifNotNil(fns.Debug)
	logs.Info = ifNotNil(fns.Info)
	logs.Error = ifNotNil(fns.Error)
}
