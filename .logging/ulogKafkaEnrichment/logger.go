package ulogKafkaEnrichment

import (
	"fmt"
	"strings"

	"github.com/blugnu/kafka"
	"github.com/blugnu/kafka/context"
	"github.com/blugnu/kafka/internal/log"
	"github.com/blugnu/ulog"
)

func enrichLog(ctx context.Context) map[string]any {
	if field := log.Field(ctx); field != nil {
		return map[string]any{"kafka": *field}
	}
	return nil
}

func logfn(level ulog.Level) func(context.Context, ...any) {
	return func(ctx context.Context, a ...any) {
		s := []string{}
		for _, v := range a {
			s = append(s, fmt.Sprintf("%v ", v))
		}
		ulog.FromContext(ctx).
			Log(level, strings.Join(s, " "))
	}
}

func Enabled() {
	ulog.RegisterEnrichment(enrichLog)
	kafka.EnableDebugLogs(logfn(ulog.DebugLevel))
	kafka.EnableTraceLogs(logfn(ulog.TraceLevel))
}
