package kafka

import (
	"github.com/blugnu/kafka/context"
	"github.com/blugnu/unilog"

	"github.com/blugnu/kafka/internal/log"
)

var Logger = unilog.Nul()

func init() {
	unilog.RegisterEnrichment(enrichLog)
}

func enrichLog(ctx context.Context, entry unilog.Enricher) unilog.Entry {
	if field := log.Field(ctx); field != nil {
		return entry.WithField("kafka", field)
	}
	return entry.(unilog.Entry)
}
