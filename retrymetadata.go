package kafka

import (
	"context"
	"fmt"
	"time"
)

type RetryMetadata struct {
	Seq      int           // the retry sequence of the event.  the initial sequence is 1 and is only incremented if the retry limit is reached and a new sequence initiated (by some mechanism, not provided by this package)
	Max      int           // the maximum number of retries allowed in the current sequence (-1 for unlimited)
	Num      int           // the number of retries attempted in the current sequence. this is incremented for each new retry message in a sequence
	Interval time.Duration // the required processing delay for the retry message
	Reason   string        // the error (as string) that caused the retry
}

func (retry *RetryMetadata) String() string {
	if retry.Max == -1 {
		return fmt.Sprintf("seq=%d num=%d interval=%s", retry.Seq, retry.Num, retry.Interval)
	}
	return fmt.Sprintf("seq=%d num=%d max=%d interval=%s", retry.Seq, retry.Num, retry.Max, retry.Interval)
}

func RetryMetadataFromContext(ctx context.Context) *RetryMetadata {
	if ctx == nil {
		return nil
	}
	if md, ok := ctx.Value(kRetryMetadata).(*RetryMetadata); ok {
		return md
	}
	return nil
}
