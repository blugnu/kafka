package kafka

type OffsetReset string

const (
	OffsetResetEarliest = OffsetReset("earliest")
	OffsetResetLatest   = OffsetReset("latest")
	OffsetResetNone     = OffsetReset("none")
)
