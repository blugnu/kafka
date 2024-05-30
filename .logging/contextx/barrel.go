package context

import "context"

type Context = context.Context

var (
	Background   = context.Background
	WithDeadline = context.WithDeadline
	WithTimeout  = context.WithTimeout
	WithValue    = context.WithValue
)
