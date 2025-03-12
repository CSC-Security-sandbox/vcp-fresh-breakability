package common

type ContextKey int

const (
	CorrelationContextKey ContextKey = iota
	CallerInfoContextKey  ContextKey = iota
	CorrelationIDName     string     = "x-correlation-id"
)
