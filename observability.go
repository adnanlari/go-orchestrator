package saga

import "context"

// Metrics is a minimal interface for reporting saga lifecycle metrics.
// The core library defines only the narrow shape it needs; adapting it
// to Prometheus, OpenTelemetry, or any other metrics system is the
// implementer's job, kept entirely out of this package so as not to
// require any of them as a dependency.
//
// labels are passed as alternating key/value pairs (e.g. "saga",
// "order_creation", "status", "completed"), the same convention used by
// most structured-metrics client libraries. An odd number of labels is
// the caller's bug, not something Metrics implementations need to guard
// against beyond not panicking.
type Metrics interface {
	// IncCounter increments the named counter by 1.
	IncCounter(name string, labels ...string)
	// ObserveDuration records d against the named histogram or summary.
	ObserveDuration(name string, seconds float64, labels ...string)
}

// Tracer is a minimal interface for reporting saga lifecycle spans. Like
// Metrics, this is a narrow interface the core library defines and
// requires nothing beyond; adapting it to OpenTelemetry or any other
// tracing system is the implementer's job. Its shape mirrors
// OpenTelemetry's own Tracer.Start closely enough that wrapping one is a
// few lines, without requiring the OpenTelemetry SDK as a dependency.
type Tracer interface {
	// StartSpan starts a new span named name, as a child of whatever
	// span ctx already carries (if any), and returns a context carrying
	// the new span together with a function to call exactly once when
	// the span ends.
	StartSpan(ctx context.Context, name string) (context.Context, func())
}

// noopSpanEnd is returned by startSpan when no Tracer is configured, so
// callers can unconditionally defer the result without a nil check.
func noopSpanEnd() {}
