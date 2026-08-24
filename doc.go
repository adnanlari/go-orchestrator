// Package saga provides an embeddable, production-oriented Saga
// orchestration library for Go.
//
// It supports sequential Saga orchestration with forward execution and
// reverse compensation, configurable retries, timeouts, cancellation,
// durable execution state, crash recovery, idempotency support, execution
// locking, and lifecycle hooks/events. Persistence and observability are
// pluggable: the library ships an in-memory store and simple interfaces so
// callers can supply their own backends without requiring a separate
// server, database, message broker, or workflow cluster.
//
// The library is under incremental construction. See ARCHITECTURE.md in
// the repository root for the design and the current state of the public
// API.
package saga
