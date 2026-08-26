package saga

import "context"

// contextKey is an unexported type for this package's context keys, per
// the standard library's own guidance for avoiding collisions with keys
// defined by other packages.
type contextKey int

const operationIDContextKey contextKey = 0

// OperationID returns the stable idempotency key for the step
// invocation ctx belongs to: the same value on every attempt of a given
// step within a given execution, whether the first attempt, a retry
// (WithRetryPolicy / WithStepRetryPolicy), or a re-invocation after
// crash recovery (RecoveryManager). It differs between a step's Action
// and its Compensate, between different steps, and between different
// executions.
//
// Pass it straight through to whatever idempotency mechanism a
// downstream dependency offers — for example, as an Idempotency-Key
// request header, or as the deduplication key argument to an API that
// supports one (this is how many payment gateways and messaging APIs
// work). This is what makes retrying or recovering a step safe despite
// the library's at-least-once (not exactly-once) execution guarantee:
// the downstream system, not this library, is what actually prevents
// the same real-world effect from happening twice. See the package
// documentation on delivery semantics.
//
// OperationID returns "" if ctx was not provided by the engine — for
// example, if called outside of an ActionFunc or CompensateFunc.
func OperationID(ctx context.Context) string {
	id, _ := ctx.Value(operationIDContextKey).(string)
	return id
}

// withOperationID returns a copy of ctx carrying opID, retrievable later
// via OperationID.
func withOperationID(ctx context.Context, opID string) context.Context {
	return context.WithValue(ctx, operationIDContextKey, opID)
}

// operationID computes the stable idempotency key for one step's Action
// within one execution. The format (execution ID, a separator, and the
// step name) is not a documented contract for callers to parse — treat
// the result as an opaque, stable string — but it is deliberately
// human-readable so it reads sensibly in logs.
func operationID(executionID, stepName string) string {
	return executionID + "/" + stepName
}

// compensationOperationID computes the stable idempotency key for one
// step's Compensate within one execution. It is deliberately distinct
// from operationID for the same step: Action and Compensate are
// different real-world operations, often against different downstream
// endpoints, and must never be mistaken for the same idempotent request.
func compensationOperationID(executionID, stepName string) string {
	return executionID + "/" + stepName + "/compensate"
}
