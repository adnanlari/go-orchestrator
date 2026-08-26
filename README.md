# go-orchestrator

An embeddable, production-oriented Saga orchestration library for Go.

Runs in-process — no separate server, database, message broker, or
workflow cluster required for core functionality. Persistence,
retries, timeouts, locking, hooks, and observability are all pluggable
and entirely optional: a saga defined with none of them configured
behaves the same as one with all of them.

## Install

```bash
go get github.com/adnanlari/go-orchestrator
```

## Quick start

```go
workflow := saga.New("order_creation")
workflow.AddStep(saga.Step("reserve_inventory", reserveInventory, releaseInventory))
workflow.AddStep(saga.Step("charge_payment", chargePayment, refundPayment))

exec, err := workflow.Execute(context.Background(), order)
if err != nil {
    // exec.Status tells you exactly how it ended: StatusCompensated
    // means it failed but rolled back cleanly; StatusCompensationFailed
    // means rollback itself also failed and needs attention.
    log.Printf("order %s: %s (%v)", exec.ID, exec.Status, err)
}
```

Each step is a forward `Action` paired with a `Compensate` that undoes
it. If a later step fails, every already-succeeded step before it is
compensated in reverse order — the failed step itself is never
compensated, since its `Action` never actually completed.

See [`examples/basic`](examples/basic) for the full runnable version of
the above, [`examples/payment`](examples/payment) for a realistic
saga using persistence, retries, idempotency, and observability
together, and [`examples/recovery`](examples/recovery) for crash
recovery.

## Architecture

The engine is the only component that talks to a saga's persistence,
retry policy, and observability hooks — a `Definition` never touches
the `Store` directly, and the `Store` never knows about the
`RetryPolicy`. Each concern is an independently pluggable interface.
See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design, including
the state machine, package layout, and why a couple of pieces (the
state machine, `OperationID`) ended up shaped the way they did.

## Compensation

If a step fails, every step before it that already succeeded is
compensated in reverse order:

```
A succeeds, B succeeds, C fails
  -> compensate(B)
  -> compensate(A)
  (C is never compensated: it never succeeded)
```

A step with a `nil` `Compensate` is treated as having nothing to undo
(e.g. a step that only sends a notification) and is skipped during
compensation without needing a no-op function. If compensating one step
fails, the engine keeps going and compensates the rest anyway — as much
of the saga as possible is rolled back rather than stopping at the first
compensation error. The saga ends in `StatusCompensated` if every
compensation succeeded, or `StatusCompensationFailed` if one or more
didn't (which usually needs a human to look at it).

## Retry

Configure retries at the saga level (a default for every step) or
override per step:

```go
saga.New("order_creation",
    saga.WithRetryPolicy(saga.ExponentialBackoff(1*time.Second, 30*time.Second, 5)),
)

saga.Step("charge_payment", chargePayment, refundPayment,
    saga.WithStepRetryPolicy(saga.NoRetry()), // this one shouldn't be retried
)
```

Built-in policies: `NoRetry` (the default if nothing is configured —
one attempt, no retries), `FixedDelay`, `ExponentialBackoff` (capped at
a max delay), and `WithJitter`, which wraps any policy to randomize its
delay and avoid many callers retrying in lockstep. A step's `Action` can
opt a specific failure out of retries regardless of policy by returning
`saga.NonRetryable(err)` — for errors a retry could never fix, like a
validation failure.

## Timeouts and cancellation

```go
saga.New("order_creation", saga.WithTimeout(2*time.Minute)) // bounds the whole run

saga.Step("charge_payment", chargePayment, refundPayment,
    saga.WithStepTimeout(5*time.Second), // bounds one attempt
)
```

Both are built on `context.Context` — specifically `context.WithTimeoutCause`
and `context.Cause`, so a saga timeout, a step timeout, and an ordinary
external `ctx` cancellation are all distinguishable (`errors.As` for
`*SagaTimeoutError`/`*StepTimeoutError`, or `errors.Is(err,
context.DeadlineExceeded)`/`context.Canceled` for the general case) even
though they all share the same underlying cancellation mechanism. Go's
context cancellation is cooperative, not forceful: if a step's `Action`
ignores `ctx` and keeps running past its deadline, `Execute` cannot
abandon it mid-call — it will still return once `Action` does, but
discards whatever `Action` returns in favor of the timeout error, since a
result arriving after the deadline can't be trusted as timely.

## Persistence

```go
store := memory.New() // or your own Store implementation
workflow := saga.New("order_creation", saga.WithStore(store))
```

`Store` is a two-method interface (`Save`, `Get`); `Execute` persists
the execution after every status change, and treats a `Save` failure as
fatal to that call — a broken store is never silently ignored, since
that would defeat the point of persisting at all. `store/memory` is the
implementation this library ships, requiring no external service.

**`store/memory` does not survive a process restart.** Its data lives in
the same process's memory as the saga engine — if that process crashes,
the store's data is gone with it. It's genuinely useful for development,
tests, and demonstrating the mechanism (as `examples/recovery` does),
but it makes crash recovery meaningless in practice: after a real crash,
a fresh process gets a fresh, empty `memory.Store`, with nothing left to
recover from. For recovery to mean anything, back `Store` with something
that outlives the process — Postgres, Redis, a file on disk. Doing so
requires no changes to `Execute`, `RecoveryManager`, or any saga
definition: implement `Save`/`Get` (and, for recovery, `ListIncomplete`)
against your backend and pass it to `WithStore`.

## Recovery

```go
rm := saga.NewRecoveryManager(store, saga.WithSaga(workflow))
results, err := rm.Recover(ctx) // call at startup, before serving new requests
```

A `RecoveryManager` finds every persisted execution left in a
non-terminal status (via the `Lister` interface — `ListIncomplete`) and
resumes each one from exactly where it left off, using the same engine
`Execute` itself uses: a step already succeeded is skipped, a step
already compensated is never compensated again, and a step that was
`Running` or `Compensating` when the prior process exited is invoked
again. That last case is why idempotency (below) matters: recovery
cannot know whether an in-flight step's real-world effect already
happened.

If you're not confident every step is idempotent yet,
`WithRecoveryPolicy(saga.RecoverSkipInFlight)` resumes only executions
that were cleanly between steps and leaves anything with an in-flight
step untouched (reported as `ErrRecoverySkipped`) rather than guessing.

If the configured `Store` also implements `Locker`, `RecoveryManager`
(and `Execute`) take an exclusive lease on an execution before touching
it, so two workers — e.g. two processes both running recovery against
the same store — can't drive the same execution concurrently. See
Execution Locking below for how that lease actually works.

## Execution locking

```go
saga.New("order_creation",
    saga.WithStore(store),           // must also implement Locker
    saga.WithLockTTL(10*time.Second), // default: saga.DefaultLockTTL (30s)
)
```

If the `Store` you pass to `WithStore` also implements `Locker`
(`store/memory` does), `Execute` and `RecoveryManager` automatically
take an exclusive lease on an execution before doing anything else, and
hold it until they return. You don't call `Acquire`/`Release`
yourself — it's entirely automatic. If a lease is already held by
another worker, the call returns an `*ExecutionLockedError` immediately
rather than risking two workers driving the same execution at once.
`RecoveryManager.Recover` treats this as an expected, non-fatal outcome
for that one execution (recorded in its `RecoveryResult.Err`) and keeps
processing the rest.

This matters most for recovery — two processes each running a
`RecoveryManager` against the same store are exactly the scenario where
two workers could otherwise pick up the same incomplete execution. A
single process's own `Execute` calls, by contrast, always use a
freshly-generated, unique execution ID, so lock contention there isn't
really possible; it goes through the same lease mechanism anyway for one
consistent code path.

It's a lease with a TTL, not a plain lock: if the worker holding it
crashes without releasing it, the lease simply expires and another
worker can take over — a permanent, non-expiring lock would leave that
execution stuck forever, which is exactly the crash scenario recovery
exists to survive. A held lease renews itself automatically every time
the engine persists a state change, so a genuinely long-running saga
won't lose its lease out from under it mid-run; a single step whose
`Action` runs longer than the configured TTL with no intermediate
persisted progress is the one case that could still let the lease
expire early.

## Hooks and events

```go
type slackAlert struct{}

func (slackAlert) Publish(ctx context.Context, ev saga.Event) {
    if ev.Type == saga.EventSagaFailed {
        notifySlack(fmt.Sprintf("saga %s failed: %s", ev.ExecutionID, ev.Error))
    }
}

saga.New("order_creation", saga.WithEventPublisher(slackAlert{}))
```

`Execute` and `RecoveryManager` fire an `Event` at nine lifecycle
points — `EventSagaStarted`, `EventSagaCompleted`, `EventSagaFailed`,
`EventStepStarted`, `EventStepCompleted`, `EventStepFailed`,
`EventCompensationStarted`, `EventCompensationCompleted`, and
`EventCompensationFailed` — each carrying the execution ID, saga name,
step name (empty for saga-level events), and error message (for the
failure events). `EventSagaFailed` fires for every outcome where
`Execute` returns a non-nil error — `StatusFailed`, `StatusCompensated`,
and `StatusCompensationFailed` alike — since all three mean the saga's
actual goal wasn't achieved, even if cleanup went fine.

Implement `EventPublisher` (one method: `Publish(ctx, Event)`) to
receive them; `WithEventPublisher` wires it in, and `MultiPublisher`
composes more than one destination into the single publisher the option
accepts. A publisher can never affect the saga it's observing — a
panicking `Publish` call is recovered and discarded — and `Publish` has
no return value for the same reason: there's no way for a hook to fail
the saga.

## Observability

```go
saga.New("order_creation",
    saga.WithLogger(slog.New(slog.NewJSONHandler(os.Stdout, nil))),
    saga.WithMetrics(myPrometheusAdapter),
    saga.WithTracer(myOTelAdapter),
)
```

Three independent, optional interfaces, none of which require a
specific backend as a dependency:

- **`WithLogger`** takes a `*log/slog.Logger` directly — standard
  library, not a custom interface — so any backend with an `slog`
  adapter (most do) works without this library depending on it. A
  structured log line is emitted at the same nine lifecycle points as
  events above (`EventSagaFailed`/`EventStepFailed`/`EventCompensationFailed`
  log at Error, everything else at Info).
- **`WithMetrics`** takes a `Metrics` (two methods: `IncCounter`,
  `ObserveDuration`) — enough to wrap a Prometheus client, an
  OpenTelemetry meter, or anything else with two small adapter methods.
  Increments a `saga_events_total` counter per lifecycle event and
  records a `saga_duration_seconds` observation once the saga reaches a
  terminal status (measured from when it started, so it includes any
  time spent stopped between a crash and recovery, not just active
  processing time).
- **`WithTracer`** takes a `Tracer` (one method: `StartSpan`), shaped
  closely enough after OpenTelemetry's own `Tracer.Start` that wrapping
  one is a few lines. One span wraps the whole saga run
  (`"saga:<name>"`), and one child span wraps each step attempt
  (`"step:<name>"`, or `"step:<name>:compensate"` for a compensation
  attempt).

None of Logger, Metrics, or Tracer can affect the saga's outcome any
more than an `EventPublisher` can — they're purely observational.

## Idempotency

```go
func chargePayment(ctx context.Context, data any) (any, error) {
    idempotencyKey := saga.OperationID(ctx)
    return gateway.ChargeWithIdempotencyKey(idempotencyKey, amount)
}
```

`OperationID(ctx)` returns a stable key for the step invocation `ctx`
belongs to — the same value on the first attempt, every retry, and any
re-invocation by `RecoveryManager` after a crash; distinct between a
step's `Action` and its `Compensate`, between different steps, and
between different executions. Pass it to whatever deduplication
mechanism a downstream dependency offers (most payment gateways and many
messaging APIs support exactly this). This is what makes retrying or
recovering a step actually safe — see Guarantees below for why the
engine can't make that guarantee on its own.

## Guarantees

- **At-least-once execution, not exactly-once.** A step's `Action` (or
  `Compensate`) can be invoked more than once for what is logically the
  same operation — after a transient failure triggers a retry, or after
  a crash leaves a step's outcome unknown and `RecoveryManager` resumes
  it. The library will never silently invoke a step *fewer* times than
  necessary, but it cannot guarantee exactly once on its own.
- **Idempotency is what closes that gap**, not the engine. Use
  `OperationID(ctx)` with a downstream dependency that supports
  deduplication, and a duplicate invocation becomes safe.
- **A `Store` failure is always fatal to the call it happens in.**
  `Execute` never continues as if a persistence failure didn't happen.
- **A failed step is never compensated.** Only steps whose `Action`
  actually completed successfully are rolled back.
- **Hooks and observability can never affect a saga's outcome.** A
  panicking `EventPublisher` is recovered and ignored; there's no way
  for `Publish`, `Metrics`, `Tracer`, or `Logger` calls to change what
  happens to the saga they're observing.

## Limitations

- **Sequential steps only.** There is no DAG or parallel-step execution
  in this library — steps run one after another, in definition order.
- **Single-process orchestration**, not a distributed workflow cluster.
  Multiple processes can safely share one `Store` (and, with `Locker`,
  coordinate on which execution each is driving), but there's no
  built-in work distribution across workers.
- **`store/memory` does not survive a restart** — see Persistence above.
- **A saga-level timeout restarts its full budget on every resume.**
  Elapsed wall-clock time before a crash isn't tracked or subtracted
  when `RecoveryManager` picks an execution back up.
- **Compensation isn't retried.** If a `Compensate` call fails, it fails
  once; there's no retry policy for compensating actions the way there
  is for forward actions.
- **No built-in SaaS control plane, DAG support, or distributed worker
  pool.** These are explicitly out of scope for this library — the
  interfaces (`Store`, `RetryPolicy`, `Locker`, ...) are kept small and
  composable so they could support such things being built on top, but
  none of it is implemented here.

## Roadmap

Implemented: saga definition, sequential execution, compensation, a
centralized state machine, in-memory persistence, configurable retries,
timeouts and cancellation, crash recovery, idempotency, execution
locking, lifecycle hooks/events, and pluggable observability (structured
logging via `log/slog`, metrics, tracing).

Not yet implemented: a hardening pass (fuzzing, benchmarks, static
analysis, a full concurrency and error-path review). Beyond that,
DAG/parallel workflows, Postgres/Redis store implementations, a
distributed worker pool, and an optional SaaS control plane are
explicitly not planned for this library, though the architecture leaves
room for them to be built on top.

## License

MIT — see [LICENSE](LICENSE).
