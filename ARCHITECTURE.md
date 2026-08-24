# Architecture

`go-orchestrator` is an embeddable Saga orchestration library. It runs
in-process, inside the calling application, and needs no external server,
database, message broker, or workflow cluster to function. Persistence and
observability are pluggable via small interfaces so a caller can start with
the in-memory store and later swap in a durable one without changing how
sagas are defined or executed.

## Conceptual architecture

```
Saga Definition
      |
      v
Saga Engine
      |
      +-----------+-----------+----------------+
      |           |           |                |
      v           v           v                v
    Store       Retry   Observability      Recovery
```

The engine is the only component that talks to the other four. Definitions
never touch the store directly, the store never knows about retry policy,
and so on — each concern is isolated behind its own interface so it can be
replaced independently.

## Core concepts

1. **Saga Definition** — an ordered, immutable list of steps identified by
   a saga name. Built once via a fluent API, then executed any number of
   times.
2. **Saga Execution** — one run of a Saga Definition against a given input.
   Has its own identity, status, and history, independent of the
   definition it was created from.
3. **Saga Step** — a named pair of functions: a forward action and a
   compensating action. The compensating action undoes the forward action
   if a later step in the same execution fails.
4. **State Machine** — the authoritative set of valid status transitions
   for an execution and its steps. All status mutation is centralized here
   so the rest of the engine cannot produce an invalid state.
5. **Store** — the persistence boundary. The engine reads and writes
   execution/step state exclusively through this interface. `MemoryStore`
   is the only implementation in this library; external stores (Postgres,
   Redis, ...) are expected to implement the same interface out-of-tree or
   in a future subpackage.
6. **Retry Policy** — decides whether a failed step should be retried and
   how long to wait before the next attempt.
7. **Recovery Manager** — after a process restart, inspects persisted
   executions that were left in a non-terminal state and resumes or
   compensates them deterministically.
8. **Execution Lock** — an ownership/lease mechanism ensuring a single
   execution is not driven forward by two workers concurrently.
9. **Hooks** — synchronous callbacks invoked at lifecycle points
   (step started, saga failed, compensation completed, ...).
10. **Events** — structured records of the same lifecycle points, published
    through a pluggable `EventPublisher` for external consumption
    (logging, metrics, audit trails).

## Delivery semantics

This library provides **at-least-once execution with idempotency support**.
It does **not** provide exactly-once distributed execution, and never will
as a core guarantee — a process can crash after a step's external effect
has succeeded but before that success is durably recorded, and on recovery
the step may be invoked again. Every step is expected to have a stable
operation/idempotency identity (see the Idempotency phase) so that
downstream systems can safely deduplicate a repeated invocation. This
constraint and its implications are documented in the README once the
relevant behavior exists.

## Configuration model

Configuration is supplied via functional options at three levels, plus a
future per-call override, resolved with the following precedence
(highest first):

```
execution override
    >
step configuration
    >
saga configuration
    >
library defaults
```

Not all levels are implemented yet; each is introduced in the phase that
needs it rather than all at once.

## Package layout

```
go-orchestrator/          module root, package `saga` — public API:
                           saga/step/execution definitions, engine entry
                           points, functional options, pluggable-interface
                           types (Store, RetryPolicy, Logger, Metrics,
                           Tracer, Hooks, EventPublisher).
  internal/                implementation details not part of the public
                           API (e.g. the state machine, ID generation).
                           Populated incrementally as phases need it.
```

Concrete pluggable implementations (e.g. an in-memory store) are expected
to live in their own subpackages as they are introduced, so that adding a
Postgres or Redis store later is an additive package, not a change to the
core API. No such subpackages exist yet.

## What is explicitly out of scope for v1

DAG/parallel workflows, distributed workers, Postgres/Redis stores, and an
optional SaaS control plane are considered in the design (interfaces are
kept small and composable so they can be added later) but are not
implemented in v1.

## Status

This library is being built incrementally, one phase at a time. See the
project's phase roadmap (tracked outside this file) for what is currently
implemented. As of this writing, only the repository scaffold exists — no
saga engine, execution, or persistence behavior has been implemented yet.
