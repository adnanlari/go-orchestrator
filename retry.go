package saga

import (
	"math/rand"
	"time"
)

// RetryPolicy decides how many times a step's Action may be attempted
// and how long to wait between attempts. Configure one with
// WithRetryPolicy (saga-level, the default for every step) or
// WithStepRetryPolicy (overrides it for one step). If neither is
// configured, NoRetry is used: a step gets exactly one attempt, matching
// the engine's behavior before RetryPolicy existed.
//
// A step's Action can opt an individual failure out of retries
// altogether, regardless of policy, by returning NonRetryable(err).
type RetryPolicy interface {
	// MaxAttempts returns the maximum number of times Action may be
	// invoked in total for one step, including the first attempt. A
	// value of 1 (or less) means no retries.
	MaxAttempts() int
	// Delay returns how long to wait before attempt number "attempt".
	// attempt is 2 for the delay before the second attempt, 3 before
	// the third, and so on; Delay is never called for attempt 1, since
	// the first attempt has no preceding wait.
	Delay(attempt int) time.Duration
}

type noRetryPolicy struct{}

func (noRetryPolicy) MaxAttempts() int                { return 1 }
func (noRetryPolicy) Delay(attempt int) time.Duration { return 0 }

// NoRetry returns a RetryPolicy that never retries: a step gets exactly
// one attempt. This is the library default.
func NoRetry() RetryPolicy { return noRetryPolicy{} }

type fixedDelayPolicy struct {
	delay       time.Duration
	maxAttempts int
}

func (p fixedDelayPolicy) MaxAttempts() int                { return p.maxAttempts }
func (p fixedDelayPolicy) Delay(attempt int) time.Duration { return p.delay }

// FixedDelay returns a RetryPolicy that waits a constant delay between
// attempts, for up to maxAttempts attempts in total. maxAttempts < 1 is
// treated as 1 (no retries).
func FixedDelay(delay time.Duration, maxAttempts int) RetryPolicy {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return fixedDelayPolicy{delay: delay, maxAttempts: maxAttempts}
}

type exponentialBackoffPolicy struct {
	base        time.Duration
	maxDelay    time.Duration
	maxAttempts int
}

func (p exponentialBackoffPolicy) MaxAttempts() int { return p.maxAttempts }

func (p exponentialBackoffPolicy) Delay(attempt int) time.Duration {
	delay := p.base
	// attempt 2's delay is base; each attempt after that doubles it,
	// capped at maxDelay so a handful of attempts can't wait for hours.
	for i := 2; i < attempt; i++ {
		if delay >= p.maxDelay {
			return p.maxDelay
		}
		delay *= 2
	}
	if delay > p.maxDelay {
		return p.maxDelay
	}
	return delay
}

// ExponentialBackoff returns a RetryPolicy whose delay doubles after
// each attempt, starting at base and never exceeding maxDelay, for up to
// maxAttempts attempts in total. maxAttempts < 1 is treated as 1 (no
// retries).
func ExponentialBackoff(base, maxDelay time.Duration, maxAttempts int) RetryPolicy {
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	return exponentialBackoffPolicy{base: base, maxDelay: maxDelay, maxAttempts: maxAttempts}
}

type jitterPolicy struct {
	policy   RetryPolicy
	fraction float64
}

func (p jitterPolicy) MaxAttempts() int { return p.policy.MaxAttempts() }

func (p jitterPolicy) Delay(attempt int) time.Duration {
	base := p.policy.Delay(attempt)
	if base <= 0 {
		return base
	}
	spread := float64(base) * p.fraction
	offset := (rand.Float64()*2 - 1) * spread // uniform in [-spread, +spread]
	result := float64(base) + offset
	if result < 0 {
		result = 0
	}
	return time.Duration(result)
}

// WithJitter wraps policy so each delay is randomized within
// +/-fraction of the underlying delay (fraction is clamped to [0, 1]).
// MaxAttempts is unaffected. Jitter helps avoid many callers retrying in
// lockstep after a shared failure (a "retry storm").
func WithJitter(policy RetryPolicy, fraction float64) RetryPolicy {
	if fraction < 0 {
		fraction = 0
	}
	if fraction > 1 {
		fraction = 1
	}
	return jitterPolicy{policy: policy, fraction: fraction}
}
