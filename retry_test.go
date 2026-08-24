package saga

import (
	"testing"
	"time"
)

func TestNoRetry(t *testing.T) {
	p := NoRetry()
	if p.MaxAttempts() != 1 {
		t.Errorf("MaxAttempts() = %d, want 1", p.MaxAttempts())
	}
	if d := p.Delay(2); d != 0 {
		t.Errorf("Delay(2) = %v, want 0", d)
	}
}

func TestFixedDelay(t *testing.T) {
	p := FixedDelay(2*time.Second, 4)
	if p.MaxAttempts() != 4 {
		t.Errorf("MaxAttempts() = %d, want 4", p.MaxAttempts())
	}
	for _, attempt := range []int{2, 3, 4} {
		if d := p.Delay(attempt); d != 2*time.Second {
			t.Errorf("Delay(%d) = %v, want %v", attempt, d, 2*time.Second)
		}
	}
}

func TestFixedDelay_ClampsMaxAttempts(t *testing.T) {
	for _, maxAttempts := range []int{0, -1, -100} {
		p := FixedDelay(time.Second, maxAttempts)
		if p.MaxAttempts() != 1 {
			t.Errorf("FixedDelay(_, %d).MaxAttempts() = %d, want 1", maxAttempts, p.MaxAttempts())
		}
	}
}

func TestExponentialBackoff(t *testing.T) {
	p := ExponentialBackoff(time.Second, 30*time.Second, 6)
	if p.MaxAttempts() != 6 {
		t.Errorf("MaxAttempts() = %d, want 6", p.MaxAttempts())
	}

	want := map[int]time.Duration{
		2: 1 * time.Second,
		3: 2 * time.Second,
		4: 4 * time.Second,
		5: 8 * time.Second,
		6: 16 * time.Second,
	}
	for attempt, wantDelay := range want {
		if got := p.Delay(attempt); got != wantDelay {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, wantDelay)
		}
	}
}

func TestExponentialBackoff_CapsAtMaxDelay(t *testing.T) {
	p := ExponentialBackoff(time.Second, 5*time.Second, 10)
	// Uncapped this would be 1,2,4,8,16,32... seconds; every attempt from
	// the point it would exceed 5s must be capped at exactly 5s.
	if got := p.Delay(4); got != 4*time.Second {
		t.Errorf("Delay(4) = %v, want %v (below cap)", got, 4*time.Second)
	}
	if got := p.Delay(5); got != 5*time.Second {
		t.Errorf("Delay(5) = %v, want %v (capped)", got, 5*time.Second)
	}
	if got := p.Delay(10); got != 5*time.Second {
		t.Errorf("Delay(10) = %v, want %v (capped)", got, 5*time.Second)
	}
}

func TestExponentialBackoff_ClampsMaxAttempts(t *testing.T) {
	p := ExponentialBackoff(time.Second, time.Minute, 0)
	if p.MaxAttempts() != 1 {
		t.Errorf("MaxAttempts() = %d, want 1", p.MaxAttempts())
	}
}

func TestWithJitter_StaysWithinBounds(t *testing.T) {
	base := FixedDelay(10*time.Second, 5)
	jittered := WithJitter(base, 0.5) // +/- 50%

	minWant := 5 * time.Second
	maxWant := 15 * time.Second
	for i := 0; i < 200; i++ {
		d := jittered.Delay(2)
		if d < minWant || d > maxWant {
			t.Fatalf("Delay(2) = %v, want within [%v, %v]", d, minWant, maxWant)
		}
	}
}

func TestWithJitter_PreservesMaxAttempts(t *testing.T) {
	base := FixedDelay(time.Second, 7)
	jittered := WithJitter(base, 0.2)
	if jittered.MaxAttempts() != 7 {
		t.Errorf("MaxAttempts() = %d, want 7", jittered.MaxAttempts())
	}
}

func TestWithJitter_ZeroDelayUnaffected(t *testing.T) {
	jittered := WithJitter(NoRetry(), 0.5)
	if d := jittered.Delay(2); d != 0 {
		t.Errorf("Delay(2) = %v, want 0", d)
	}
}

func TestWithJitter_ClampsFraction(t *testing.T) {
	base := FixedDelay(10*time.Second, 5)
	jittered := WithJitter(base, 5.0) // way over 1.0, should clamp to 1.0
	for i := 0; i < 50; i++ {
		d := jittered.Delay(2)
		if d < 0 || d > 20*time.Second {
			t.Fatalf("Delay(2) = %v, want within [0, 20s] after clamping fraction to 1.0", d)
		}
	}
}

func TestWithJitter_ProducesVariation(t *testing.T) {
	base := FixedDelay(10*time.Second, 5)
	jittered := WithJitter(base, 0.5)

	seen := make(map[time.Duration]bool)
	for i := 0; i < 50; i++ {
		seen[jittered.Delay(2)] = true
	}
	if len(seen) < 2 {
		t.Error("WithJitter should produce varying delays across many calls")
	}
}
