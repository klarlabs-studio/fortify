package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/fallback"
	"go.klarlabs.de/fortify/ferrors"
	"go.klarlabs.de/fortify/ratelimit"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
)

func TestChainExecution(t *testing.T) {
	t.Run("executes function through single middleware", func(t *testing.T) {
		tm := timeout.New[int](timeout.Config{
			DefaultTimeout: time.Second,
		})

		chain := New[int]().
			WithTimeout(tm, 100*time.Millisecond)

		result, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != 42 {
			t.Errorf("result = %v, want 42", result)
		}
	})

	t.Run("executes function through multiple middlewares", func(t *testing.T) {
		tm := timeout.New[int](timeout.Config{
			DefaultTimeout: time.Second,
		})
		r := retry.New[int](retry.Config{
			MaxAttempts: 3,
		})

		chain := New[int]().
			WithTimeout(tm, 100*time.Millisecond).
			WithRetry(r)

		result, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != 42 {
			t.Errorf("result = %v, want 42", result)
		}
	})

	t.Run("applies middlewares in correct order", func(t *testing.T) {
		// Timeout wraps retry - timeout should trigger first
		tm := timeout.New[int](timeout.Config{
			DefaultTimeout: 50 * time.Millisecond,
		})
		r := retry.New[int](retry.Config{
			MaxAttempts:   3,
			InitialDelay:  10 * time.Millisecond,
			BackoffPolicy: retry.BackoffConstant,
		})

		chain := New[int]().
			WithTimeout(tm, 50*time.Millisecond).
			WithRetry(r)

		_, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			time.Sleep(100 * time.Millisecond)
			return 42, nil
		})

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("error = %v, want context.DeadlineExceeded", err)
		}
	})

	t.Run("works with all pattern types", func(t *testing.T) {
		cb := circuitbreaker.New[int](circuitbreaker.Config{
			MaxRequests: 10,
			Interval:    time.Second,
		})
		r := retry.New[int](retry.Config{
			MaxAttempts: 2,
		})
		rl := ratelimit.New(ratelimit.Config{
			Rate:     100,
			Interval: time.Second,
		})
		tm := timeout.New[int](timeout.Config{
			DefaultTimeout: time.Second,
		})
		bh := bulkhead.New[int](bulkhead.Config{
			MaxConcurrent: 10,
		})

		chain := New[int]().
			WithCircuitBreaker(cb).
			WithRetry(r).
			WithRateLimit(rl, "test-key").
			WithTimeout(tm, 500*time.Millisecond).
			WithBulkhead(bh)

		result, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != 42 {
			t.Errorf("result = %v, want 42", result)
		}
	})
}

func TestChainWithoutMiddleware(t *testing.T) {
	t.Run("executes function without middleware", func(t *testing.T) {
		chain := New[int]()

		result, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 42, nil
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if result != 42 {
			t.Errorf("result = %v, want 42", result)
		}
	})
}

func TestChainErrorPropagation(t *testing.T) {
	t.Run("propagates errors through chain", func(t *testing.T) {
		r := retry.New[int](retry.Config{
			MaxAttempts: 2,
		})

		chain := New[int]().WithRetry(r)

		expectedErr := errors.New("test error")
		_, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 0, expectedErr
		})

		if !errors.Is(err, expectedErr) {
			t.Errorf("error = %v, want %v", err, expectedErr)
		}
	})
}

func TestChainWithFallback(t *testing.T) {
	fb := fallback.New[string](fallback.Config[string]{
		Fallback: func(context.Context, error) (string, error) {
			return "recovered", nil
		},
	})

	chain := New[string]().WithFallback(fb)

	out, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		return "", errors.New("boom")
	})
	if err != nil {
		t.Fatalf("expected fallback to recover, got %v", err)
	}
	if out != "recovered" {
		t.Errorf("out = %q, want \"recovered\"", out)
	}
}

// TestRetryOutsideBreakerDoesNotStormAnOpenCircuit covers the composition
// hazard in #69.
//
// With retry outside the circuit breaker — the layering Resilience4j and
// Polly recommend, and one users reach for even though fortify's own
// guidance puts retry inside — each retry attempt is a separate
// cb.Execute. Under a "retry everything" default, every logical call made
// while the breaker is Open therefore costs MaxAttempts rejections spaced
// by backoff, instead of the single rejection the breaker intended. That
// multiplies load on the rejection path and multiplies the latency before
// the caller sees an error already known at the first attempt — during the
// incident the breaker exists to contain.
func TestRetryOutsideBreakerDoesNotStormAnOpenCircuit(t *testing.T) {
	downstream := errors.New("downstream exploded")

	const backoff = 50 * time.Millisecond

	cb := circuitbreaker.New[int](circuitbreaker.Config{
		Timeout:     time.Minute, // stay Open for the whole test
		ReadyToTrip: func(circuitbreaker.Counts) bool { return true },
	})
	defer func() { _ = cb.Close() }()

	// Trip the breaker with a single failure.
	if _, err := cb.Execute(context.Background(), func(context.Context) (int, error) {
		return 0, downstream
	}); !errors.Is(err, downstream) {
		t.Fatalf("setup: err = %v, want %v", err, downstream)
	}
	if got := cb.State(); got != circuitbreaker.StateOpen {
		t.Fatalf("setup: breaker state = %s, want open", got)
	}

	// Retry added first makes retry the OUTER layer.
	retries := 0
	chain := New[int]().
		WithRetry(retry.New[int](retry.Config{
			MaxAttempts:   3,
			InitialDelay:  backoff,
			BackoffPolicy: retry.BackoffConstant,
			OnRetry:       func(int, error) { retries++ },
		})).
		WithCircuitBreaker(cb)

	start := time.Now()
	_, err := chain.Execute(context.Background(), func(context.Context) (int, error) {
		t.Error("operation ran while the circuit was open")
		return 0, nil
	})
	elapsed := time.Since(start)

	if !errors.Is(err, ferrors.ErrCircuitOpen) {
		t.Fatalf("err = %v, want ErrCircuitOpen", err)
	}
	if retries != 0 {
		t.Errorf("retry made %d further attempts against an open circuit, want 0", retries)
	}
	// Two extra attempts would have cost 2*backoff in pure waiting.
	if elapsed >= backoff {
		t.Errorf("call took %s against an open circuit; a rejection known at attempt 1 should return immediately", elapsed)
	}
}

// TestRetryInsideBreakerObservesOneOutcomePerCall records the layering
// fortify recommends: the breaker added first is the outer layer, so it
// sees a single outcome per logical call and a blip that retry recovers
// from is counted as one success rather than several failures.
func TestRetryInsideBreakerObservesOneOutcomePerCall(t *testing.T) {
	transient := errors.New("transient blip")

	cb := circuitbreaker.New[int](circuitbreaker.Config{
		Timeout:     time.Minute,
		ReadyToTrip: func(c circuitbreaker.Counts) bool { return c.ConsecutiveFailures >= 2 },
	})
	defer func() { _ = cb.Close() }()

	// Breaker added first => breaker outermost, retry inside it.
	chain := New[int]().
		WithCircuitBreaker(cb).
		WithRetry(retry.New[int](retry.Config{
			MaxAttempts:  3,
			InitialDelay: time.Millisecond,
		}))

	// Two logical calls, each failing twice then succeeding. With retry
	// inside, that is two successes; with retry outside it would be four
	// consecutive failures and a tripped circuit.
	for call := 0; call < 2; call++ {
		attempt := 0
		result, err := chain.Execute(context.Background(), func(context.Context) (int, error) {
			attempt++
			if attempt < 3 {
				return 0, transient
			}
			return 42, nil
		})
		if err != nil {
			t.Fatalf("call %d: unexpected error: %v", call, err)
		}
		if result != 42 {
			t.Errorf("call %d: result = %d, want 42", call, result)
		}
	}

	if got := cb.State(); got != circuitbreaker.StateClosed {
		t.Errorf("breaker state = %s, want closed: recovered blips must not trip it", got)
	}
}
