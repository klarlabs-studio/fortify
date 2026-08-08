package middleware

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/ratelimit"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
)

// TestChainLayeringOrder pins the rule the middleware package documents:
// the middleware added first is the OUTERMOST layer.
//
// The rule is load-bearing rather than cosmetic. It decides whether retry
// sits inside the circuit breaker (breaker sees one outcome per logical
// call) or outside it (every attempt is a separate breaker observation, so
// a recovered blip can trip the circuit). Same config, opposite production
// behaviour, and nothing at the call site shows which you got — so the
// build order in Chain.Execute is worth a test, not just a sentence.
func TestChainLayeringOrder(t *testing.T) {
	t.Parallel()

	// tracer records entry and exit around the layer it wraps.
	tracer := func(name string, log *[]string) Middleware[int] {
		return func(next func(context.Context) (int, error)) func(context.Context) (int, error) {
			return func(ctx context.Context) (int, error) {
				*log = append(*log, "enter:"+name)
				result, err := next(ctx)
				*log = append(*log, "exit:"+name)
				return result, err
			}
		}
	}

	t.Run("first added wraps the rest", func(t *testing.T) {
		t.Parallel()

		var log []string
		c := New[int]()
		c.middlewares = append(c.middlewares,
			tracer("first", &log),
			tracer("second", &log),
			tracer("third", &log),
		)

		if _, err := c.Execute(context.Background(), func(context.Context) (int, error) {
			log = append(log, "operation")
			return 42, nil
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{
			"enter:first", "enter:second", "enter:third",
			"operation",
			"exit:third", "exit:second", "exit:first",
		}
		if strings.Join(log, ",") != strings.Join(want, ",") {
			t.Errorf("order:\n got %v\nwant %v", log, want)
		}
	})

	t.Run("single middleware still wraps the operation", func(t *testing.T) {
		t.Parallel()

		var log []string
		c := New[int]()
		c.middlewares = append(c.middlewares, tracer("only", &log))

		if _, err := c.Execute(context.Background(), func(context.Context) (int, error) {
			log = append(log, "operation")
			return 1, nil
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		want := []string{"enter:only", "operation", "exit:only"}
		if strings.Join(log, ",") != strings.Join(want, ",") {
			t.Errorf("order:\n got %v\nwant %v", log, want)
		}
	})

	t.Run("empty chain runs the operation directly", func(t *testing.T) {
		t.Parallel()

		var log []string
		if _, err := New[int]().Execute(context.Background(), func(context.Context) (int, error) {
			log = append(log, "operation")
			return 1, nil
		}); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if len(log) != 1 || log[0] != "operation" {
			t.Errorf("log = %v, want [operation]", log)
		}
	})
}

// TestCanonicalStackLayering pins the outer-to-inner order of the canonical
// stack documented in the package doc and docs/how-to-compose.md:
//
//	Bulkhead → RateLimit → CircuitBreaker → Retry → Timeout → operation
//
// This guards the documented recipe against silent drift: if the build
// order in Execute is ever reversed, the arrow diagram in the docs would
// still read plausibly while describing the opposite chain.
func TestCanonicalStackLayering(t *testing.T) {
	t.Parallel()

	rl := ratelimit.New(ratelimit.Config{Rate: 100, Burst: 100, Interval: time.Second})
	defer func() { _ = rl.Close() }()

	cb := circuitbreaker.New[int](circuitbreaker.Config{Timeout: time.Minute})
	defer func() { _ = cb.Close() }()

	chain := New[int]().
		WithBulkhead(bulkhead.New[int](bulkhead.Config{MaxConcurrent: 4})).
		WithRateLimit(rl, "layering-key").
		WithCircuitBreaker(cb).
		WithRetry(retry.New[int](retry.Config{MaxAttempts: 3, InitialDelay: time.Millisecond})).
		WithTimeout(timeout.New[int](timeout.Config{DefaultTimeout: time.Second}), time.Second)

	if got := len(chain.middlewares); got != 5 {
		t.Fatalf("len(middlewares) = %d, want 5", got)
	}

	// Timeout is innermost, so each attempt gets its own deadline rather
	// than the whole retry sequence sharing one. Two failures then a
	// success must therefore complete, not exhaust a shared budget.
	attempts := 0
	result, err := chain.Execute(context.Background(), func(ctx context.Context) (int, error) {
		attempts++
		if attempts < 3 {
			return 0, context.DeadlineExceeded
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("innermost timeout did not set a deadline on the operation context")
		} else if time.Until(deadline) <= 0 {
			t.Error("operation context deadline already elapsed")
		}
		return 7, nil
	})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != 7 {
		t.Errorf("result = %d, want 7", result)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if got := cb.State(); got != circuitbreaker.StateClosed {
		t.Errorf("breaker state = %s, want closed: retry is inside the breaker, so the recovered call is one success", got)
	}
}
