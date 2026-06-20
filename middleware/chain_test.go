package middleware

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/fallback"
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
