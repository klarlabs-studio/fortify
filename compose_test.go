package fortify_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.klarlabs.de/fortify"
	"go.klarlabs.de/fortify/fallback"
	"go.klarlabs.de/fortify/retry"
)

func TestComposer_WithFallback_ReturnsDefaultOnPrimaryFailure(t *testing.T) {
	fb := fallback.New[string](fallback.Config[string]{
		Fallback: func(context.Context, error) (string, error) {
			return "default", nil
		},
	})

	chain := fortify.New[string]().WithFallback(fb)

	out, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		return "", errors.New("primary boom")
	})
	if err != nil {
		t.Fatalf("expected fallback to recover, got err: %v", err)
	}
	if out != "default" {
		t.Errorf("out = %q, want \"default\"", out)
	}
}

func TestComposer_WithFallback_PassesThroughPrimarySuccess(t *testing.T) {
	fb := fallback.New[string](fallback.Config[string]{
		Fallback: func(context.Context, error) (string, error) {
			return "default", nil
		},
	})

	chain := fortify.New[string]().WithFallback(fb)

	out, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		return "primary", nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != "primary" {
		t.Errorf("out = %q, want \"primary\"", out)
	}
}

func TestComposer_WithFallback_ComposesWithRetry(t *testing.T) {
	// Spec's core API chains .WithRetry(...).WithFallback(...): retry exhausts,
	// then fallback supplies the default. WithFallback must be addable as part
	// of the fluent Composer surface, not only standalone.
	rt := retry.New[string](retry.Config{MaxAttempts: 2, InitialDelay: time.Microsecond})
	fb := fallback.New[string](fallback.Config[string]{
		Fallback: func(context.Context, error) (string, error) {
			return "fallback", nil
		},
	})

	attempts := 0
	chain := fortify.New[string]().
		WithFallback(fb).
		WithRetry(rt)

	out, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		attempts++
		return "", errors.New("always fails")
	})
	if err != nil {
		t.Fatalf("expected fallback to recover after retry exhausts, got: %v", err)
	}
	if out != "fallback" {
		t.Errorf("out = %q, want \"fallback\"", out)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (retry ran before fallback)", attempts)
	}
}

func TestExecute_TypedFreeFunction(t *testing.T) {
	// fortify.Execute[T](ctx, policy, fn) is the spec's typed-execute free
	// function. It runs fn through the supplied Composer and returns the typed
	// result without an any-cast at the call site.
	chain := fortify.New[int]().
		WithCostBudget(fortify.CostBudgetConfig{
			MaxCost:  100.0,
			CostFunc: func(any, error) float64 { return 1.0 },
		})

	out, err := fortify.Execute[int](context.Background(), chain, func(context.Context) (int, error) {
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if out != 42 {
		t.Errorf("out = %d, want 42", out)
	}
}

func TestExecute_TypedFreeFunction_PropagatesError(t *testing.T) {
	chain := fortify.New[int]()
	sentinel := errors.New("boom")

	_, err := fortify.Execute[int](context.Background(), chain, func(context.Context) (int, error) {
		return 0, sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel, got %v", err)
	}
}

func TestExecute_TypedFreeFunction_BudgetExceeded(t *testing.T) {
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:  0.50,
		CostFunc: func(any, error) float64 { return 1.0 },
	})

	_, err := fortify.Execute[string](context.Background(), chain, func(context.Context) (string, error) {
		return "ok", nil
	})
	if !errors.Is(err, fortify.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
}
