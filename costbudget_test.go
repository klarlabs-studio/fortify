package fortify_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.klarlabs.de/fortify"
)

func TestWithCostBudget_AccumulatesUntilExceeded(t *testing.T) {
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost: 1.0, // $1.00 ceiling
		CostFunc: func(_ any, _ error) float64 {
			return 0.30 // $0.30 per call
		},
	})

	ctx := context.Background()

	// 0.30 * 3 = 0.90 <= 1.00 : allowed.
	for i := 0; i < 3; i++ {
		if _, err := chain.Execute(ctx, func(context.Context) (string, error) {
			return "ok", nil
		}); err != nil {
			t.Fatalf("call %d unexpected err: %v", i, err)
		}
	}

	// 4th call pushes to 1.20 > 1.00 : breach.
	_, err := chain.Execute(ctx, func(context.Context) (string, error) {
		return "ok", nil
	})
	if !errors.Is(err, fortify.ErrBudgetExceeded) {
		t.Fatalf("expected fortify.ErrBudgetExceeded, got %v", err)
	}
}

func TestWithCostBudget_ErrorsIsMatchesBudgetSentinel(t *testing.T) {
	chain := fortify.New[int]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:  0.50,
		CostFunc: func(any, error) float64 { return 1.0 },
	})

	_, err := chain.Execute(context.Background(), func(context.Context) (int, error) {
		return 1, nil
	})
	if !errors.Is(err, fortify.ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded via errors.Is, got %v", err)
	}
}

func TestWithCostBudget_CostFuncSeesResultAndError(t *testing.T) {
	var gotResult any
	var gotErr error
	sentinel := errors.New("upstream failure")

	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost: 100.0,
		CostFunc: func(result any, err error) float64 {
			gotResult = result
			gotErr = err
			return 1.0
		},
	})

	_, _ = chain.Execute(context.Background(), func(context.Context) (string, error) {
		return "payload", sentinel
	})

	if gotResult != "payload" {
		t.Errorf("CostFunc result = %v, want \"payload\"", gotResult)
	}
	if !errors.Is(gotErr, sentinel) {
		t.Errorf("CostFunc err = %v, want sentinel", gotErr)
	}
}

func TestWithCostBudget_ResetAfterAutoResets(t *testing.T) {
	clock := time.Unix(0, 0)
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:    1.0,
		CostFunc:   func(any, error) float64 { return 0.75 },
		ResetAfter: time.Minute,
		// nowForTest is an unexported test hook on the config; see below.
	}.WithClockForTest(func() time.Time { return clock }))

	ctx := context.Background()

	// First call: 0.75 <= 1.0, allowed.
	if _, err := chain.Execute(ctx, func(context.Context) (string, error) { return "", nil }); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// Second call: 1.50 > 1.0, breach within the window.
	if _, err := chain.Execute(ctx, func(context.Context) (string, error) { return "", nil }); !errors.Is(err, fortify.ErrBudgetExceeded) {
		t.Fatalf("expected breach within window, got %v", err)
	}

	// Advance past the window; the budget must auto-reset.
	clock = clock.Add(time.Minute + time.Nanosecond)
	if _, err := chain.Execute(ctx, func(context.Context) (string, error) { return "", nil }); err != nil {
		t.Fatalf("expected auto-reset after ResetAfter, got %v", err)
	}
}

func TestWithCostBudget_ComposesWithinChain(t *testing.T) {
	calls := 0
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:  1.0,
		CostFunc: func(any, error) float64 { return 0.60 },
	})

	ctx := context.Background()
	op := func(context.Context) (string, error) {
		calls++
		return "v", nil
	}

	if _, err := chain.Execute(ctx, op); err != nil {
		t.Fatalf("first: %v", err)
	}
	if _, err := chain.Execute(ctx, op); !errors.Is(err, fortify.ErrBudgetExceeded) {
		t.Fatalf("expected breach on second, got %v", err)
	}
	if calls != 2 {
		t.Errorf("operation invoked %d times, want 2 (charged even on breaching call)", calls)
	}
}
