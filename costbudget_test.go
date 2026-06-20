package fortify_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"go.klarlabs.de/fortify"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
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
	// Clock is a public config field (mirroring budget.Config.Clock); it drives
	// the ResetAfter window deterministically without sleeping.
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:    1.0,
		CostFunc:   func(any, error) float64 { return 0.75 },
		ResetAfter: time.Minute,
		Clock:      func() time.Time { return clock },
	})

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

func TestWithCostBudget_PanicsOnInvalidMaxCost(t *testing.T) {
	cases := []struct {
		name    string
		maxCost float64
	}{
		{"zero", 0},
		{"negative", -1},
		{"NaN", math.NaN()},
		{"PosInf", math.Inf(1)},
		{"NegInf", math.Inf(-1)},
		{"overflow", 1e18}, // 1e18 * 1e6 overflows int64 micro-USD
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatalf("expected panic for MaxCost=%v, got none", tc.maxCost)
				}
			}()
			fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
				MaxCost:  tc.maxCost,
				CostFunc: func(any, error) float64 { return 0 },
			})
		})
	}
}

func TestWithCostBudget_ValidMaxCostDoesNotPanic(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unexpected panic for valid MaxCost: %v", r)
		}
	}()
	chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
		MaxCost:  5.0,
		CostFunc: func(any, error) float64 { return 1.0 },
	})
	if _, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		return "ok", nil
	}); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
}

func TestWithCostBudget_CostFuncNaNInfOverflowChargesNothing(t *testing.T) {
	// A bad CostFunc return (NaN/Inf/overflow) must not corrupt the budget:
	// it is guarded to charge nothing rather than saturating to int64-max and
	// instantly breaching, or producing a torn micro-USD value.
	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 1e18, -1.0} {
		chain := fortify.New[string]().WithCostBudget(fortify.CostBudgetConfig{
			MaxCost:  1.0,
			CostFunc: func(any, error) float64 { return bad },
		})
		// Several calls with a bad cost must never breach (nothing charged).
		for i := 0; i < 5; i++ {
			if _, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
				return "ok", nil
			}); err != nil {
				t.Fatalf("bad cost %v call %d: unexpected breach: %v", bad, i, err)
			}
		}
	}
}

func TestComposer_DelegatesChainPatterns(t *testing.T) {
	// The curated Composer must not be a WithCostBudget-only dead end: it
	// exposes the other chain patterns by delegating to the embedded chain.
	rt := retry.New[string](retry.Config{MaxAttempts: 3, InitialDelay: time.Microsecond})
	tm := timeout.New[string](timeout.Config{})

	attempts := 0
	chain := fortify.New[string]().
		WithRetry(rt).
		WithTimeout(tm, time.Second).
		WithCostBudget(fortify.CostBudgetConfig{
			MaxCost:  100.0,
			CostFunc: func(any, error) float64 { return 1.0 },
		})

	out, err := chain.Execute(context.Background(), func(context.Context) (string, error) {
		attempts++
		if attempts < 2 {
			return "", errors.New("transient")
		}
		return "ok", nil
	})
	if err != nil {
		t.Fatalf("delegated chain Execute err: %v", err)
	}
	if out != "ok" {
		t.Errorf("out = %q, want \"ok\"", out)
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2 (retry delegated)", attempts)
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
