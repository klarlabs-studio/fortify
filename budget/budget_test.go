package budget

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type res struct{ tokens int64 }

func chargeTokens(_ context.Context, r res, _ error) Cost {
	return Cost{Tokens: r.tokens}
}

func TestNew_RejectsAllZeroMax(t *testing.T) {
	if _, err := New[res](Config[res]{}); err == nil {
		t.Fatal("expected error for empty Max, got nil")
	}
}

func TestExecute_AllowsUntilTokenCap(t *testing.T) {
	b, err := New[res](Config[res]{
		Max:    Cost{Tokens: 100},
		Charge: chargeTokens,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 4; i++ {
		_, err := b.Execute(ctx, func(context.Context) (res, error) {
			return res{tokens: 25}, nil
		})
		if err != nil {
			t.Fatalf("call %d unexpected err: %v", i, err)
		}
	}

	// Fifth call should breach (4*25 = 100, fifth pushes over).
	_, err = b.Execute(ctx, func(context.Context) (res, error) {
		return res{tokens: 25}, nil
	})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}

	var be *BudgetExceededError
	if !errors.As(err, &be) {
		t.Fatal("expected *BudgetExceededError via errors.As")
	}
	if be.Consumed.Tokens != 125 {
		t.Errorf("Consumed.Tokens = %d, want 125", be.Consumed.Tokens)
	}
	if be.Max.Tokens != 100 {
		t.Errorf("Max.Tokens = %d, want 100", be.Max.Tokens)
	}
}

func TestExecute_RefusesAfterBreach(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{Calls: 1},
	})
	ctx := context.Background()

	_, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })
	if err != nil {
		t.Fatalf("first call err = %v, want nil", err)
	}

	called := false
	_, err = b.Execute(ctx, func(context.Context) (res, error) {
		called = true
		return res{}, nil
	})
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
	if called {
		t.Error("fn ran after budget breach")
	}
}

func TestExecute_OnExceededFiresOnce(t *testing.T) {
	var fired atomic.Int32
	b, _ := New[res](Config[res]{
		Max:        Cost{Calls: 2},
		OnExceeded: func(Cost) { fired.Add(1) },
	})
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, _ = b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })
	}

	if got := fired.Load(); got != 1 {
		t.Errorf("OnExceeded fired %d times, want 1", got)
	}
}

func TestExecute_ChargeNilOnlyCounts(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{Calls: 3},
	})
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	_, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected breach on 4th call, got %v", err)
	}
}

func TestExecute_USDCap(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{USDMicros: 1000},
		Charge: func(_ context.Context, _ res, _ error) Cost {
			return Cost{USDMicros: 600}
		},
	})
	ctx := context.Background()
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })
	if !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected breach, got %v", err)
	}
}

func TestExecute_ConcurrentSafe(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max:    Cost{Tokens: 10_000},
		Charge: chargeTokens,
	})
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = b.Execute(ctx, func(context.Context) (res, error) {
				return res{tokens: 100}, nil
			})
		}()
	}
	wg.Wait()
	c := b.Consumed()
	if c.Calls != 50 {
		t.Errorf("Calls = %d, want 50", c.Calls)
	}
	if c.Tokens != 5000 {
		t.Errorf("Tokens = %d, want 5000", c.Tokens)
	}
}

func TestReset(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{Calls: 1},
	})
	ctx := context.Background()
	_, _ = b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected breach, got %v", err)
	}
	b.Reset()
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
		t.Errorf("after reset: %v", err)
	}
}

func TestResetAfter_AutoResetsOnceWindowElapses(t *testing.T) {
	b, err := New[res](Config[res]{
		Max:        Cost{Calls: 1},
		ResetAfter: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	clock := time.Unix(0, 0)
	b.now = func() time.Time { return clock }

	ctx := context.Background()

	// First call consumes the only allowed call.
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
		t.Fatalf("first call: %v", err)
	}
	// Second call within the window breaches.
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected breach within window, got %v", err)
	}

	// Advance time past the reset window; the next Execute should auto-reset.
	clock = clock.Add(time.Minute + time.Nanosecond)
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
		t.Fatalf("expected auto-reset to clear budget, got %v", err)
	}
	if c := b.Consumed(); c.Calls != 1 {
		t.Errorf("after auto-reset Calls = %d, want 1", c.Calls)
	}
}

func TestResetAfter_DoesNotResetBeforeWindow(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max:        Cost{Calls: 2},
		ResetAfter: time.Minute,
	})

	clock := time.Unix(0, 0)
	b.now = func() time.Time { return clock }

	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		clock = clock.Add(10 * time.Second) // still inside the window
	}
	// Third call, total elapsed 20s < 1m, must breach (no reset yet).
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected breach before window elapsed, got %v", err)
	}
}

func TestResetAfter_ZeroDisablesAutoReset(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{Calls: 1},
	})
	clock := time.Unix(0, 0)
	b.now = func() time.Time { return clock }

	ctx := context.Background()
	_, _ = b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil })

	// Even far in the future, with ResetAfter unset the budget stays breached.
	clock = clock.Add(24 * time.Hour)
	if _, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, nil }); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected sustained breach with ResetAfter=0, got %v", err)
	}
}

func TestBudgetExceededError_LogValue(t *testing.T) {
	e := &BudgetExceededError{
		Consumed: Cost{Tokens: 150, USDMicros: 2000, Calls: 5},
		Max:      Cost{Tokens: 100, USDMicros: 1500, Calls: 3},
	}
	v := e.LogValue()
	if v.Kind() != slog.KindGroup {
		t.Fatalf("LogValue kind = %v, want Group", v.Kind())
	}
	got := map[string]any{}
	for _, a := range v.Group() {
		got[a.Key] = a.Value.Any()
	}
	if got["consumed_tokens"] != int64(150) || got["max_tokens"] != int64(100) {
		t.Errorf("token attrs wrong: %v", got)
	}
	if got["consumed_calls"] != int64(5) || got["max_calls"] != int64(3) {
		t.Errorf("call attrs wrong: %v", got)
	}

	var nilErr *BudgetExceededError
	if g := nilErr.LogValue(); g.Kind() != slog.KindGroup || len(g.Group()) != 0 {
		t.Errorf("nil LogValue = %v, want empty group", g)
	}
}

func TestExecute_PreservesUnderlyingError(t *testing.T) {
	b, _ := New[res](Config[res]{
		Max: Cost{Calls: 5},
	})
	ctx := context.Background()
	want := errors.New("network gone")
	_, err := b.Execute(ctx, func(context.Context) (res, error) { return res{}, want })
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want underlying network error", err)
	}
}
