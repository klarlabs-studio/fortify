package circuitbreaker

import (
	"context"
	"errors"
	"testing"
	"time"
)

var errDownstream = errors.New("downstream failed")

func failing(context.Context) (int, error) { return 0, errDownstream }
func succeeding(context.Context) (int, error) {
	return 1, nil
}

// TestCountWindow covers the circular buffer directly, including the
// eviction accounting that keeps stats O(1).
func TestCountWindow(t *testing.T) {
	t.Parallel()

	t.Run("counts outcomes until full", func(t *testing.T) {
		t.Parallel()
		w := newCountWindow(4)
		now := time.Now()

		w.record(false, now)
		w.record(true, now)
		w.record(false, now)

		calls, failures := w.stats(now)
		if calls != 3 || failures != 2 {
			t.Errorf("stats = (%d, %d), want (3, 2)", calls, failures)
		}
	})

	t.Run("evicts the oldest outcome once full", func(t *testing.T) {
		t.Parallel()
		w := newCountWindow(3)
		now := time.Now()

		// Fill with three failures.
		for i := 0; i < 3; i++ {
			w.record(false, now)
		}
		if calls, failures := w.stats(now); calls != 3 || failures != 3 {
			t.Fatalf("stats = (%d, %d), want (3, 3)", calls, failures)
		}

		// Three successes push every failure out one at a time.
		want := []int{2, 1, 0}
		for i, wantFailures := range want {
			w.record(true, now)
			calls, failures := w.stats(now)
			if calls != 3 {
				t.Errorf("after success %d: calls = %d, want 3 (window stays full)", i+1, calls)
			}
			if failures != wantFailures {
				t.Errorf("after success %d: failures = %d, want %d", i+1, failures, wantFailures)
			}
		}
	})

	t.Run("a burst spanning many laps is counted, not lost", func(t *testing.T) {
		t.Parallel()
		w := newCountWindow(10)
		now := time.Now()

		for i := 0; i < 100; i++ {
			w.record(true, now)
		}
		for i := 0; i < 6; i++ {
			w.record(false, now)
		}

		calls, failures := w.stats(now)
		if calls != 10 || failures != 6 {
			t.Errorf("stats = (%d, %d), want (10, 6)", calls, failures)
		}
	})

	t.Run("reset clears everything", func(t *testing.T) {
		t.Parallel()
		w := newCountWindow(3)
		now := time.Now()
		w.record(false, now)
		w.record(false, now)

		w.reset()

		if calls, failures := w.stats(now); calls != 0 || failures != 0 {
			t.Errorf("stats after reset = (%d, %d), want (0, 0)", calls, failures)
		}
	})
}

// TestTimeWindow drives the bucket ring with an explicit clock, so the
// eviction boundary is tested deterministically rather than by sleeping.
func TestTimeWindow(t *testing.T) {
	t.Parallel()

	base := time.Unix(1_700_000_000, 0)

	t.Run("aggregates within the window", func(t *testing.T) {
		t.Parallel()
		w := newTimeWindow(5)

		w.record(false, base)
		w.record(true, base.Add(time.Second))
		w.record(false, base.Add(2*time.Second))

		calls, failures := w.stats(base.Add(2 * time.Second))
		if calls != 3 || failures != 2 {
			t.Errorf("stats = (%d, %d), want (3, 2)", calls, failures)
		}
	})

	t.Run("drops buckets that fall out of the window", func(t *testing.T) {
		t.Parallel()
		w := newTimeWindow(3)

		w.record(false, base)                   // t=0
		w.record(false, base.Add(time.Second))  // t=1
		w.record(true, base.Add(2*time.Second)) // t=2

		if calls, failures := w.stats(base.Add(2 * time.Second)); calls != 3 || failures != 2 {
			t.Fatalf("at t=2 stats = (%d, %d), want (3, 2)", calls, failures)
		}

		// At t=3 the window covers seconds 1..3, so t=0 has aged out.
		if calls, failures := w.stats(base.Add(3 * time.Second)); calls != 2 || failures != 1 {
			t.Errorf("at t=3 stats = (%d, %d), want (2, 1)", calls, failures)
		}

		// At t=5 nothing recorded remains.
		if calls, failures := w.stats(base.Add(5 * time.Second)); calls != 0 || failures != 0 {
			t.Errorf("at t=5 stats = (%d, %d), want (0, 0)", calls, failures)
		}
	})

	t.Run("a stale bucket from a previous lap is not double counted", func(t *testing.T) {
		t.Parallel()
		w := newTimeWindow(3)

		w.record(false, base) // lands in bucket 0 for second N

		// Exactly one lap later the same bucket index is reused; the old
		// content must be discarded, not added to.
		later := base.Add(3 * time.Second)
		w.record(false, later)

		calls, failures := w.stats(later)
		if calls != 1 || failures != 1 {
			t.Errorf("stats = (%d, %d), want (1, 1)", calls, failures)
		}
	})

	t.Run("reset clears everything", func(t *testing.T) {
		t.Parallel()
		w := newTimeWindow(3)
		w.record(false, base)

		w.reset()

		if calls, failures := w.stats(base); calls != 0 || failures != 0 {
			t.Errorf("stats after reset = (%d, %d), want (0, 0)", calls, failures)
		}
	})
}

func TestNormalizeSlidingWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   Config
		want Config
	}{
		{
			name: "disabled is left alone",
			in:   Config{},
			want: Config{},
		},
		{
			name: "count window gets count defaults",
			in:   Config{SlidingWindowType: SlidingWindowCount},
			want: Config{
				SlidingWindowType:    SlidingWindowCount,
				SlidingWindowSize:    DefaultCountWindowSize,
				MinimumCalls:         DefaultMinimumCalls,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "time window gets time defaults",
			in:   Config{SlidingWindowType: SlidingWindowTime},
			want: Config{
				SlidingWindowType:    SlidingWindowTime,
				SlidingWindowSize:    DefaultTimeWindowSeconds,
				MinimumCalls:         DefaultMinimumCalls,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "minimum calls cannot exceed a count window",
			in: Config{
				SlidingWindowType: SlidingWindowCount,
				SlidingWindowSize: 5,
				MinimumCalls:      50,
			},
			want: Config{
				SlidingWindowType:    SlidingWindowCount,
				SlidingWindowSize:    5,
				MinimumCalls:         5,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "minimum calls may exceed a time window",
			in: Config{
				SlidingWindowType: SlidingWindowTime,
				SlidingWindowSize: 5,
				MinimumCalls:      50,
			},
			want: Config{
				SlidingWindowType:    SlidingWindowTime,
				SlidingWindowSize:    5,
				MinimumCalls:         50,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "out-of-range threshold falls back to the default",
			in: Config{
				SlidingWindowType:    SlidingWindowCount,
				SlidingWindowSize:    20,
				MinimumCalls:         10,
				FailureRateThreshold: 1.5,
			},
			want: Config{
				SlidingWindowType:    SlidingWindowCount,
				SlidingWindowSize:    20,
				MinimumCalls:         10,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "oversized window is clamped",
			in: Config{
				SlidingWindowType: SlidingWindowCount,
				SlidingWindowSize: MaxSlidingWindowSize * 4,
				MinimumCalls:      10,
			},
			want: Config{
				SlidingWindowType:    SlidingWindowCount,
				SlidingWindowSize:    MaxSlidingWindowSize,
				MinimumCalls:         10,
				FailureRateThreshold: DefaultFailureRateThreshold,
			},
		},
		{
			name: "unknown window type disables the window",
			in:   Config{SlidingWindowType: SlidingWindowType(99)},
			want: Config{SlidingWindowType: SlidingWindowDisabled},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.in
			got.normalizeSlidingWindow()

			if got.SlidingWindowType != tt.want.SlidingWindowType ||
				got.SlidingWindowSize != tt.want.SlidingWindowSize ||
				got.MinimumCalls != tt.want.MinimumCalls ||
				got.FailureRateThreshold != tt.want.FailureRateThreshold {
				t.Errorf("normalized = {type:%v size:%d min:%d rate:%v}, want {type:%v size:%d min:%d rate:%v}",
					got.SlidingWindowType, got.SlidingWindowSize, got.MinimumCalls, got.FailureRateThreshold,
					tt.want.SlidingWindowType, tt.want.SlidingWindowSize, tt.want.MinimumCalls, tt.want.FailureRateThreshold)
			}
		})
	}
}

// TestSlidingWindowTripsOnPartialFailure is the headline case from #70:
// a downstream failing half its calls never accumulates a
// consecutive-failure streak, so consecutive counting leaves the circuit
// closed indefinitely while a rate window opens it.
func TestSlidingWindowTripsOnPartialFailure(t *testing.T) {
	t.Parallel()

	// F S F F S F S F F S — the sequence from the issue, repeated.
	pattern := []bool{false, true, false, false, true, false, true, false, false, true}

	run := func(t *testing.T, cfg Config) State {
		t.Helper()
		cb := New[int](cfg)
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for round := 0; round < 5; round++ {
			for _, success := range pattern {
				if success {
					_, _ = cb.Execute(ctx, succeeding)
				} else {
					_, _ = cb.Execute(ctx, failing)
				}
				if cb.State() != StateClosed {
					return cb.State()
				}
			}
		}
		return cb.State()
	}

	t.Run("consecutive counting never trips", func(t *testing.T) {
		t.Parallel()
		got := run(t, Config{
			Timeout:     time.Minute,
			ReadyToTrip: func(c Counts) bool { return c.ConsecutiveFailures >= 5 },
		})
		if got != StateClosed {
			t.Errorf("state = %s, want closed: no streak of 5 ever forms in F S F F S F", got)
		}
	})

	t.Run("count-based rate window trips", func(t *testing.T) {
		t.Parallel()
		got := run(t, Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         10,
			FailureRateThreshold: 0.5,
		})
		if got != StateOpen {
			t.Errorf("state = %s, want open: the window sees a 60%% failure rate", got)
		}
	})

	t.Run("time-based rate window trips", func(t *testing.T) {
		t.Parallel()
		got := run(t, Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowTime,
			SlidingWindowSize:    60,
			MinimumCalls:         10,
			FailureRateThreshold: 0.5,
		})
		if got != StateOpen {
			t.Errorf("state = %s, want open: the window sees a 60%% failure rate", got)
		}
	})
}

func TestSlidingWindowMinimumCalls(t *testing.T) {
	t.Parallel()

	t.Run("a cold breaker does not open on one unlucky request", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         10,
			FailureRateThreshold: 0.5,
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		_, _ = cb.Execute(ctx, failing)

		if got := cb.State(); got != StateClosed {
			t.Errorf("state = %s, want closed: 1 call is below MinimumCalls", got)
		}
	})

	t.Run("opens exactly when minimum calls is reached", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         6,
			FailureRateThreshold: 1.0,
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 5; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Fatalf("after 5 failures state = %s, want closed (MinimumCalls is 6)", got)
		}

		_, _ = cb.Execute(ctx, failing)
		if got := cb.State(); got != StateOpen {
			t.Errorf("after 6 failures state = %s, want open", got)
		}
	})

	t.Run("a success can be the call that makes the window eligible", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         4,
			FailureRateThreshold: 0.5,
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Fatalf("after 3 failures state = %s, want closed", got)
		}

		// 3 failures of 4 calls is 75%, over the 50% threshold.
		_, _ = cb.Execute(ctx, succeeding)
		if got := cb.State(); got != StateOpen {
			t.Errorf("state = %s, want open once the 4th call makes the window eligible", got)
		}
	})
}

// TestSlidingWindowDoesNotStraddleABoundary contrasts the sliding window
// with Interval's tumbling reset, the second failure mode in #70.
func TestSlidingWindowDoesNotStraddleABoundary(t *testing.T) {
	t.Parallel()

	const interval = 120 * time.Millisecond

	// Six failures split evenly across a tumbling boundary: three before,
	// three after. A threshold of 5 consecutive failures never sees more
	// than 3, because Interval clears the counts in between.
	burst := func(t *testing.T, cfg Config) State {
		t.Helper()
		cb := New[int](cfg)
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		time.Sleep(interval + 40*time.Millisecond) // cross the reset
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		return cb.State()
	}

	t.Run("tumbling interval loses the burst", func(t *testing.T) {
		t.Parallel()
		got := burst(t, Config{
			Timeout:     time.Minute,
			Interval:    interval,
			ReadyToTrip: func(c Counts) bool { return c.ConsecutiveFailures >= 5 },
		})
		if got != StateClosed {
			t.Errorf("state = %s, want closed: the reset splits 6 failures into 3 + 3", got)
		}
	})

	t.Run("sliding window catches it", func(t *testing.T) {
		t.Parallel()
		got := burst(t, Config{
			Timeout:              time.Minute,
			Interval:             interval,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         5,
			FailureRateThreshold: 0.5,
		})
		if got != StateOpen {
			t.Errorf("state = %s, want open: a sliding window has no boundary to straddle", got)
		}
	})
}

func TestSlidingWindowPrecedenceAndLifecycle(t *testing.T) {
	t.Parallel()

	t.Run("ReadyToTrip overrides the window", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout: time.Minute,
			// Would trip at 50% over 10 calls if it were consulted.
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         4,
			FailureRateThreshold: 0.5,
			// But an explicit predicate wins, and this one never trips.
			ReadyToTrip: func(Counts) bool { return false },
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 20; i++ {
			_, _ = cb.Execute(ctx, failing)
		}

		if got := cb.State(); got != StateClosed {
			t.Errorf("state = %s, want closed: ReadyToTrip overrides the window", got)
		}
	})

	t.Run("default config is unchanged by the new fields", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{Timeout: time.Minute})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 4; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Fatalf("after 4 failures state = %s, want closed (default trips at 5)", got)
		}

		_, _ = cb.Execute(ctx, failing)
		if got := cb.State(); got != StateOpen {
			t.Errorf("after 5 failures state = %s, want open", got)
		}
	})

	t.Run("Reset clears the window", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout:              time.Minute,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         4,
			FailureRateThreshold: 1.0,
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}

		cb.Reset()

		// Three more failures would have tripped it without the reset.
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Errorf("state = %s, want closed: Reset discarded the earlier failures", got)
		}
	})

	t.Run("recovery through half-open starts from fresh evidence", func(t *testing.T) {
		t.Parallel()
		cb := New[int](Config{
			Timeout:              50 * time.Millisecond,
			MaxRequests:          1,
			SlidingWindowType:    SlidingWindowCount,
			SlidingWindowSize:    20,
			MinimumCalls:         4,
			FailureRateThreshold: 1.0,
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 4; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateOpen {
			t.Fatalf("state = %s, want open", got)
		}

		time.Sleep(80 * time.Millisecond) // let it reach half-open
		if _, err := cb.Execute(ctx, succeeding); err != nil {
			t.Fatalf("trial request failed: %v", err)
		}
		if got := cb.State(); got != StateClosed {
			t.Fatalf("state = %s, want closed after a successful trial", got)
		}

		// The pre-trip failures must not still be in the window.
		for i := 0; i < 3; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Errorf("state = %s, want closed: the window was reset on recovery", got)
		}
	})
}
