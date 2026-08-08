package circuitbreaker

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

func TestTripOnConsecutiveFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		n      int
		counts Counts
		want   bool
	}{
		{
			name:   "below threshold does not trip",
			n:      5,
			counts: Counts{ConsecutiveFailures: 4},
			want:   false,
		},
		{
			name:   "at threshold trips",
			n:      5,
			counts: Counts{ConsecutiveFailures: 5},
			want:   true,
		},
		{
			name:   "above threshold trips",
			n:      5,
			counts: Counts{ConsecutiveFailures: 9},
			want:   true,
		},
		{
			name:   "zero is treated as one",
			n:      0,
			counts: Counts{ConsecutiveFailures: 1},
			want:   true,
		},
		{
			name:   "negative is treated as one",
			n:      -7,
			counts: Counts{ConsecutiveFailures: 1},
			want:   true,
		},
		{
			name:   "clamped threshold still needs a failure",
			n:      -7,
			counts: Counts{ConsecutiveFailures: 0},
			want:   false,
		},
		{
			name:   "threshold beyond uint32 clamps instead of wrapping",
			n:      math.MaxUint32 + 100,
			counts: Counts{ConsecutiveFailures: 1},
			want:   false,
		},
		{
			name:   "threshold beyond uint32 trips at the clamp",
			n:      math.MaxUint32 + 100,
			counts: Counts{ConsecutiveFailures: math.MaxUint32},
			want:   true,
		},
		{
			name:   "only consecutive failures matter, not totals",
			n:      3,
			counts: Counts{Requests: 100, TotalFailures: 50, ConsecutiveFailures: 2},
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := TripOnConsecutiveFailures(tt.n)(tt.counts); got != tt.want {
				t.Errorf("TripOnConsecutiveFailures(%d)(%+v) = %v, want %v", tt.n, tt.counts, got, tt.want)
			}
		})
	}
}

func TestTripOnFailureRatio(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ratio       float64
		minRequests int
		counts      Counts
		want        bool
	}{
		{
			name:        "below minimum requests never trips",
			ratio:       0.5,
			minRequests: 20,
			counts:      Counts{Requests: 19, TotalFailures: 19},
			want:        false,
		},
		{
			name:        "at minimum requests and above ratio trips",
			ratio:       0.5,
			minRequests: 20,
			counts:      Counts{Requests: 20, TotalFailures: 11},
			want:        true,
		},
		{
			name:        "at minimum requests and exactly at ratio trips",
			ratio:       0.5,
			minRequests: 20,
			counts:      Counts{Requests: 20, TotalFailures: 10},
			want:        true,
		},
		{
			name:        "below ratio does not trip",
			ratio:       0.5,
			minRequests: 20,
			counts:      Counts{Requests: 20, TotalFailures: 9},
			want:        false,
		},
		{
			name:        "a half-failing downstream trips, unlike consecutive counting",
			ratio:       0.5,
			minRequests: 10,
			counts:      Counts{Requests: 10, TotalFailures: 5, ConsecutiveFailures: 1},
			want:        true,
		},
		{
			name:        "ratio 1.0 requires every call to fail",
			ratio:       1.0,
			minRequests: 5,
			counts:      Counts{Requests: 10, TotalFailures: 9},
			want:        false,
		},
		{
			name:        "ratio 1.0 trips at total failure",
			ratio:       1.0,
			minRequests: 5,
			counts:      Counts{Requests: 10, TotalFailures: 10},
			want:        true,
		},
		{
			name:        "ratio above 1 clamps to 1",
			ratio:       4.2,
			minRequests: 5,
			counts:      Counts{Requests: 10, TotalFailures: 10},
			want:        true,
		},
		{
			name:        "negative ratio clamps to 0 and trips on any failure",
			ratio:       -0.5,
			minRequests: 5,
			counts:      Counts{Requests: 5, TotalFailures: 1},
			want:        true,
		},
		{
			name:        "NaN ratio clamps to 0",
			ratio:       math.NaN(),
			minRequests: 5,
			counts:      Counts{Requests: 5, TotalFailures: 1},
			want:        true,
		},
		{
			name:        "minRequests below one is treated as one",
			ratio:       0.5,
			minRequests: 0,
			counts:      Counts{Requests: 1, TotalFailures: 1},
			want:        true,
		},
		{
			name:        "negative minRequests is treated as one",
			ratio:       0.5,
			minRequests: -3,
			counts:      Counts{Requests: 1, TotalFailures: 1},
			want:        true,
		},
		{
			name:        "zero requests never trips",
			ratio:       0.5,
			minRequests: 1,
			counts:      Counts{},
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := TripOnFailureRatio(tt.ratio, tt.minRequests)(tt.counts)
			if got != tt.want {
				t.Errorf("TripOnFailureRatio(%v, %d)(%+v) = %v, want %v",
					tt.ratio, tt.minRequests, tt.counts, got, tt.want)
			}
		})
	}
}

// TestTripPredicatesDriveTheBreaker wires the predicates through a real
// breaker, so the tables above cannot drift from actual tripping behaviour.
func TestTripPredicatesDriveTheBreaker(t *testing.T) {
	t.Parallel()

	failing := func(context.Context) (int, error) {
		return 0, errors.New("downstream failed")
	}
	succeeding := func(context.Context) (int, error) {
		return 1, nil
	}

	t.Run("consecutive failures", func(t *testing.T) {
		t.Parallel()

		cb := New[int](Config{
			Timeout:     time.Minute,
			ReadyToTrip: TripOnConsecutiveFailures(3),
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		for i := 0; i < 2; i++ {
			_, _ = cb.Execute(ctx, failing)
		}
		if got := cb.State(); got != StateClosed {
			t.Fatalf("after 2 failures state = %s, want closed", got)
		}

		_, _ = cb.Execute(ctx, failing)
		if got := cb.State(); got != StateOpen {
			t.Errorf("after 3 failures state = %s, want open", got)
		}
	})

	t.Run("a success resets consecutive counting", func(t *testing.T) {
		t.Parallel()

		cb := New[int](Config{
			Timeout:     time.Minute,
			ReadyToTrip: TripOnConsecutiveFailures(3),
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		_, _ = cb.Execute(ctx, failing)
		_, _ = cb.Execute(ctx, failing)
		_, _ = cb.Execute(ctx, succeeding)
		_, _ = cb.Execute(ctx, failing)
		_, _ = cb.Execute(ctx, failing)

		if got := cb.State(); got != StateClosed {
			t.Errorf("state = %s, want closed: the success reset the streak", got)
		}
	})

	t.Run("failure ratio catches a partially failing downstream", func(t *testing.T) {
		t.Parallel()

		cb := New[int](Config{
			Timeout:     time.Minute,
			ReadyToTrip: TripOnFailureRatio(0.5, 6),
		})
		defer func() { _ = cb.Close() }()

		ctx := context.Background()
		// Alternate failure/success: consecutive counting would never trip
		// here, because every failure is followed by a success.
		for i := 0; i < 4; i++ {
			_, _ = cb.Execute(ctx, failing)
			if cb.State() == StateOpen {
				break
			}
			_, _ = cb.Execute(ctx, succeeding)
		}

		if got := cb.State(); got != StateOpen {
			t.Errorf("state = %s, want open: a 50%% failure rate should trip a ratio predicate", got)
		}
	})
}
