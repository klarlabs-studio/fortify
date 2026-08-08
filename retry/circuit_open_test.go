package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.klarlabs.de/fortify/ferrors"
)

// TestCircuitOpenNotRetryableByDefault pins the classification of
// ferrors.ErrCircuitOpen.
//
// An open circuit is a decision, not a transient failure: the breaker has
// already concluded the downstream must be left alone. Retrying it
// multiplies load on the rejection path and multiplies the latency a
// caller waits before receiving an error that was known at the first
// attempt. Polly names the same trap — "never configure retry to catch
// BrokenCircuitException". So the default classification, which retries
// every other error, must exclude it.
//
// Explicit classification still wins: a caller who genuinely wants to
// retry an open circuit says so via IsRetryable, RetryableErrors, or
// ferrors.AsRetryable.
func TestCircuitOpenNotRetryableByDefault(t *testing.T) {
	t.Parallel()

	// structured is the error a real breaker returns. It unwraps to
	// ferrors.ErrCircuitOpen, so matching must see through it.
	structured := &ferrors.CircuitOpenError{
		State:      "open",
		RetryAfter: 30 * time.Second,
	}

	tests := []struct {
		name         string
		config       Config
		err          error
		wantAttempts int
	}{
		{
			name:         "default config does not retry the bare sentinel",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          ferrors.ErrCircuitOpen,
			wantAttempts: 1,
		},
		{
			name:         "default config does not retry the structured error",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          structured,
			wantAttempts: 1,
		},
		{
			name:         "default config does not retry a wrapped circuit-open error",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          fmt.Errorf("calling users service: %w", ferrors.ErrCircuitOpen),
			wantAttempts: 1,
		},
		{
			name: "an unrelated NonRetryableErrors entry does not re-enable it",
			config: Config{
				MaxAttempts:        3,
				InitialDelay:       time.Millisecond,
				NonRetryableErrors: []error{errors.New("something else")},
			},
			err:          ferrors.ErrCircuitOpen,
			wantAttempts: 1,
		},
		{
			name:         "ordinary errors stay retryable",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          errors.New("connection reset by peer"),
			wantAttempts: 3,
		},
		{
			name:         "bulkhead rejections stay retryable",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          ferrors.ErrBulkheadFull,
			wantAttempts: 3,
		},
		{
			name:         "rate-limit rejections stay retryable",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          ferrors.ErrRateLimitExceeded,
			wantAttempts: 3,
		},
		{
			name: "IsRetryable overrides the default",
			config: Config{
				MaxAttempts:  3,
				InitialDelay: time.Millisecond,
				IsRetryable:  func(error) bool { return true },
			},
			err:          ferrors.ErrCircuitOpen,
			wantAttempts: 3,
		},
		{
			name: "listing it in RetryableErrors opts back in",
			config: Config{
				MaxAttempts:     3,
				InitialDelay:    time.Millisecond,
				RetryableErrors: []error{ferrors.ErrCircuitOpen},
			},
			err:          ferrors.ErrCircuitOpen,
			wantAttempts: 3,
		},
		{
			name:         "ferrors.AsRetryable opts back in",
			config:       Config{MaxAttempts: 3, InitialDelay: time.Millisecond},
			err:          ferrors.AsRetryable(ferrors.ErrCircuitOpen),
			wantAttempts: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := New[int](tt.config)

			attempts := 0
			_, err := r.Execute(context.Background(), func(ctx context.Context) (int, error) {
				attempts++
				return 0, tt.err
			})

			if attempts != tt.wantAttempts {
				t.Errorf("attempts = %d, want %d", attempts, tt.wantAttempts)
			}
			if err == nil {
				t.Error("expected the final error to be returned, got nil")
			}
		})
	}
}

// TestCircuitOpenRejectionIsReturnedVerbatim checks that declining to retry
// does not swallow the breaker's structured error: callers still get the
// state and retry-after hint back through errors.As.
func TestCircuitOpenRejectionIsReturnedVerbatim(t *testing.T) {
	t.Parallel()

	want := &ferrors.CircuitOpenError{
		Name:       "users-api",
		State:      "open",
		RetryAfter: 12 * time.Second,
	}

	r := New[int](Config{MaxAttempts: 3, InitialDelay: time.Millisecond})

	_, err := r.Execute(context.Background(), func(ctx context.Context) (int, error) {
		return 0, want
	})

	if !errors.Is(err, ferrors.ErrCircuitOpen) {
		t.Fatalf("errors.Is(err, ErrCircuitOpen) = false, err = %v", err)
	}

	var got *ferrors.CircuitOpenError
	if !errors.As(err, &got) {
		t.Fatalf("errors.As did not recover *CircuitOpenError from %v", err)
	}
	if got.Name != want.Name || got.RetryAfter != want.RetryAfter {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
