// Package retry provides automatic retry logic with intelligent backoff strategies
// for handling transient failures in distributed systems.
//
// The retry package supports multiple backoff policies (exponential, linear, constant),
// error classification for determining retryability, and context-aware cancellation.
//
// Example usage:
//
//	r := retry.New[*User](retry.Config{
//	    MaxAttempts:   3,
//	    InitialDelay:  100 * time.Millisecond,
//	    Multiplier:    2.0,
//	    BackoffPolicy: retry.BackoffExponential,
//	    Jitter:        true,
//	})
//
//	user, err := r.Execute(ctx, func(ctx context.Context) (*User, error) {
//	    return fetchUser(ctx, userID)
//	})
//
// # Retryability
//
// With no classification configured, every error is retried except
// ferrors.ErrCircuitOpen: an open circuit is a decision the breaker has
// already made, not a transient failure, so retrying it only adds load
// and latency to the rejection path. See Config for the full precedence
// order and for how to opt back in.
package retry

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"go.klarlabs.de/fortify/ferrors"
)

// Retry is a generic interface for retry pattern implementation.
// It automatically retries failed operations with configurable backoff strategies.
type Retry[T any] interface {
	// Execute runs the given function with automatic retries on failure.
	// It respects context cancellation and stops retrying if the context is cancelled.
	// Returns the result and error from the last attempt.
	Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error)
}

// retry is the concrete implementation of Retry.
type retry[T any] struct {
	config Config
}

// New creates a new Retry instance with the given configuration.
//
//nolint:gocritic // hugeParam: Config passed by value for API consistency across all patterns
func New[T any](config Config) Retry[T] {
	config.setDefaults()
	return &retry[T]{
		config: config,
	}
}

// Execute implements the Retry interface.
func (r *retry[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var result T
	var err error

	// Reusable timer across iterations. Replacing time.After avoids leaking a
	// timer per attempt when the context is cancelled mid-backoff. Start
	// stopped; we Reset before each wait.
	var timer *time.Timer

	for attempt := 1; attempt <= r.config.MaxAttempts; attempt++ {
		// Check context before attempting
		if err := ctx.Err(); err != nil {
			if timer != nil {
				timer.Stop()
			}
			return result, err
		}

		// Execute the function
		result, err = fn(ctx)

		// Success - return immediately
		if err == nil {
			if timer != nil {
				timer.Stop()
			}
			return result, nil
		}

		// Check if error is retryable
		if !r.isRetryable(err) {
			r.logAttempt(ctx, attempt, err, false)
			if timer != nil {
				timer.Stop()
			}
			return result, err
		}

		// Last attempt - don't wait
		if attempt == r.config.MaxAttempts {
			r.logAttempt(ctx, attempt, err, false)
			if timer != nil {
				timer.Stop()
			}
			return result, err
		}

		// Log retry attempt
		r.logAttempt(ctx, attempt, err, true)

		// Call OnRetry callback
		if r.config.OnRetry != nil {
			r.safeCallback(func() {
				r.config.OnRetry(attempt+1, err)
			})
		}

		// Calculate backoff delay
		delay := calculateBackoff(
			r.config.BackoffPolicy,
			attempt,
			r.config.InitialDelay,
			r.config.MaxDelay,
			r.config.Multiplier,
			r.config.Jitter,
		)

		if timer == nil {
			timer = time.NewTimer(delay)
		} else {
			timer.Reset(delay)
		}

		// Wait before retry with context cancellation support.
		select {
		case <-timer.C:
			// Continue to next attempt.
		case <-ctx.Done():
			// Drain the timer to avoid leaking.
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return result, ctx.Err()
		}
	}

	if timer != nil {
		timer.Stop()
	}
	return result, err
}

// isRetryable determines if an error should trigger a retry.
func (r *retry[T]) isRetryable(err error) bool {
	if err == nil {
		return false
	}

	// Custom predicate takes precedence
	if r.config.IsRetryable != nil {
		return r.config.IsRetryable(err)
	}

	// Check non-retryable errors first
	for _, nonRetryable := range r.config.NonRetryableErrors {
		if errors.Is(err, nonRetryable) {
			return false
		}
	}

	// Check retryable errors
	if len(r.config.RetryableErrors) > 0 {
		for _, retryable := range r.config.RetryableErrors {
			if errors.Is(err, retryable) {
				return true
			}
		}
		// If RetryableErrors is specified but error doesn't match, don't retry
		return false
	}

	// Check if error implements RetryableError interface.
	// An explicit ferrors.AsRetryable marking is a deliberate caller
	// signal, so it outranks the control-flow default below.
	if ferrors.IsRetryable(err) {
		return true
	}

	// Fortify's own control-flow rejections are decisions, not transient
	// failures. An open circuit has already concluded the downstream must
	// be left alone; retrying multiplies load on the rejection path and
	// multiplies the latency before the caller sees an error that was
	// known at the first attempt. Excluded from the permissive default so
	// that composing retry outside a breaker is not a silent trap.
	//
	// Deliberately narrow: ErrBulkheadFull and ErrRateLimitExceeded are
	// also rejections, but they describe a queue that drains on its own,
	// so backing off and retrying remains a defensible policy.
	if errors.Is(err, ferrors.ErrCircuitOpen) {
		return false
	}

	// Default: retry all errors if no classification is configured
	return true
}

// logAttempt logs retry attempts using structured logging.
func (r *retry[T]) logAttempt(ctx context.Context, attempt int, err error, willRetry bool) {
	if r.config.Logger == nil {
		return
	}

	if willRetry {
		r.config.Logger.WarnContext(ctx, "retry attempt failed, retrying",
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", r.config.MaxAttempts),
			slog.String("error", err.Error()),
		)
	} else {
		r.config.Logger.ErrorContext(ctx, "retry exhausted",
			slog.Int("attempt", attempt),
			slog.Int("max_attempts", r.config.MaxAttempts),
			slog.String("error", err.Error()),
		)
	}
}

// safeCallback executes a callback with panic recovery.
func (r *retry[T]) safeCallback(fn func()) {
	defer func() {
		if rec := recover(); rec != nil {
			if r.config.Logger != nil {
				r.config.Logger.Error("retry callback panic",
					slog.String("pattern", "retry"),
					slog.Any("panic", rec),
				)
			}
		}
	}()
	fn()
}
