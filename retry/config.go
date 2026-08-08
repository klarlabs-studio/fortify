package retry

import (
	"log/slog"
	"time"
)

// Config holds the configuration for a Retry instance.
//
// # Default retryability
//
// With no classification configured, every error is retried except
// ferrors.ErrCircuitOpen. An open circuit is a deliberate rejection
// rather than a transient failure — the breaker has already decided the
// downstream must be left alone — so retrying it multiplies load on the
// rejection path and multiplies the latency before the caller sees an
// error that was known at the first attempt.
//
// ferrors.ErrBulkheadFull and ferrors.ErrRateLimitExceeded remain
// retryable by default: both describe a queue that drains on its own.
//
// To retry an open circuit anyway, opt in explicitly with IsRetryable,
// by listing ferrors.ErrCircuitOpen in RetryableErrors, or by wrapping
// with ferrors.AsRetryable.
//
// Field alignment optimization is intentionally disabled for this public API struct because:
// 1. This is a user-facing configuration struct that appears in documentation
// 2. Fields are logically grouped (errors, callbacks, timing, policy) for better API comprehension
// 3. The struct is only instantiated once per retry instance (not in hot paths)
// 4. Memory overhead is negligible compared to API clarity
// 5. Reordering fields would break logical documentation flow
//
//nolint:govet // fieldalignment: API clarity prioritized over memory optimization
type Config struct {
	// RetryableErrors is a list of errors that should trigger a retry.
	// Uses errors.Is for comparison. If both RetryableErrors and NonRetryableErrors
	// are nil/empty, and IsRetryable is nil, all errors are considered retryable
	// except ferrors.ErrCircuitOpen (see the "Default retryability" section above).
	//
	// Listing ferrors.ErrCircuitOpen here opts back into retrying it.
	RetryableErrors []error

	// NonRetryableErrors is a list of errors that should NOT trigger a retry.
	// Uses errors.Is for comparison. Takes precedence over RetryableErrors.
	NonRetryableErrors []error

	// IsRetryable is a custom function to determine if an error should trigger a retry.
	// If provided, this takes precedence over RetryableErrors and NonRetryableErrors.
	// Return true to retry, false to stop.
	IsRetryable func(error) bool

	// OnRetry is called before each retry attempt (not before the first attempt).
	// Receives the attempt number (2, 3, 4, ...) and the error from the previous attempt.
	OnRetry func(attempt int, err error)

	// Logger is used for structured logging. If nil, no logging is performed.
	Logger *slog.Logger

	// InitialDelay is the delay before the first retry attempt.
	// If InitialDelay is 0, defaults to 100ms.
	InitialDelay time.Duration

	// MaxDelay is the maximum delay between retries.
	// If MaxDelay is 0, no maximum is enforced.
	MaxDelay time.Duration

	// Multiplier is the factor by which the delay increases for exponential backoff.
	// If Multiplier is 0, defaults to 2.0.
	Multiplier float64

	// MaxAttempts is the maximum number of attempts (including the initial attempt).
	// If MaxAttempts is 0, defaults to 3.
	MaxAttempts int

	// BackoffPolicy determines the backoff strategy.
	// Defaults to BackoffExponential.
	BackoffPolicy BackoffPolicy

	// Jitter adds random variance to retry delays to prevent thundering herd.
	// When true, adds 0-10% random jitter to each delay.
	Jitter bool
}

// setDefaults applies default values to unset configuration fields
// and clamps invalid values to safe defaults.
func (c *Config) setDefaults() {
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = 3
	}

	if c.InitialDelay <= 0 {
		c.InitialDelay = 100 * time.Millisecond
	}

	if c.MaxDelay < 0 {
		c.MaxDelay = 0
	}

	if c.Multiplier <= 0 {
		c.Multiplier = 2.0
	}
	// Cap Multiplier to a sane upper bound. Combined with the per-call
	// maxBackoffDuration guard in calculateBackoff, this prevents
	// math.Pow from producing +Inf -> negative time.Duration.
	if c.Multiplier > 100 {
		c.Multiplier = 100
	}
}
