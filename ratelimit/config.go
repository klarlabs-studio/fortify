package ratelimit

import (
	"context"
	"log/slog"
	"time"
)

// Config holds the configuration for a RateLimiter.
type Config struct {
	// KeyFunc transforms or replaces the rate-limit key for each call.
	// Receives the request context AND the caller-supplied key (passed to
	// Allow/Wait/Take). KeyFunc may use the supplied key, ignore it, or
	// derive a new key from context (user ID, IP, tenant, etc.).
	//
	// If nil, the caller-supplied key is used verbatim.
	//
	// Common patterns:
	//
	//	// Always derive from context, ignore caller key:
	//	KeyFunc: func(ctx context.Context, _ string) string {
	//	    return userIDFromContext(ctx)
	//	}
	//
	//	// Prefer ctx-derived, fall back to caller key:
	//	KeyFunc: func(ctx context.Context, key string) string {
	//	    if u := userIDFromContext(ctx); u != "" { return u }
	//	    return key
	//	}
	//
	//	// Namespace the caller key by tenant from context:
	//	KeyFunc: func(ctx context.Context, key string) string {
	//	    return tenantFromContext(ctx) + ":" + key
	//	}
	//
	// Key length constraints: the returned key must respect the Store's
	// maximum key length (e.g., DefaultMaxKeyLength=1024 for MemoryStore).
	// Keys exceeding the limit cause ErrKeyTooLong errors from the Store.
	KeyFunc func(ctx context.Context, key string) string

	// OnAllow is called when a request is allowed (not rate limited).
	// It receives the context and key that was allowed.
	//
	// This callback is useful for observability - tracking allowed requests,
	// updating metrics, or logging successful rate limit checks.
	//
	// By default, the callback is executed synchronously, which may block the hot path.
	// For non-blocking behavior, either:
	//   1. Keep the callback lightweight (counter increment)
	//   2. Dispatch to a goroutine or channel within your callback
	//   3. Use a bounded worker pool for heavy operations
	//
	// The callback is wrapped with panic recovery - panics are logged but don't crash.
	OnAllow func(ctx context.Context, key string)

	// OnLimit is called when a request is rate limited (denied).
	// It receives the context and key that was rate limited.
	//
	// By default, the callback is executed synchronously, which may block the hot path.
	// For non-blocking behavior, either:
	//   1. Keep the callback lightweight (logging, counter increment)
	//   2. Dispatch to a goroutine or channel within your callback
	//   3. Use a bounded worker pool for heavy operations
	//
	// The callback is wrapped with panic recovery - panics are logged but don't crash.
	OnLimit func(ctx context.Context, key string)

	// Logger is used for structured logging. If nil, no logging is performed.
	Logger *slog.Logger

	// Metrics is used for metrics collection. If nil, no metrics are recorded.
	// Implement the Metrics interface to integrate with your metrics system.
	Metrics Metrics

	// Store is the storage backend for rate limiter state.
	// If nil, an in-memory store is created automatically using NewMemoryStore().
	// For distributed rate limiting across multiple instances, provide a custom
	// Store implementation backed by Redis, DynamoDB, or another distributed backend.
	Store Store

	// Interval is the time period over which Rate tokens are added.
	// Defaults to 1 second if zero.
	//
	// Interval is the unit of Rate: together they express any quota, not
	// just per-second ones. Set it to time.Minute to transcribe a
	// requests-per-minute quota (the form most third-party and LLM
	// provider docs use) without converting to a per-second figure.
	//
	// Refill is continuous, not stepped: with Rate=50 and
	// Interval=time.Minute one token returns roughly every 1.2s rather
	// than 50 arriving at once each minute.
	//
	// Clamped to [MinInterval, MaxInterval].
	Interval time.Duration

	// Rate is the number of tokens added to the bucket per Interval.
	// Must be positive. Defaults to 100 if zero or negative.
	//
	// Rate is always read relative to Interval, so sub-1/sec sustained
	// rates are expressed by widening Interval rather than by a
	// fractional Rate:
	//
	//	Rate: 10,  Interval: time.Second  // 10 requests/second
	//	Rate: 50,  Interval: time.Minute  // 50 requests/minute (0.83/sec)
	//	Rate: 500, Interval: time.Hour    // 500 requests/hour
	Rate int

	// Burst is the maximum number of tokens in the bucket (bucket capacity).
	// This allows short bursts of requests up to this limit.
	// Must be positive. Defaults to Rate if zero or negative.
	//
	// Setting Burst > Rate allows temporary bursts above the sustained rate.
	// Setting Burst == Rate enforces strict rate limiting with no burst.
	Burst int

	// MaxTokensPerRequest limits the maximum tokens that can be requested
	// in a single Take() call. This prevents integer overflow and DoS attacks.
	// Defaults to Burst * 10 if zero or negative.
	MaxTokensPerRequest int

	// FailOpen determines behavior when storage operations fail.
	// If true, allows requests when storage is unavailable (favors availability).
	// If false (default), denies requests when storage fails (favors consistency).
	// This only applies when using a custom Store that can fail (e.g., Redis, DynamoDB).
	//
	// Security note: FailOpen=true may allow more requests than intended during
	// storage outages. Use with caution in security-critical applications.
	FailOpen bool
}

const (
	// DefaultRate is the default rate limit.
	DefaultRate = 100

	// MaxRate is the maximum allowed rate limit.
	MaxRate = 1000000

	// MaxBurst is the maximum allowed burst size.
	MaxBurst = 1000000

	// MinInterval is the minimum allowed interval.
	// Intervals below 1ms are operationally meaningless for rate limiting
	// and maximize the overflow risk surface in token calculations (HIGH-01).
	MinInterval = time.Millisecond

	// MaxInterval is the maximum allowed interval.
	MaxInterval = 24 * time.Hour

	// DefaultMaxTokensMultiplier is the default multiplier for MaxTokensPerRequest.
	DefaultMaxTokensMultiplier = 10

	// MaxMaxTokensPerRequest is the maximum allowed value for MaxTokensPerRequest.
	// This prevents integer overflow in token calculations.
	MaxMaxTokensPerRequest = 10000000
)

// setDefaults applies default values to unset configuration fields.
func (c *Config) setDefaults() {
	if c.Rate <= 0 {
		c.Rate = DefaultRate
	}
	// Cap rate to prevent overflow
	if c.Rate > MaxRate {
		c.Rate = MaxRate
	}

	if c.Burst <= 0 {
		c.Burst = c.Rate
	}
	// Cap burst to prevent overflow
	if c.Burst > MaxBurst {
		c.Burst = MaxBurst
	}

	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	// Floor interval to prevent overflow in token calculations (HIGH-01)
	if c.Interval < MinInterval {
		c.Interval = MinInterval
	}
	// Cap interval to reasonable maximum
	if c.Interval > MaxInterval {
		c.Interval = MaxInterval
	}

	if c.MaxTokensPerRequest <= 0 {
		c.MaxTokensPerRequest = c.Burst * DefaultMaxTokensMultiplier
	}
	// Cap MaxTokensPerRequest to prevent overflow
	if c.MaxTokensPerRequest > MaxMaxTokensPerRequest {
		c.MaxTokensPerRequest = MaxMaxTokensPerRequest
	}

	if c.Store == nil {
		c.Store = NewMemoryStore()
	}
}
