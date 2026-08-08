// Package ratelimit provides token bucket based rate limiting for controlling
// request rates and preventing resource exhaustion.
//
// The rate limiter uses a token bucket algorithm where tokens are added at a
// constant rate up to a maximum burst capacity. Each request consumes one or more
// tokens. When the bucket is empty, requests are either rejected (Allow) or wait
// for tokens to become available (Wait).
//
// # Basic Usage
//
// Create a rate limiter with default in-memory storage:
//
//	limiter := ratelimit.New(ratelimit.Config{
//	    Rate:     100,           // 100 tokens per second
//	    Burst:    150,           // Allow bursts up to 150
//	    Interval: time.Second,
//	})
//
//	if limiter.Allow(ctx, "user-123") {
//	    // Process request
//	} else {
//	    // Return 429 Too Many Requests
//	}
//
// # Expressing a Quota
//
// Rate is a count per Interval, so the pair expresses any quota — the
// Interval is what carries the unit, and it is not restricted to seconds:
//
//	Rate: 10,  Interval: time.Second  // 10 requests/second
//	Rate: 50,  Interval: time.Minute  // 50 requests/minute
//	Rate: 500, Interval: time.Hour    // 500 requests/hour
//
// Widening Interval is how sustained rates below one request per second
// are configured. Provider quotas stated per minute (as most LLM APIs
// are) transcribe directly, with no per-second arithmetic:
//
//	// Provider tier documented as "50 RPM".
//	limiter := ratelimit.New(ratelimit.Config{
//	    Rate:     50,
//	    Interval: time.Minute,
//	    Burst:    10, // cap how much of the quota one spike may claim
//	})
//
// Refill is continuous rather than stepped: the example above returns
// roughly one token every 1.2s, not 50 tokens once a minute. Burst is the
// bucket capacity and therefore the largest spike allowed; set it equal
// to Rate for a strict quota, or lower to smooth traffic further.
//
// # Storage Backends
//
// The rate limiter uses a pluggable Store interface for state management.
// By default, an in-memory store is used. For distributed rate limiting
// across multiple instances, provide a custom Store implementation backed
// by Redis, DynamoDB, or another distributed backend.
//
//	limiter := ratelimit.New(ratelimit.Config{
//	    Rate:     100,
//	    Burst:    150,
//	    Interval: time.Second,
//	    Store:    myRedisStore,  // Custom Store implementation
//	    FailOpen: true,          // Allow requests on storage failure
//	})
//
// # Key Extraction
//
// Use KeyFunc to extract rate limiting keys from context:
//
//	limiter := ratelimit.New(ratelimit.Config{
//	    Rate: 100,
//	    KeyFunc: func(ctx context.Context, _ string) string {
//	        return ctx.Value("user_id").(string)
//	    },
//	})
//
// # Observability
//
// The rate limiter supports structured logging via slog and metrics collection
// via the optional Metrics interface:
//
//	limiter := ratelimit.New(ratelimit.Config{
//	    Rate:    100,
//	    Logger:  slog.Default(),
//	    Metrics: myMetricsRecorder,
//	})
//
// # Resource Management
//
// Always close the rate limiter when done to release resources:
//
//	limiter := ratelimit.New(config)
//	defer limiter.Close()
//
// This is especially important when using distributed stores that maintain
// connections (Redis, database connections, etc.).
package ratelimit
