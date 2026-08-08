// Package middleware provides composable middleware chains for combining
// multiple resilience patterns into a single execution pipeline.
//
// The middleware package allows you to stack circuit breakers, retries,
// rate limiters, timeouts, and bulkheads in any order, creating flexible
// and powerful resilience strategies.
//
// # Layering order
//
// The middleware added FIRST is the OUTERMOST layer. Each subsequent
// With… call nests one level closer to the operation, which runs
// innermost.
//
//	middleware.New[T]().
//	    WithBulkhead(bh).       // outermost: wraps everything below
//	    WithRateLimit(rl, key).
//	    WithCircuitBreaker(cb).
//	    WithRetry(r).
//	    WithTimeout(tm, d)      // innermost: wraps only the operation
//
// Order is not a stylistic choice — it changes production behaviour, and
// it does so silently. The chain compiles and behaves plausibly whichever
// way round it is built, so a mistake surfaces only under load, as
// circuits that trip when they should not.
//
// The consequential pair is retry and the circuit breaker:
//
//   - WithCircuitBreaker(cb).WithRetry(r) puts retry INSIDE the breaker.
//     The breaker observes one outcome per logical call, so a transient
//     failure that retry recovers from is recorded as a success. This is
//     the recommended arrangement.
//   - WithRetry(r).WithCircuitBreaker(cb) puts retry OUTSIDE the breaker.
//     Every attempt becomes a separate breaker observation, so a single
//     recovered blip can contribute MaxAttempts consecutive failures and
//     trip a circuit protecting a downstream that is in fact healthy.
//
// With FailuresToTrip: 5 and MaxAttempts: 3, two recovered blips open the
// circuit in the second arrangement and nothing happens in the first.
//
// Two further placements are worth stating:
//
//   - Timeout innermost bounds each attempt rather than the whole retry
//     sequence, so a slow first attempt cannot consume the budget the
//     remaining attempts need.
//   - Rate limit above the breaker keeps the wait for a quota slot from
//     being charged against an attempt's timeout budget.
//
// See docs/how-to-compose.md for the full rationale and the common
// mis-orderings. The Presets (HTTPClient, DatabaseQuery, RPCDownstream,
// LLMCall) apply this layering already; prefer one of those when it fits.
//
// # Example
//
//	chain := middleware.New[Response]().
//	    WithCircuitBreaker(cb).
//	    WithRetry(retry).
//	    WithTimeout(timeout, 5*time.Second)
//
//	response, err := chain.Execute(ctx, func(ctx context.Context) (Response, error) {
//	    return apiClient.Call(ctx)
//	})
package middleware

import (
	"context"
	"time"

	"go.klarlabs.de/fortify/adaptive"
	"go.klarlabs.de/fortify/budget"
	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/fallback"
	"go.klarlabs.de/fortify/hedge"
	"go.klarlabs.de/fortify/ratelimit"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
)

// Middleware represents a function that wraps another function with resilience behavior.
type Middleware[T any] func(next func(context.Context) (T, error)) func(context.Context) (T, error)

// Chain represents a composable chain of resilience middlewares.
type Chain[T any] struct {
	middlewares []Middleware[T]
}

// New creates a new empty middleware chain.
func New[T any]() *Chain[T] {
	return &Chain[T]{
		middlewares: make([]Middleware[T], 0),
	}
}

// WithCircuitBreaker adds a circuit breaker to the middleware chain.
// Place it OUTSIDE retry (i.e., add it before WithRetry) so the breaker
// observes one outcome per logical call and a transient failure that
// retry recovers from is recorded as a success. Adding it after WithRetry
// makes every attempt a separate observation, so a recovered blip can
// trip a circuit protecting a healthy downstream.
func (c *Chain[T]) WithCircuitBreaker(cb circuitbreaker.CircuitBreaker[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return cb.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithRetry adds retry logic to the middleware chain. Place it INSIDE the
// circuit breaker (i.e., add it after WithCircuitBreaker) and outside the
// timeout, so retries re-run only the operation and each attempt carries
// its own deadline.
//
// If you do invert it, check how your retry.Config classifies
// ferrors.ErrCircuitOpen: with retry outside the breaker, every logical
// call made while the circuit is Open passes the breaker's rejection back
// to retry, and retrying that only adds load and latency to a decision
// already made. See retry.Config.
func (c *Chain[T]) WithRetry(r retry.Retry[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return r.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithRateLimit adds rate limiting to the middleware chain.
// The key parameter identifies the rate limit bucket (e.g., user ID, IP address).
//
// Place it above the circuit breaker (i.e., add it before
// WithCircuitBreaker) and outside any timeout: this middleware blocks in
// rl.Wait until a token is free, and inside a timeout that wait is
// charged against the attempt's deadline rather than the operation.
func (c *Chain[T]) WithRateLimit(rl ratelimit.RateLimiter, key string) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			var zero T
			if err := rl.Wait(ctx, key); err != nil {
				return zero, err
			}
			return next(ctx)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithTimeout adds timeout enforcement to the middleware chain. Place it
// innermost (i.e., add it last) so duration bounds each individual
// attempt rather than the whole retry sequence — otherwise a slow first
// attempt can consume the budget the remaining attempts need.
//
// Worst-case latency for the chain is therefore roughly
// MaxAttempts × duration plus accumulated backoff, not duration. Bound
// the total with a parent context deadline when that matters.
func (c *Chain[T]) WithTimeout(tm timeout.Timeout[T], duration time.Duration) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return tm.Execute(ctx, duration, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithBulkhead adds concurrency limiting to the middleware chain. Place
// it outermost among the rejection patterns (i.e., add it first, or just
// after WithAdaptive) so saturation is shed before any other work is
// done, and outside WithRetry so retries do not each queue for a new slot.
func (c *Chain[T]) WithBulkhead(bh bulkhead.Bulkhead[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return bh.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithAdaptive adds an adaptive concurrency limiter to the middleware chain.
// Place outermost in the chain (before bulkhead) to shed load before any
// pattern-specific work occurs.
func (c *Chain[T]) WithAdaptive(a adaptive.Limiter[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return a.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithBudget adds a cost-budget gate to the middleware chain. Place inside
// WithRetry (i.e., add it after WithRetry in the builder chain) so each
// retry attempt is charged. Place outside WithTimeout so timeouts charge
// against the budget too.
//
// The budget's Charge callback should return *BudgetExceededError as a
// non-retryable error in your retry IsRetryable predicate; otherwise a
// retry will be attempted, which Budget will refuse and return the same
// error.
func (c *Chain[T]) WithBudget(b *budget.Budget[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return b.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithHedge adds hedged-request execution to the middleware chain. Place
// innermost (closest to the operation) so hedging multiplies only the
// operation itself, not the surrounding patterns.
//
// Use only with idempotent operations — hedge attempts may run to completion
// in parallel before cancellation propagates.
func (c *Chain[T]) WithHedge(h hedge.Hedge[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return h.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithFallback adds a fallback to the middleware chain. Place it outermost
// (i.e., add it first) so it can recover from failures produced by any of the
// inner patterns — once retries, the circuit breaker, and timeout have all
// given up, the fallback supplies a default value.
//
// Use only when a sensible default exists for the operation; the fallback's
// handler receives the final error from the wrapped pipeline.
func (c *Chain[T]) WithFallback(fb fallback.Fallback[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return fb.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// Execute runs the given function through all middlewares in the chain.
//
// The middleware added first is the outermost layer: it is entered first
// and exited last, and it wraps every middleware added after it. fn runs
// innermost. A chain built as A, B, C therefore executes
// A → B → C → fn → C → B → A.
//
// See the package doc for why the order matters.
func (c *Chain[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	// Build the chain from right to left
	next := fn
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		next = c.middlewares[i](next)
	}
	return next(ctx)
}
