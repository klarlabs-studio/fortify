// Package middleware provides composable middleware chains for combining
// multiple resilience patterns into a single execution pipeline.
//
// The middleware package allows you to stack circuit breakers, retries,
// rate limiters, timeouts, and bulkheads in any order, creating flexible
// and powerful resilience strategies.
//
// Example usage:
//
//	chain := middleware.New[Response]().
//	    WithCircuitBreaker(cb).
//	    WithRetry(retry).
//	    WithTimeout(timeout, 5*time.Second).
//	    WithRateLimit(limiter, "user-123")
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
func (c *Chain[T]) WithCircuitBreaker(cb circuitbreaker.CircuitBreaker[T]) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return cb.Execute(ctx, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithRetry adds retry logic to the middleware chain.
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

// WithTimeout adds timeout enforcement to the middleware chain.
func (c *Chain[T]) WithTimeout(tm timeout.Timeout[T], duration time.Duration) *Chain[T] {
	middleware := func(next func(context.Context) (T, error)) func(context.Context) (T, error) {
		return func(ctx context.Context) (T, error) {
			return tm.Execute(ctx, duration, next)
		}
	}
	c.middlewares = append(c.middlewares, middleware)
	return c
}

// WithBulkhead adds concurrency limiting to the middleware chain.
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
// Middlewares are applied in the order they were added to the chain.
func (c *Chain[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	// Build the chain from right to left
	next := fn
	for i := len(c.middlewares) - 1; i >= 0; i-- {
		next = c.middlewares[i](next)
	}
	return next(ctx)
}
