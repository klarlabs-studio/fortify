// Package fortify provides production-grade resilience and fault-tolerance
// patterns for Go.
//
// Fortify offers a comprehensive suite of battle-tested patterns including
// circuit breakers, retries, rate limiting, timeouts, bulkheads, fallbacks,
// hedged requests, and adaptive concurrency, with zero external dependencies
// for core functionality.
//
// # Features
//
//   - Type-safe: Go generics for compile-time safety on result types
//   - High performance: sub-microsecond overhead, zero allocations on hot paths
//   - Zero dependencies: core patterns import nothing outside the standard library
//   - Observable: built-in support for slog, OpenTelemetry, and Prometheus
//   - Framework integration: HTTP and gRPC adapters
//   - Composable: fluent middleware.Chain API for combining patterns
//   - Well tested: >95% coverage with race detection
//
// # Installation
//
//	go get go.klarlabs.de/fortify
//
// The minimum supported Go version is declared in go.mod (currently Go 1.25).
//
// # Quick Start
//
//	import (
//	    "context"
//	    "time"
//
//	    "go.klarlabs.de/fortify/circuitbreaker"
//	    "go.klarlabs.de/fortify/retry"
//	)
//
//	func main() {
//	    // Create a circuit breaker
//	    cb := circuitbreaker.New[string](circuitbreaker.Config{
//	        MaxRequests: 100,
//	        Interval:    time.Second * 10,
//	        ReadyToTrip: func(counts circuitbreaker.Counts) bool {
//	            return counts.ConsecutiveFailures >= 3
//	        },
//	    })
//
//	    // Create a retry strategy
//	    r := retry.New[string](retry.Config{
//	        MaxAttempts:   3,
//	        InitialDelay:  time.Millisecond * 100,
//	        BackoffPolicy: retry.BackoffExponential,
//	    })
//
//	    // Use them together
//	    result, err := cb.Execute(context.Background(), func(ctx context.Context) (string, error) {
//	        return r.Execute(ctx, func(ctx context.Context) (string, error) {
//	            return callExternalService(ctx)
//	        })
//	    })
//	}
//
// # Core Patterns
//
// Circuit Breaker: Prevents cascading failures by temporarily stopping requests to failing services
//
//	import "go.klarlabs.de/fortify/circuitbreaker"
//
//	cb := circuitbreaker.New[Response](circuitbreaker.Config{
//	    MaxRequests: 100,
//	    Interval:    time.Second * 60,
//	    Timeout:     time.Second * 30,
//	})
//
// Retry: Handles transient failures with intelligent backoff strategies
//
//	import "go.klarlabs.de/fortify/retry"
//
//	r := retry.New[Response](retry.Config{
//	    MaxAttempts:   3,
//	    InitialDelay:  time.Millisecond * 100,
//	    BackoffPolicy: retry.BackoffExponential,
//	    Multiplier:    2.0,
//	})
//
// Rate Limiter: Token bucket algorithm for controlling request rates
//
//	import "go.klarlabs.de/fortify/ratelimit"
//
//	rl := ratelimit.New(ratelimit.Config{
//	    Rate:     100,
//	    Burst:    10,
//	    Interval: time.Second,
//	})
//
// Timeout: Context-based timeout enforcement
//
//	import "go.klarlabs.de/fortify/timeout"
//
//	t := timeout.New[Response](timeout.Config{
//	    DefaultTimeout: time.Second * 5,
//	})
//
// Bulkhead: Limits concurrent executions to prevent resource exhaustion
//
//	import "go.klarlabs.de/fortify/bulkhead"
//
//	bh := bulkhead.New[Response](bulkhead.Config{
//	    MaxConcurrent: 10,
//	    MaxQueue:      100,
//	})
//
// Fallback: Provides graceful degradation with fallback values
//
//	import "go.klarlabs.de/fortify/fallback"
//
//	fb := fallback.New[Response](fallback.Config[Response]{
//	    Fallback: func(ctx context.Context, err error) (Response, error) {
//	        return cachedResponse, nil
//	    },
//	})
//
// Hedge: Reduces tail latency by firing parallel attempts when the primary call
// is slow. Cancels the loser when a result arrives.
//
//	import "go.klarlabs.de/fortify/hedge"
//
//	h := hedge.New[Response](hedge.Config{
//	    HedgeAfter: 100 * time.Millisecond,
//	    MaxAttempts: 2,
//	})
//
// Adaptive concurrency: Auto-tunes a concurrency cap using AIMD, Vegas, or
// Gradient2 based on observed latency and error signals.
//
//	import "go.klarlabs.de/fortify/adaptive"
//
//	a := adaptive.New[Response](adaptive.Config{
//	    Algorithm: adaptive.Vegas,
//	    InitialLimit: 10,
//	})
//
// # Middleware Composition
//
// Combine multiple patterns using the middleware chain:
//
//	import "go.klarlabs.de/fortify/middleware"
//
//	chain := middleware.New[Response]().
//	    WithCircuitBreaker(cb).
//	    WithRetry(r).
//	    WithTimeout(t, 5*time.Second).
//	    WithRateLimit(rl, "user-123")
//
//	result, err := chain.Execute(ctx, func(ctx context.Context) (Response, error) {
//	    return apiClient.Call(ctx)
//	})
//
// # Cost Budgets
//
// For operations whose cost cannot be capped by attempt count alone
// (LLM calls, paid APIs), the top-level composer offers a monetary cost
// budget. Costs are expressed in US dollars; ResetAfter optionally turns
// the ceiling into a rolling time window.
//
//	out, err := fortify.New[Response]().
//	    WithCostBudget(fortify.CostBudgetConfig{
//	        MaxCost:    5.00, // $5 ceiling
//	        ResetAfter: time.Hour,
//	        CostFunc: func(result any, _ error) float64 {
//	            return result.(Response).USDCost
//	        },
//	    }).
//	    Execute(ctx, callProvider)
//
//	if errors.Is(err, fortify.ErrBudgetExceeded) {
//	    // ceiling reached; operation refused
//	}
//
// For token or call-count ceilings, OnExceeded callbacks, or sharing one
// budget across chains, use the budget package with
// middleware.Chain.WithBudget directly.
//
// # HTTP Integration
//
// Use resilience patterns as HTTP middleware:
//
//	import (
//	    "net/http"
//	    "go.klarlabs.de/fortify/http"
//	)
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/api", apiHandler)
//
//	// Wrap with circuit breaker
//	handler := fortifyhttp.CircuitBreaker(cb, mux)
//
//	http.ListenAndServe(":8080", handler)
//
// # gRPC Integration
//
// Add resilience to gRPC services:
//
//	import (
//	    "go.klarlabs.de/fortify/grpc"
//	    "google.golang.org/grpc"
//	)
//
//	server := grpc.NewServer(
//	    grpc.UnaryInterceptor(
//	        fortifygrpc.UnaryCircuitBreakerInterceptor(cb),
//	    ),
//	)
//
// # Observability
//
// Built-in support for logging, tracing, and metrics:
//
//	import (
//	    "go.klarlabs.de/fortify/otel"
//	    "go.klarlabs.de/fortify/slog"
//	    "go.klarlabs.de/fortify/metrics"
//	)
//
//	// OpenTelemetry tracing
//	cb := circuitbreaker.New[Response](circuitbreaker.Config{
//	    // ... config
//	})
//	traced := otel.WithTracing(cb, "circuit-breaker")
//
//	// Structured logging
//	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
//	cb := circuitbreaker.New[Response](circuitbreaker.Config{
//	    Logger: logger,
//	    // ... config
//	})
//
//	// Prometheus metrics
//	collector := metrics.NewCollector()
//	prometheus.MustRegister(collector)
//
//	// OpenTelemetry metrics (counters, histograms, gauges)
//	import fortifyotelmetrics "go.klarlabs.de/fortify/metrics/otel"
//	m, _ := fortifyotelmetrics.NewMeter(meterProvider) // nil = global provider
//	m.RecordRetryDuration("planner", elapsed.Seconds())
//
// # Package Organization
//
// Core Patterns:
//   - circuitbreaker: Circuit breaker pattern implementation
//   - retry: Retry pattern with backoff strategies
//   - ratelimit: Token bucket rate limiting (pluggable Store for distributed setups)
//   - timeout: Context-based timeout enforcement
//   - bulkhead: Concurrency limiting with semaphores
//   - fallback: Graceful degradation pattern
//   - hedge: Tail-latency reduction via parallel hedged attempts
//   - adaptive: Adaptive concurrency limiting (AIMD, Vegas, Gradient2)
//
// Integration:
//   - http: HTTP middleware for resilience patterns
//   - grpc: gRPC interceptors for client and server
//   - middleware: Composable middleware chains
//
// Observability:
//   - otel: OpenTelemetry tracing integration (spans)
//   - metrics/otel: OpenTelemetry metrics adapter (counters, histograms, gauges)
//   - slog: Structured logging handlers
//   - metrics: Prometheus metrics exporters
//
// Utilities:
//   - ferrors: Standard error types and utilities
//   - testing: Testing utilities and chaos engineering tools
//
// # Error Handling
//
// Fortify provides standard error types for pattern-specific failures:
//
//	import "go.klarlabs.de/fortify/ferrors"
//
//	result, err := cb.Execute(ctx, operation)
//	if errors.Is(err, ferrors.ErrCircuitOpen) {
//	    // Circuit breaker is open
//	}
//	if errors.Is(err, ferrors.ErrRateLimitExceeded) {
//	    // Rate limit exceeded
//	}
//	if errors.Is(err, ferrors.ErrBulkheadFull) {
//	    // Bulkhead at capacity
//	}
//
// # Testing
//
// Fortify includes utilities for chaos engineering and resilience testing:
//
//	import "go.klarlabs.de/fortify/testing"
//
//	// Error injection for testing
//	injector := testing.NewErrorInjector(0.3) // 30% failure rate
//	result, err := injector.Execute(ctx, func(ctx context.Context) (Response, error) {
//	    return apiCall(ctx)
//	})
//
//	// Latency injection
//	latency := testing.NewLatencyInjector(
//	    time.Millisecond*100,
//	    time.Millisecond*500,
//	)
//
// # Performance
//
// Steady-state fast paths target sub-microsecond overhead with zero allocations.
// Run the package benchmarks with `go test -bench=. ./...` for the numbers on
// your hardware; representative figures are published in the README and
// regenerated per release.
//
// # Documentation
//
// For detailed documentation, examples, and API reference:
//   - Package Documentation: https://pkg.go.dev/go.klarlabs.de/fortify
//   - GitHub Repository: https://github.com/klarlabs-studio/fortify
//   - Production Guide: https://github.com/klarlabs-studio/fortify/blob/main/docs/PRODUCTION.md
//   - Error Handling: https://github.com/klarlabs-studio/fortify/blob/main/docs/ERROR_HANDLING.md
//   - Migration Guide: https://github.com/klarlabs-studio/fortify/blob/main/docs/MIGRATION.md
//
// # License
//
// Fortify is released under the MIT License.
// See LICENSE file for details.
package fortify
