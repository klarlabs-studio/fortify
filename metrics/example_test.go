package metrics_test

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.klarlabs.de/fortify/metrics"
)

// Example demonstrates basic Prometheus metrics integration.
func Example() {
	// Register Fortify metrics with default Prometheus registry
	metrics.MustRegister(prometheus.DefaultRegisterer)

	// Use the default collector to record metrics
	collector := metrics.DefaultCollector()

	// Record circuit breaker metrics
	collector.RecordCircuitBreakerRequest("api-client", "closed")
	collector.RecordCircuitBreakerSuccess("api-client")

	// Record retry metrics
	collector.RecordRetryAttempts("database-query", 2)
	collector.RecordRetrySuccess("database-query")

	fmt.Println("Metrics recorded successfully")
	// Output: Metrics recorded successfully
}

// Example_httpServer demonstrates exposing Fortify metrics via HTTP.
func Example_httpServer() {
	// Create a custom registry
	registry := prometheus.NewRegistry()

	// Create and register Fortify collector
	collector := metrics.NewCollector()
	registry.MustRegister(collector)

	// Record some metrics
	collector.RecordCircuitBreakerState("payment-service", 0) // 0 = closed
	collector.RecordCircuitBreakerRequest("payment-service", "closed")
	collector.RecordCircuitBreakerSuccess("payment-service")

	// Expose metrics endpoint
	http.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	// Start server (commented out for example)
	// http.ListenAndServe(":8080", nil)

	fmt.Println("Metrics server configured")
	// Output: Metrics server configured
}

// Example_circuitBreaker demonstrates circuit breaker metrics.
func Example_circuitBreaker() {
	collector := metrics.DefaultCollector()

	// Record circuit breaker lifecycle
	collector.RecordCircuitBreakerState("external-api", 0) // 0 = closed
	collector.RecordCircuitBreakerRequest("external-api", "closed")
	collector.RecordCircuitBreakerSuccess("external-api")

	// Circuit opens after failures
	collector.RecordCircuitBreakerFailure("external-api")
	collector.RecordCircuitBreakerFailure("external-api")
	collector.RecordCircuitBreakerFailure("external-api")
	collector.RecordCircuitBreakerStateChange("external-api", "closed", "open")
	collector.RecordCircuitBreakerState("external-api", 1) // 1 = open

	// Subsequent requests fail fast
	collector.RecordCircuitBreakerRequest("external-api", "open")

	fmt.Println("Circuit breaker metrics recorded")
	// Output: Circuit breaker metrics recorded
}

// Example_retry demonstrates retry metrics.
func Example_retry() {
	collector := metrics.DefaultCollector()

	// Successful retry after 3 attempts
	collector.RecordRetryAttempts("database-write", 3)
	collector.RecordRetryDuration("database-write", 0.450) // 450ms
	collector.RecordRetrySuccess("database-write")

	// Failed retry
	collector.RecordRetryAttempts("api-call", 5)
	collector.RecordRetryDuration("api-call", 2.1) // 2.1s
	collector.RecordRetryFailure("api-call")

	fmt.Println("Retry metrics recorded")
	// Output: Retry metrics recorded
}

// Example_rateLimit demonstrates rate limiting metrics.
func Example_rateLimit() {
	collector := metrics.DefaultCollector()

	// Allowed requests
	collector.RecordRateLimitAllowed("api-endpoint", "user-123")
	collector.RecordRateLimitAllowed("api-endpoint", "user-456")

	// Denied request
	collector.RecordRateLimitDenied("api-endpoint", "user-123")

	// Wait time for rate limit
	collector.RecordRateLimitWaitTime("api-endpoint", "user-123", 0.5)

	fmt.Println("Rate limit metrics recorded")
	// Output: Rate limit metrics recorded
}

// Example_timeout demonstrates timeout metrics.
func Example_timeout() {
	collector := metrics.DefaultCollector()

	// Successful execution
	collector.RecordTimeoutExecution("slow-query")
	collector.RecordTimeoutDuration("slow-query", false, 0.8)

	// Timeout exceeded
	collector.RecordTimeoutExecution("very-slow-query")
	collector.RecordTimeoutExceeded("very-slow-query")
	collector.RecordTimeoutDuration("very-slow-query", true, 5.0)

	fmt.Println("Timeout metrics recorded")
	// Output: Timeout metrics recorded
}

// Example_bulkhead demonstrates bulkhead metrics.
func Example_bulkhead() {
	collector := metrics.DefaultCollector()

	// Active and queued requests
	collector.RecordBulkheadActive("worker-pool", 10)
	collector.RecordBulkheadQueued("worker-pool", 5)

	// Successful request
	collector.RecordBulkheadSuccess("worker-pool")
	collector.RecordBulkheadDuration("worker-pool", 0.250)

	// Update active count
	collector.RecordBulkheadActive("worker-pool", 9)

	// Rejected request (bulkhead full)
	collector.RecordBulkheadRejected("worker-pool")

	fmt.Println("Bulkhead metrics recorded")
	// Output: Bulkhead metrics recorded
}

// Example_multiplePatterns demonstrates recording metrics for multiple patterns.
func Example_multiplePatterns() {
	collector := metrics.DefaultCollector()

	// Scenario: API call with retry and circuit breaker
	// Circuit breaker is closed
	collector.RecordCircuitBreakerState("user-service", 0)
	collector.RecordCircuitBreakerRequest("user-service", "closed")

	// First attempt fails, retry
	collector.RecordRetryAttempts("user-service-call", 1)

	// Second attempt succeeds
	collector.RecordRetryAttempts("user-service-call", 2)
	collector.RecordRetrySuccess("user-service-call")
	collector.RecordRetryDuration("user-service-call", 0.350)

	collector.RecordCircuitBreakerSuccess("user-service")

	fmt.Println("Multiple pattern metrics recorded")
	// Output: Multiple pattern metrics recorded
}
