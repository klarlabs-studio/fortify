package circuitbreaker_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.klarlabs.de/fortify/circuitbreaker"
)

// Example demonstrates basic circuit breaker usage with a simple HTTP-like service call.
func Example() {
	// Create a circuit breaker with default configuration
	cb := circuitbreaker.New[string](circuitbreaker.Config{
		MaxRequests: 3,
		Interval:    time.Second * 10,
		Timeout:     time.Second * 5,
	})

	// Simulate calling an external service
	result, err := cb.Execute(context.Background(), func(ctx context.Context) (string, error) {
		// Your service call here
		return "success", nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	fmt.Printf("Result: %s\n", result)
	// Output: Result: success
}

// Example_customConfiguration demonstrates circuit breaker with custom failure detection.
func Example_customConfiguration() {
	// Create circuit breaker that opens after 3 consecutive failures
	cb := circuitbreaker.New[int](circuitbreaker.Config{
		MaxRequests: 5,
		Interval:    time.Second * 30,
		Timeout:     time.Second * 60,
		ReadyToTrip: func(counts circuitbreaker.Counts) bool {
			// Open circuit after 3 consecutive failures
			return counts.ConsecutiveFailures >= 3
		},
	})

	// Simulate 5 requests
	for i := 0; i < 5; i++ {
		_, err := cb.Execute(context.Background(), func(ctx context.Context) (int, error) {
			// First 3 requests fail
			if i < 3 {
				return 0, errors.New("service unavailable")
			}
			return i, nil
		})

		if err != nil {
			fmt.Printf("Request %d: failed\n", i+1)
		} else {
			fmt.Printf("Request %d: succeeded\n", i+1)
		}

		// Check state after 3rd request
		if i == 2 {
			fmt.Printf("Circuit breaker state: %s\n", cb.State())
		}
	}
	// Output:
	// Request 1: failed
	// Request 2: failed
	// Request 3: failed
	// Circuit breaker state: open
	// Request 4: failed
	// Request 5: failed
}

// Example_errorRateThreshold demonstrates opening the circuit based on error rate.
func Example_errorRateThreshold() {
	cb := circuitbreaker.New[string](circuitbreaker.Config{
		MaxRequests: 10,
		Interval:    time.Second * 60,
		Timeout:     time.Second * 30,
		ReadyToTrip: func(counts circuitbreaker.Counts) bool {
			// Open circuit if error rate exceeds 50% with minimum 10 requests
			failureRate := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 10 && failureRate >= 0.5
		},
	})

	// Simulate 15 requests with 60% failure rate
	for i := 0; i < 15; i++ {
		_, err := cb.Execute(context.Background(), func(ctx context.Context) (string, error) {
			// 60% of requests fail
			if i%5 < 3 {
				return "", errors.New("temporary error")
			}
			return "ok", nil
		})

		if err != nil {
			fmt.Printf("Request %d: error\n", i+1)
		} else {
			fmt.Printf("Request %d: success\n", i+1)
		}

		// Check state after 11th request (first failure after threshold)
		if i == 10 {
			fmt.Printf("State after 11 requests: %s\n", cb.State())
		}
	}
	// Output:
	// Request 1: error
	// Request 2: error
	// Request 3: error
	// Request 4: success
	// Request 5: success
	// Request 6: error
	// Request 7: error
	// Request 8: error
	// Request 9: success
	// Request 10: success
	// Request 11: error
	// State after 11 requests: open
	// Request 12: error
	// Request 13: error
	// Request 14: error
	// Request 15: error
}

// Example_stateManagement demonstrates checking circuit breaker state and manual reset.
func Example_stateManagement() {
	cb := circuitbreaker.New[int](circuitbreaker.Config{
		MaxRequests: 3,
		Interval:    time.Second * 10,
		Timeout:     time.Second * 5,
		ReadyToTrip: func(counts circuitbreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})

	// Initial state
	fmt.Printf("Initial state: %s\n", cb.State())

	// Cause failures to open the circuit
	for i := 0; i < 2; i++ {
		//nolint:errcheck // intentionally ignoring error in example
		_, _ = cb.Execute(context.Background(), func(ctx context.Context) (int, error) {
			return 0, errors.New("failure")
		})
	}

	fmt.Printf("State after failures: %s\n", cb.State())

	// Manually reset the circuit breaker
	cb.Reset()
	fmt.Printf("State after reset: %s\n", cb.State())

	// Output:
	// Initial state: closed
	// State after failures: open
	// State after reset: closed
}

// Example_contextCancellation demonstrates how circuit breaker respects context cancellation.
func Example_contextCancellation() {
	cb := circuitbreaker.New[string](circuitbreaker.Config{
		MaxRequests: 5,
		Interval:    time.Second * 10,
		Timeout:     time.Second * 60,
	})

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	_, err := cb.Execute(ctx, func(ctx context.Context) (string, error) {
		// Simulate long-running operation
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Second):
			return "completed", nil
		}
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
	// Output: Error: context deadline exceeded
}

// Example_httpClient demonstrates using circuit breaker with an HTTP client.
func Example_httpClient() {
	// Create circuit breaker for HTTP calls
	cb := circuitbreaker.New[int](circuitbreaker.Config{
		MaxRequests: 10,
		Interval:    time.Second * 60,
		Timeout:     time.Second * 30,
		ReadyToTrip: func(counts circuitbreaker.Counts) bool {
			// Open after 5 failures in a row
			return counts.ConsecutiveFailures >= 5
		},
		IsSuccessful: func(err error) bool {
			// Consider 5xx errors as failures, but 4xx as success
			// (since 4xx are client errors, not service problems)
			if err == nil {
				return true
			}
			// In real implementation, check HTTP status code
			return false
		},
		OnStateChange: func(from, to circuitbreaker.State) {
			fmt.Printf("HTTP circuit breaker: %s -> %s\n", from, to)
		},
	})

	// Make an HTTP call through the circuit breaker
	statusCode, err := cb.Execute(context.Background(), func(ctx context.Context) (int, error) {
		// Simulate HTTP call
		// resp, err := http.Get("https://api.example.com/endpoint")
		// if err != nil {
		//     return 0, err
		// }
		// return resp.StatusCode, nil
		return 200, nil
	})

	if err != nil {
		fmt.Printf("HTTP call failed: %v\n", err)
		return
	}

	fmt.Printf("HTTP status: %d\n", statusCode)
	// Output: HTTP status: 200
}

// ExampleTripOnConsecutiveFailures shows the shipped predicate replacing a
// hand-written ReadyToTrip. The threshold comes straight from application
// config as an int, so there is no uint32 conversion at the call site and
// nothing for gosec's G115 rule to flag.
func ExampleTripOnConsecutiveFailures() {
	// Threshold as it would arrive from config/flags — a plain int.
	failuresToTrip := 3

	cb := circuitbreaker.New[string](circuitbreaker.Config{
		Timeout:     30 * time.Second,
		ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(failuresToTrip),
	})
	defer func() { _ = cb.Close() }()

	ctx := context.Background()
	for i := 0; i < failuresToTrip; i++ {
		_, _ = cb.Execute(ctx, func(context.Context) (string, error) {
			return "", errors.New("downstream unavailable")
		})
	}

	fmt.Printf("state after %d consecutive failures: %s\n", failuresToTrip, cb.State())
	// Output: state after 3 consecutive failures: open
}

// ExampleTripOnFailureRatio shows the ratio predicate catching a downstream
// that is degraded rather than dead. Consecutive counting cannot: every
// failure here is followed by a success, so the streak never builds.
func ExampleTripOnFailureRatio() {
	cb := circuitbreaker.New[string](circuitbreaker.Config{
		Timeout: 30 * time.Second,
		// Trip at a 50% failure rate, but only once 6 requests are in.
		ReadyToTrip: circuitbreaker.TripOnFailureRatio(0.5, 6),
	})
	defer func() { _ = cb.Close() }()

	ctx := context.Background()
	for i := 0; i < 4 && cb.State() == circuitbreaker.StateClosed; i++ {
		_, _ = cb.Execute(ctx, func(context.Context) (string, error) {
			return "", errors.New("downstream flaky")
		})
		_, _ = cb.Execute(ctx, func(context.Context) (string, error) {
			return "ok", nil
		})
	}

	fmt.Printf("state at a 50%% failure rate: %s\n", cb.State())
	// Output: state at a 50% failure rate: open
}
