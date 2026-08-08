package ratelimit_test

import (
	"context"
	"fmt"
	"time"

	"go.klarlabs.de/fortify/ratelimit"
)

// Example demonstrates basic rate limiting usage.
func Example() {
	// Create rate limiter: 10 requests per second with burst of 20
	rl := ratelimit.New(ratelimit.Config{
		Rate:     10,
		Burst:    20,
		Interval: time.Second,
	})

	// Check if request is allowed
	if rl.Allow(context.Background(), "user-123") {
		fmt.Println("Request allowed")
	} else {
		fmt.Println("Request denied - rate limit exceeded")
	}
	// Output: Request allowed
}

// Example_wait demonstrates waiting for rate limit availability.
func Example_wait() {
	// Create strict rate limiter: 2 requests per second, no burst
	rl := ratelimit.New(ratelimit.Config{
		Rate:     2,
		Burst:    2,
		Interval: time.Second,
	})

	ctx := context.Background()

	// First 2 requests proceed immediately
	for i := 0; i < 2; i++ {
		if err := rl.Wait(ctx, "api-key-456"); err == nil {
			fmt.Printf("Request %d: allowed\n", i+1)
		}
	}

	// Third request would block (not shown in example to avoid timing issues)
	fmt.Println("Rate limiter working")
	// Output:
	// Request 1: allowed
	// Request 2: allowed
	// Rate limiter working
}

// Example_perUserRateLimit demonstrates rate limiting per user.
func Example_perUserRateLimit() {
	// 5 requests per second per user
	rl := ratelimit.New(ratelimit.Config{
		Rate:     5,
		Burst:    10,
		Interval: time.Second,
	})

	// Different users have independent rate limits
	user1Allowed := rl.Allow(context.Background(), "user-1")
	user2Allowed := rl.Allow(context.Background(), "user-2")

	fmt.Printf("User 1 allowed: %v\n", user1Allowed)
	fmt.Printf("User 2 allowed: %v\n", user2Allowed)
	// Output:
	// User 1 allowed: true
	// User 2 allowed: true
}

// Example_burstCapacity demonstrates burst capacity handling.
func Example_burstCapacity() {
	// 10 requests per second, but allow burst of 50
	rl := ratelimit.New(ratelimit.Config{
		Rate:     10,
		Burst:    50,
		Interval: time.Second,
	})

	ctx := context.Background()
	allowed := 0

	// Try 60 requests immediately
	for i := 0; i < 60; i++ {
		if rl.Allow(ctx, "burst-test") {
			allowed++
		}
	}

	// Should allow burst of 50, deny the rest
	fmt.Printf("Allowed: %d requests\n", allowed)
	fmt.Printf("Denied: %d requests\n", 60-allowed)
	// Output:
	// Allowed: 50 requests
	// Denied: 10 requests
}

// Example_keyFunc demonstrates extracting rate limit key from context.
func Example_keyFunc() {
	type contextKey string
	const userIDKey contextKey = "user_id"

	// Configure rate limiter to extract user ID from context
	rl := ratelimit.New(ratelimit.Config{
		Rate:     100,
		Burst:    200,
		Interval: time.Second,
		KeyFunc: func(ctx context.Context, _ string) string {
			if userID, ok := ctx.Value(userIDKey).(string); ok {
				return userID
			}
			return "anonymous"
		},
	})

	// Create context with user ID
	ctx := context.WithValue(context.Background(), userIDKey, "user-789")

	// Key is automatically extracted from context
	if rl.Allow(ctx, "") {
		fmt.Println("Request allowed for user from context")
	}
	// Output: Request allowed for user from context
}

// Example_callbacks demonstrates rate limit event callbacks.
func Example_callbacks() {
	limitedCount := 0

	rl := ratelimit.New(ratelimit.Config{
		Rate:     1,
		Burst:    1,
		Interval: time.Second,
		OnLimit: func(ctx context.Context, key string) {
			limitedCount++
			fmt.Printf("Rate limit exceeded for key: %s\n", key)
		},
	})

	ctx := context.Background()

	// First request succeeds
	rl.Allow(ctx, "test-key")

	// Second request is rate limited
	rl.Allow(ctx, "test-key")

	fmt.Printf("Total rate limited: %d\n", limitedCount)
	// Output:
	// Rate limit exceeded for key: test-key
	// Total rate limited: 1
}

// Example_take demonstrates taking multiple tokens at once.
func Example_take() {
	// 100 requests per second
	rl := ratelimit.New(ratelimit.Config{
		Rate:     100,
		Burst:    100,
		Interval: time.Second,
	})

	// Take 10 tokens for a batch operation
	ctx := context.Background()
	if rl.Take(ctx, "batch-job", 10) {
		fmt.Println("Batch operation allowed (10 tokens)")
	}

	// Take remaining capacity
	if rl.Take(ctx, "batch-job", 90) {
		fmt.Println("Remaining tokens taken: 90")
	}
	// Output:
	// Batch operation allowed (10 tokens)
	// Remaining tokens taken: 90
}

// Example_apiRateLimit demonstrates API rate limiting pattern.
func Example_apiRateLimit() {
	// API rate limit: 1000 requests per hour per API key
	rl := ratelimit.New(ratelimit.Config{
		Rate:     1000,
		Burst:    1000,
		Interval: time.Hour,
		OnLimit: func(ctx context.Context, key string) {
			fmt.Printf("API rate limit exceeded for key: %s\n", key)
		},
	})

	ctx := context.Background()
	apiKey := "api-key-12345"

	// Check if request is allowed
	if rl.Allow(ctx, apiKey) {
		// Process API request
		fmt.Println("API request processed")
	} else {
		// Return 429 Too Many Requests
		fmt.Println("Rate limit exceeded - try again later")
	}
	// Output: API request processed
}

// Example_tieredRateLimit demonstrates different limits for different user tiers.
func Example_tieredRateLimit() {
	// Create rate limiters for different tiers
	freeTier := ratelimit.New(ratelimit.Config{
		Rate:     10,
		Burst:    10,
		Interval: time.Minute,
	})

	premiumTier := ratelimit.New(ratelimit.Config{
		Rate:     100,
		Burst:    200,
		Interval: time.Minute,
	})

	ctx := context.Background()

	// Check rate limits for different users
	if freeTier.Allow(ctx, "free-user-123") {
		fmt.Println("Free tier: request allowed")
	}

	if premiumTier.Allow(ctx, "premium-user-456") {
		fmt.Println("Premium tier: request allowed")
	}
	// Output:
	// Free tier: request allowed
	// Premium tier: request allowed
}

// Example_dynamicRateLimit demonstrates adjusting rate limits based on system load.
func Example_dynamicRateLimit() {
	// Start with normal rate limit
	normalRate := 100

	rl := ratelimit.New(ratelimit.Config{
		Rate:     normalRate,
		Burst:    normalRate * 2,
		Interval: time.Second,
	})

	ctx := context.Background()

	// Under normal conditions
	if rl.Allow(ctx, "dynamic-key") {
		fmt.Println("Request allowed under normal rate")
	}

	// During high load, you could create a new rate limiter with lower limits
	// This is a simplified example - in practice, you'd monitor system metrics
	highLoadRate := 50
	rlHighLoad := ratelimit.New(ratelimit.Config{
		Rate:     highLoadRate,
		Burst:    highLoadRate,
		Interval: time.Second,
	})

	if rlHighLoad.Allow(ctx, "dynamic-key") {
		fmt.Println("Request allowed under high load rate")
	}
	// Output:
	// Request allowed under normal rate
	// Request allowed under high load rate
}

// Example_ipBasedRateLimit demonstrates rate limiting by IP address.
func Example_ipBasedRateLimit() {
	// 100 requests per minute per IP
	rl := ratelimit.New(ratelimit.Config{
		Rate:     100,
		Burst:    150,
		Interval: time.Minute,
		OnLimit: func(ctx context.Context, key string) {
			fmt.Printf("Rate limit exceeded for IP: %s\n", key)
		},
	})

	ctx := context.Background()

	// In a real HTTP handler, you'd extract IP from request
	clientIP := "192.168.1.100"

	if rl.Allow(ctx, clientIP) {
		fmt.Printf("Request from %s allowed\n", clientIP)
	}
	// Output: Request from 192.168.1.100 allowed
}

// Example_contextCancellation demonstrates rate limiter respecting context cancellation.
func Example_contextCancellation() {
	rl := ratelimit.New(ratelimit.Config{
		Rate:     1,
		Burst:    1,
		Interval: time.Second,
	})

	// Use up the burst capacity
	rl.Allow(context.Background(), "test-key")

	// Create context with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	// This would normally wait for a token, but context will be cancelled
	err := rl.Wait(ctx, "test-key")
	fmt.Printf("Error: %v\n", err)
	// Output: Error: context deadline exceeded
}

// Example_execute demonstrates using Execute to combine rate limiting with operation execution.
func Example_execute() {
	rl := ratelimit.New(ratelimit.Config{
		Rate:     10,
		Burst:    10,
		Interval: time.Second,
	})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()

	// Execute combines rate limiting check with operation execution.
	// The operation only runs if rate limiting allows.
	err := rl.Execute(ctx, "api-call", func() error {
		fmt.Println("Operation executed successfully")
		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
	// Output: Operation executed successfully
}

// Example_executeN demonstrates using ExecuteN for operations consuming multiple tokens.
func Example_executeN() {
	rl := ratelimit.New(ratelimit.Config{
		Rate:     100,
		Burst:    100,
		Interval: time.Second,
	})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()

	// ExecuteN is useful for operations with variable cost.
	// For example, batch operations might consume tokens proportional to batch size.
	batchSize := 5
	err := rl.ExecuteN(ctx, "batch-operation", batchSize, func() error {
		fmt.Printf("Batch operation executed (consumed %d tokens)\n", batchSize)
		return nil
	})

	if err != nil {
		fmt.Printf("Error: %v\n", err)
	}
	// Output: Batch operation executed (consumed 5 tokens)
}

// Example_reset demonstrates using Reset for testing scenarios.
func Example_reset() {
	rl := ratelimit.New(ratelimit.Config{
		Rate:     1,
		Burst:    1,
		Interval: time.Second,
	})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()

	// Use up the rate limit
	rl.Allow(ctx, "test-key")
	fmt.Printf("Before reset - buckets: %d\n", rl.BucketCount())

	// Reset clears all rate limiting state.
	// SECURITY NOTE: In production, wrap Reset() with authorization checks.
	err := rl.Reset(ctx)
	if err != nil {
		fmt.Printf("Reset error: %v\n", err)
		return
	}

	fmt.Printf("After reset - buckets: %d\n", rl.BucketCount())
	// Output:
	// Before reset - buckets: 1
	// After reset - buckets: 0
}

// Example_bucketCount demonstrates monitoring active rate limit buckets.
func Example_bucketCount() {
	rl := ratelimit.New(ratelimit.Config{
		Rate:     10,
		Burst:    10,
		Interval: time.Second,
	})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()

	// Initially no buckets
	fmt.Printf("Initial bucket count: %d\n", rl.BucketCount())

	// Each unique key creates a bucket
	rl.Allow(ctx, "user-1")
	rl.Allow(ctx, "user-2")
	rl.Allow(ctx, "user-3")

	// Same key doesn't create new bucket
	rl.Allow(ctx, "user-1")

	fmt.Printf("After operations: %d buckets\n", rl.BucketCount())
	// Output:
	// Initial bucket count: 0
	// After operations: 3 buckets
}

// Example_perMinuteQuota demonstrates transcribing a provider quota that is
// documented per minute — the form used by most LLM and third-party APIs —
// without converting it to a per-second figure. Interval carries the unit,
// so sustained rates below one request per second need no fractional Rate.
func Example_perMinuteQuota() {
	// Provider tier documented as "50 RPM".
	rl := ratelimit.New(ratelimit.Config{
		Rate:     50,
		Interval: time.Minute,
		Burst:    10, // cap how much of the quota a single spike may claim
	})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()

	// The bucket starts full at Burst, so a spike is capped at 10.
	allowed := 0
	for i := 0; i < 25; i++ {
		if rl.Allow(ctx, "openai:gpt-5") {
			allowed++
		}
	}

	fmt.Printf("allowed in spike: %d\n", allowed)
	// Output: allowed in spike: 10
}
