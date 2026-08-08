package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimiterAllow(t *testing.T) {
	t.Parallel()
	t.Run("allows requests within rate limit", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
		})

		ctx := context.Background()
		for i := 0; i < 5; i++ {
			if !limiter.Allow(ctx, "test-key") {
				t.Errorf("request %d should be allowed", i+1)
			}
		}
	})

	t.Run("blocks requests exceeding burst", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     5,
			Burst:    3,
			Interval: time.Second,
		})

		ctx := context.Background()
		// First 3 should succeed (burst limit)
		for i := 0; i < 3; i++ {
			if !limiter.Allow(ctx, "test-key") {
				t.Errorf("request %d should be allowed (within burst)", i+1)
			}
		}

		// 4th request should be blocked
		if limiter.Allow(ctx, "test-key") {
			t.Error("request should be blocked (exceeds burst)")
		}
	})

	t.Run("separate keys have independent limits", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     2,
			Burst:    2,
			Interval: time.Second,
		})

		ctx := context.Background()

		// Exhaust key1
		limiter.Allow(ctx, "key1")
		limiter.Allow(ctx, "key1")

		// key2 should still have full quota
		if !limiter.Allow(ctx, "key2") {
			t.Error("key2 should be allowed (independent quota)")
		}
		if !limiter.Allow(ctx, "key2") {
			t.Error("key2 should be allowed (independent quota)")
		}
	})

	t.Run("refills tokens over time", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    2,
			Interval: 100 * time.Millisecond,
		})

		ctx := context.Background()

		// Exhaust burst
		limiter.Allow(ctx, "test-key")
		limiter.Allow(ctx, "test-key")

		// Should be blocked
		if limiter.Allow(ctx, "test-key") {
			t.Error("should be blocked initially")
		}

		// Wait for refill (10 tokens per 100ms = 1 token per 10ms)
		time.Sleep(15 * time.Millisecond)

		// Should have ~1.5 tokens refilled
		if !limiter.Allow(ctx, "test-key") {
			t.Error("should be allowed after refill")
		}
	})
}

func TestRateLimiterWait(t *testing.T) {
	t.Run("waits for token availability", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    1,
			Interval: 100 * time.Millisecond,
		})

		ctx := context.Background()

		// Exhaust burst
		limiter.Allow(ctx, "test-key")

		// Wait should block until token available
		start := time.Now()
		err := limiter.Wait(ctx, "test-key")
		duration := time.Since(start)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Should have waited ~10ms for 1 token refill
		if duration < 5*time.Millisecond {
			t.Errorf("should have waited for refill, duration: %v", duration)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		// Exhaust burst
		limiter.Allow(ctx, "test-key")

		// Wait should fail due to context timeout
		err := limiter.Wait(ctx, "test-key")
		if err == nil {
			t.Error("expected context deadline exceeded error")
		}
		if ctx.Err() == nil {
			t.Error("context should be cancelled")
		}
	})
}

func TestRateLimiterTake(t *testing.T) {
	t.Parallel()
	t.Run("takes multiple tokens at once", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})

		ctx := context.Background()

		// Take 5 tokens
		if !limiter.Take(ctx, "test-key", 5) {
			t.Error("should allow taking 5 tokens")
		}

		// Take 5 more tokens
		if !limiter.Take(ctx, "test-key", 5) {
			t.Error("should allow taking 5 more tokens")
		}

		// Should be exhausted
		if limiter.Take(ctx, "test-key", 1) {
			t.Error("should be exhausted")
		}
	})

	t.Run("rejects when insufficient tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    5,
			Interval: time.Second,
		})

		ctx := context.Background()

		// Try to take more than burst
		if limiter.Take(ctx, "test-key", 10) {
			t.Error("should reject request exceeding burst")
		}

		// Burst should still be available
		if !limiter.Take(ctx, "test-key", 5) {
			t.Error("burst should still be available after rejected request")
		}
	})
}

func TestRateLimiterKeyFunc(t *testing.T) {
	type contextKey string
	const userIDKey contextKey = "user_id"

	t.Run("uses custom key function", func(t *testing.T) {
		limiter := New(Config{
			Rate:     2,
			Burst:    2,
			Interval: time.Second,
			KeyFunc: func(ctx context.Context, _ string) string {
				userID := ctx.Value(userIDKey)
				if userID == nil {
					return "anonymous"
				}
				//nolint:errcheck // type assertion safe here
				return userID.(string)
			},
		})

		// Use context with user_id
		ctx1 := context.WithValue(context.Background(), userIDKey, "user1")
		ctx2 := context.WithValue(context.Background(), userIDKey, "user2")

		// Exhaust user1's quota
		limiter.Allow(ctx1, "")
		limiter.Allow(ctx1, "")

		// user2 should have full quota
		if !limiter.Allow(ctx2, "") {
			t.Error("user2 should have independent quota")
		}
	})
}

func TestRateLimiterConcurrent(t *testing.T) {
	t.Run("handles concurrent requests safely", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		ctx := context.Background()
		allowed := atomic.Int32{}
		denied := atomic.Int32{}

		var wg sync.WaitGroup
		for i := 0; i < 200; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if limiter.Allow(ctx, "test-key") {
					allowed.Add(1)
				} else {
					denied.Add(1)
				}
			}()
		}

		wg.Wait()

		// Should allow approximately burst amount (allow ±1 due to concurrent timing)
		allowedCount := int(allowed.Load())
		deniedCount := int(denied.Load())

		if allowedCount < 99 || allowedCount > 101 {
			t.Errorf("allowed = %d, want ~100 (99-101)", allowedCount)
		}
		if deniedCount < 99 || deniedCount > 101 {
			t.Errorf("denied = %d, want ~100 (99-101)", deniedCount)
		}
		if allowedCount+deniedCount != 200 {
			t.Errorf("total = %d, want 200", allowedCount+deniedCount)
		}
	})
}

func TestRateLimiterCallbacks(t *testing.T) {
	t.Run("calls OnLimit callback", func(t *testing.T) {
		limitedKeys := make(chan string, 10)

		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
			OnLimit: func(ctx context.Context, key string) {
				limitedKeys <- key
			},
		})

		ctx := context.Background()

		// Exhaust quota
		limiter.Allow(ctx, "test-key")

		// This should trigger OnLimit
		limiter.Allow(ctx, "test-key")

		select {
		case key := <-limitedKeys:
			if key != "test-key" {
				t.Errorf("OnLimit called with key = %v, want test-key", key)
			}
		case <-time.After(100 * time.Millisecond):
			t.Error("OnLimit callback not called")
		}
	})
}

func TestRateLimiterDefaults(t *testing.T) {
	t.Parallel()
	t.Run("applies default configuration", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{})

		ctx := context.Background()

		// Should have some default rate
		allowed := 0
		for i := 0; i < 1000; i++ {
			if limiter.Allow(ctx, "test-key") {
				allowed++
			}
		}

		if allowed == 0 {
			t.Error("default config should allow some requests")
		}
		if allowed == 1000 {
			t.Error("default config should have some limit")
		}
	})
}

// TestMemoryStore tests the in-memory Store implementation.
//
//nolint:gocyclo // test function with many subtests
func TestMemoryStore(t *testing.T) {
	t.Run("AtomicUpdate creates new state", func(t *testing.T) {
		store := NewMemoryStore()
		ctx := context.Background()

		state, err := store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			if s != nil {
				t.Error("expected nil state for new key")
			}
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if state == nil || state.Tokens != 10 {
			t.Errorf("expected state with 10 tokens, got %v", state)
		}
	})

	t.Run("AtomicUpdate modifies existing state", func(t *testing.T) {
		store := NewMemoryStore()
		ctx := context.Background()

		// Create initial state
		//nolint:errcheck // test setup
		_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		// Modify state
		state, err := store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			if s == nil || s.Tokens != 10 {
				t.Errorf("expected existing state with 10 tokens, got %v", s)
			}
			return &BucketState{Tokens: 5, LastRefill: s.LastRefill}
		})

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if state == nil || state.Tokens != 5 {
			t.Errorf("expected state with 5 tokens, got %v", state)
		}
	})

	t.Run("AtomicUpdate handles concurrent access", func(t *testing.T) {
		store := NewMemoryStore()
		ctx := context.Background()

		// Create initial state with 100 tokens
		//nolint:errcheck // test setup
		_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 100, LastRefill: time.Now()}
		})

		// Concurrently decrement tokens
		var wg sync.WaitGroup
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				//nolint:errcheck // test setup
				_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
					if s == nil {
						return nil
					}
					return &BucketState{Tokens: s.Tokens - 1, LastRefill: s.LastRefill}
				})
			}()
		}
		wg.Wait()

		// Verify final state
		//nolint:errcheck // test verification
		state, _ := store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return s
		})

		if state == nil || state.Tokens != 0 {
			t.Errorf("expected 0 tokens after 100 decrements, got %v", state.Tokens)
		}
	})

	t.Run("Delete removes bucket", func(t *testing.T) {
		store := NewMemoryStore()
		ctx := context.Background()

		// Create state
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		// Delete
		err := store.Delete(ctx, "test-key")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Verify deleted
		//nolint:errcheck // test verification
		state, _ := store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			if s != nil {
				t.Error("expected nil state after delete")
			}
			return nil
		})

		if state != nil {
			t.Errorf("expected nil state, got %v", state)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		store := NewMemoryStore()
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if err == nil {
			t.Error("expected context cancelled error")
		}
	})
}

// TestCustomStore tests using a custom Store implementation.
func TestCustomStore(t *testing.T) {
	t.Run("uses custom store", func(t *testing.T) {
		customStore := &mockStore{
			states: make(map[string]*BucketState),
		}

		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
			Store:    customStore,
		})

		ctx := context.Background()

		// Should work with custom store
		for i := 0; i < 5; i++ {
			if !limiter.Allow(ctx, "test-key") {
				t.Errorf("request %d should be allowed", i+1)
			}
		}

		// Should be blocked
		if limiter.Allow(ctx, "test-key") {
			t.Error("6th request should be blocked")
		}

		// Verify store was used
		if customStore.updateCalls == 0 {
			t.Error("custom store was not used")
		}
	})
}

// TestFailOpen tests the FailOpen configuration.
func TestFailOpen(t *testing.T) {
	t.Run("denies on error when FailOpen is false", func(t *testing.T) {
		failingStore := &mockStore{
			err: errors.New("storage error"),
		}

		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: false,
		})

		ctx := context.Background()

		// Should deny due to storage error
		if limiter.Allow(ctx, "test-key") {
			t.Error("should deny when FailOpen is false and storage fails")
		}
	})

	t.Run("allows on error when FailOpen is true", func(t *testing.T) {
		failingStore := &mockStore{
			err: errors.New("storage error"),
		}

		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: true,
		})

		ctx := context.Background()

		// Should allow due to FailOpen
		if !limiter.Allow(ctx, "test-key") {
			t.Error("should allow when FailOpen is true and storage fails")
		}
	})
}

// mockStore is a test implementation of Store.
type mockStore struct {
	states      map[string]*BucketState
	err         error
	mu          sync.Mutex
	updateCalls int
}

func (m *mockStore) AtomicUpdate(ctx context.Context, key string, updateFn func(*BucketState) *BucketState) (*BucketState, error) {
	if m.err != nil {
		return nil, m.err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.updateCalls++

	current := m.states[key]
	newState := updateFn(current)
	if newState != nil {
		if m.states == nil {
			m.states = make(map[string]*BucketState)
		}
		m.states[key] = newState
	}

	return newState, nil
}

func (m *mockStore) Delete(ctx context.Context, key string) error {
	if m.err != nil {
		return m.err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.states, key)
	return nil
}

func (m *mockStore) Get(ctx context.Context, key string) (*BucketState, error) {
	if m.err != nil {
		return nil, m.err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.states[key]
	if state == nil {
		return nil, nil
	}

	// Return a copy
	return &BucketState{
		Tokens:     state.Tokens,
		LastRefill: state.LastRefill,
	}, nil
}

func (m *mockStore) Close() error {
	return nil
}

// TestMemoryStoreGet tests the Get method.
func TestMemoryStoreGet(t *testing.T) {
	t.Run("returns nil for non-existent key", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		state, err := store.Get(ctx, "non-existent")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if state != nil {
			t.Errorf("expected nil state, got %v", state)
		}
	})

	t.Run("returns copy of existing state", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create state
		now := time.Now()
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: now}
		})

		// Get state
		state, err := store.Get(ctx, "test-key")
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if state == nil {
			t.Fatal("expected non-nil state")
		}
		if state.Tokens != 10 {
			t.Errorf("expected 10 tokens, got %v", state.Tokens)
		}

		// Modify the returned state and verify original is unchanged
		state.Tokens = 999

		state2, _ := store.Get(ctx, "test-key") //nolint:errcheck // test helper
		if state2.Tokens != 10 {
			t.Errorf("original state was modified, got %v tokens", state2.Tokens)
		}
	})
}

// TestMemoryStoreClose tests the Close method.
func TestMemoryStoreClose(t *testing.T) {
	t.Run("close stops cleanup goroutine", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithCleanupInterval(10 * time.Millisecond),
		)
		ctx := context.Background()

		// Create some state
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		// Close the store
		err := store.Close()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Operations should return error after close
		_, err = store.AtomicUpdate(ctx, "test-key", func(s *BucketState) *BucketState {
			return s
		})
		if !errors.Is(err, ErrStoreClosed) {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})

	t.Run("double close is safe", func(t *testing.T) {
		store := NewMemoryStore()

		err1 := store.Close()
		err2 := store.Close()

		if err1 != nil || err2 != nil {
			t.Errorf("double close should be safe, got err1=%v, err2=%v", err1, err2)
		}
	})
}

// TestMemoryStoreKeyLimit tests the maximum key limit.
func TestMemoryStoreKeyLimit(t *testing.T) {
	t.Run("rejects new keys when limit reached", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithMaxKeys(5),
			WithCleanupInterval(0), // Disable cleanup for this test
		)
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create 5 keys (at limit)
		for i := 0; i < 5; i++ {
			_, err := store.AtomicUpdate(ctx, fmt.Sprintf("key-%d", i), func(s *BucketState) *BucketState {
				return &BucketState{Tokens: 10, LastRefill: time.Now()}
			})
			if err != nil {
				t.Errorf("unexpected error creating key %d: %v", i, err)
			}
		}

		if store.KeyCount() != 5 {
			t.Errorf("expected 5 keys, got %d", store.KeyCount())
		}

		// Try to create 6th key
		_, err := store.AtomicUpdate(ctx, "key-6", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})
		if !errors.Is(err, ErrKeyLimitExceeded) {
			t.Errorf("expected ErrKeyLimitExceeded, got %v", err)
		}
	})

	t.Run("allows new keys after deletion", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithMaxKeys(3),
			WithCleanupInterval(0),
		)
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create 3 keys
		for i := 0; i < 3; i++ {
			//nolint:errcheck // test setup
			_, _ = store.AtomicUpdate(ctx, fmt.Sprintf("key-%d", i), func(s *BucketState) *BucketState {
				return &BucketState{Tokens: 10, LastRefill: time.Now()}
			})
		}

		// Delete one key
		_ = store.Delete(ctx, "key-1") //nolint:errcheck // test setup

		if store.KeyCount() != 2 {
			t.Errorf("expected 2 keys after delete, got %d", store.KeyCount())
		}

		// Should now be able to create a new key
		_, err := store.AtomicUpdate(ctx, "key-new", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})
		if err != nil {
			t.Errorf("expected to create new key after delete, got %v", err)
		}
	})
}

// TestMemoryStoreTTLCleanup tests TTL-based cleanup.
func TestMemoryStoreTTLCleanup(t *testing.T) {
	t.Run("cleans up stale entries", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithCleanupInterval(50*time.Millisecond),
			WithEntryTTL(100*time.Millisecond),
		)
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create a key
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "stale-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if store.KeyCount() != 1 {
			t.Errorf("expected 1 key, got %d", store.KeyCount())
		}

		// Wait for TTL + cleanup interval
		time.Sleep(200 * time.Millisecond)

		// Key should be cleaned up
		if store.KeyCount() != 0 {
			t.Errorf("expected 0 keys after cleanup, got %d", store.KeyCount())
		}
	})

	t.Run("does not clean up active entries", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithCleanupInterval(50*time.Millisecond),
			WithEntryTTL(150*time.Millisecond),
		)
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create a key
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "active-key", func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		// Keep accessing the key to prevent expiry
		for i := 0; i < 5; i++ {
			time.Sleep(40 * time.Millisecond)
			//nolint:errcheck,gosec // test setup
			_, _ = store.AtomicUpdate(ctx, "active-key", func(s *BucketState) *BucketState {
				return s
			})
		}

		// Key should still exist
		if store.KeyCount() != 1 {
			t.Errorf("expected 1 key (still active), got %d", store.KeyCount())
		}
	})
}

// TestMemoryStoreHealthCheck tests the HealthCheck method.
func TestMemoryStoreHealthCheck(t *testing.T) {
	t.Run("returns nil when healthy", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()

		err := store.HealthCheck(context.Background())
		if err != nil {
			t.Errorf("expected healthy, got %v", err)
		}
	})

	t.Run("returns error when closed", func(t *testing.T) {
		store := NewMemoryStore()
		_ = store.Close()

		err := store.HealthCheck(context.Background())
		if !errors.Is(err, ErrStoreClosed) {
			t.Errorf("expected ErrStoreClosed, got %v", err)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.HealthCheck(ctx)
		if err == nil {
			t.Error("expected context error")
		}
	})
}

// TestRateLimiterClose tests the Close method on RateLimiter.
func TestRateLimiterClose(t *testing.T) {
	t.Run("closes underlying store", func(t *testing.T) {
		store := NewMemoryStore()
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})

		err := limiter.Close()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}

		// Store should be closed
		err = store.HealthCheck(context.Background())
		if !errors.Is(err, ErrStoreClosed) {
			t.Errorf("expected store to be closed, got %v", err)
		}
	})
}

// TestRateLimiterTakeValidation tests Take() input validation.
func TestRateLimiterTakeValidation(t *testing.T) {
	t.Run("rejects zero tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		if limiter.Take(context.Background(), "test-key", 0) {
			t.Error("should reject zero tokens")
		}
	})

	t.Run("rejects negative tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		if limiter.Take(context.Background(), "test-key", -5) {
			t.Error("should reject negative tokens")
		}
	})

	t.Run("rejects tokens exceeding MaxTokensPerRequest", func(t *testing.T) {
		limiter := New(Config{
			Rate:                10,
			Burst:               10,
			Interval:            time.Second,
			MaxTokensPerRequest: 50,
		})
		defer func() { _ = limiter.Close() }()

		if limiter.Take(context.Background(), "test-key", 51) {
			t.Error("should reject tokens exceeding MaxTokensPerRequest")
		}
	})

	t.Run("allows tokens within MaxTokensPerRequest", func(t *testing.T) {
		limiter := New(Config{
			Rate:                100,
			Burst:               100,
			Interval:            time.Second,
			MaxTokensPerRequest: 50,
		})
		defer func() { _ = limiter.Close() }()

		if !limiter.Take(context.Background(), "test-key", 50) {
			t.Error("should allow tokens within MaxTokensPerRequest")
		}
	})
}

// mockMetrics is a test implementation of Metrics.
type mockMetrics struct {
	lastOperation string
	mu            sync.Mutex
	allowCount    int
	denyCount     int
	errorCount    int
	latencyCount  int
}

func (m *mockMetrics) OnAllow(ctx context.Context, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.allowCount++
}

func (m *mockMetrics) OnDeny(ctx context.Context, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.denyCount++
}

func (m *mockMetrics) OnError(ctx context.Context, key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorCount++
}

func (m *mockMetrics) OnStoreLatency(ctx context.Context, operation string, duration time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencyCount++
	m.lastOperation = operation
}

// TestRateLimiterMetrics tests metrics collection.
func TestRateLimiterMetrics(t *testing.T) {
	t.Run("records allow metrics", func(t *testing.T) {
		metrics := &mockMetrics{}
		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
			Metrics:  metrics,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		for i := 0; i < 3; i++ {
			limiter.Allow(ctx, "test-key")
		}

		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		if metrics.allowCount != 3 {
			t.Errorf("expected 3 allow events, got %d", metrics.allowCount)
		}
		if metrics.latencyCount != 3 {
			t.Errorf("expected 3 latency events, got %d", metrics.latencyCount)
		}
	})

	t.Run("records deny metrics", func(t *testing.T) {
		metrics := &mockMetrics{}
		limiter := New(Config{
			Rate:     2,
			Burst:    2,
			Interval: time.Second,
			Metrics:  metrics,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		// Exhaust burst
		limiter.Allow(ctx, "test-key")
		limiter.Allow(ctx, "test-key")
		// This should be denied
		limiter.Allow(ctx, "test-key")
		limiter.Allow(ctx, "test-key")

		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		if metrics.allowCount != 2 {
			t.Errorf("expected 2 allow events, got %d", metrics.allowCount)
		}
		if metrics.denyCount != 2 {
			t.Errorf("expected 2 deny events, got %d", metrics.denyCount)
		}
	})

	t.Run("records error metrics", func(t *testing.T) {
		metrics := &mockMetrics{}
		failingStore := &mockStore{err: errors.New("storage error")}

		limiter := New(Config{
			Rate:     5,
			Burst:    5,
			Interval: time.Second,
			Store:    failingStore,
			Metrics:  metrics,
			FailOpen: true,
		})
		defer func() { _ = limiter.Close() }()

		limiter.Allow(context.Background(), "test-key")

		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		if metrics.errorCount != 1 {
			t.Errorf("expected 1 error event, got %d", metrics.errorCount)
		}
	})

	t.Run("records latency for Take operation", func(t *testing.T) {
		metrics := &mockMetrics{}
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Metrics:  metrics,
		})
		defer func() { _ = limiter.Close() }()

		limiter.Take(context.Background(), "test-key", 5)

		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		if metrics.lastOperation != "take" {
			t.Errorf("expected operation 'take', got %s", metrics.lastOperation)
		}
	})
}

// TestMemoryStoreClear tests the Clear method.
func TestMemoryStoreClear(t *testing.T) {
	t.Run("removes all buckets", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithCleanupInterval(0))
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create multiple keys
		for i := 0; i < 10; i++ {
			//nolint:errcheck,gosec // test setup
			_, _ = store.AtomicUpdate(ctx, fmt.Sprintf("key-%d", i), func(s *BucketState) *BucketState {
				return &BucketState{Tokens: 10, LastRefill: time.Now()}
			})
		}

		if store.KeyCount() != 10 {
			t.Errorf("expected 10 keys, got %d", store.KeyCount())
		}

		// Clear
		store.Clear()

		if store.KeyCount() != 0 {
			t.Errorf("expected 0 keys after clear, got %d", store.KeyCount())
		}
	})
}

// TestMemoryStoreKeyLengthLimit tests the key length validation.
func TestMemoryStoreKeyLengthLimit(t *testing.T) {
	t.Run("rejects keys exceeding max length", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithMaxKeyLength(10), WithCleanupInterval(0))
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Key that's too long
		longKey := "12345678901" // 11 chars
		_, err := store.AtomicUpdate(ctx, longKey, func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if !errors.Is(err, ErrKeyTooLong) {
			t.Errorf("expected ErrKeyTooLong, got %v", err)
		}
	})

	t.Run("allows keys within max length", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithMaxKeyLength(10), WithCleanupInterval(0))
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Key that fits
		shortKey := "1234567890" // 10 chars
		_, err := store.AtomicUpdate(ctx, shortKey, func(s *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}
	})

	t.Run("Get rejects keys exceeding max length", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithMaxKeyLength(10), WithCleanupInterval(0))
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		longKey := "12345678901" // 11 chars
		_, err := store.Get(ctx, longKey)

		if !errors.Is(err, ErrKeyTooLong) {
			t.Errorf("expected ErrKeyTooLong, got %v", err)
		}
	})
}

// TestRateLimiterHealthCheck tests the HealthCheck method on RateLimiter.
func TestRateLimiterHealthCheck(t *testing.T) {
	t.Run("returns nil when store is healthy", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.HealthCheck(context.Background())
		if err != nil {
			t.Errorf("expected nil, got %v", err)
		}
	})

	t.Run("returns error when rate limiter is closed", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})

		_ = limiter.Close()

		err := limiter.HealthCheck(context.Background())
		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got %v", err)
		}
	})

	t.Run("returns nil for store without HealthChecker", func(t *testing.T) {
		// mockStore doesn't implement HealthChecker
		store := &mockStore{}
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.HealthCheck(context.Background())
		if err != nil {
			t.Errorf("expected nil for non-HealthChecker store, got %v", err)
		}
	})

	t.Run("logs warning once for store without HealthChecker", func(t *testing.T) {
		var logBuffer bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

		// mockStore doesn't implement HealthChecker
		store := &mockStore{}
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
			Logger:   logger,
		})
		defer func() { _ = limiter.Close() }()

		// First call should log warning
		_ = limiter.HealthCheck(context.Background()) //nolint:errcheck // testing log behavior
		logOutput := logBuffer.String()
		if !strings.Contains(logOutput, "Store does not implement HealthChecker") {
			t.Errorf("expected warning log on first call, got: %s", logOutput)
		}
		if !strings.Contains(logOutput, "*ratelimit.mockStore") {
			t.Errorf("expected store type in log, got: %s", logOutput)
		}

		// Reset buffer and call again - should NOT log again (sync.Once)
		logBuffer.Reset()
		_ = limiter.HealthCheck(context.Background()) //nolint:errcheck // testing log behavior
		secondOutput := logBuffer.String()
		if strings.Contains(secondOutput, "Store does not implement HealthChecker") {
			t.Errorf("expected no warning on second call, got: %s", secondOutput)
		}
	})
}

// TestConfigUpperBounds tests that config values are capped.
func TestConfigUpperBounds(t *testing.T) {
	t.Parallel()
	t.Run("caps rate to MaxRate", func(t *testing.T) {
		t.Parallel()
		config := Config{
			Rate: MaxRate + 1,
		}
		config.setDefaults()

		if config.Rate != MaxRate {
			t.Errorf("expected rate to be capped at %d, got %d", MaxRate, config.Rate)
		}
	})

	t.Run("caps burst to MaxBurst", func(t *testing.T) {
		config := Config{
			Rate:  100,
			Burst: MaxBurst + 1,
		}
		config.setDefaults()

		if config.Burst != MaxBurst {
			t.Errorf("expected burst to be capped at %d, got %d", MaxBurst, config.Burst)
		}
	})

	t.Run("caps interval to MaxInterval", func(t *testing.T) {
		config := Config{
			Rate:     100,
			Interval: MaxInterval + time.Hour,
		}
		config.setDefaults()

		if config.Interval != MaxInterval {
			t.Errorf("expected interval to be capped at %v, got %v", MaxInterval, config.Interval)
		}
	})

	t.Run("floors interval to MinInterval", func(t *testing.T) {
		t.Parallel()
		config := Config{
			Rate:     100,
			Interval: time.Nanosecond, // Below MinInterval
		}
		config.setDefaults()

		if config.Interval != MinInterval {
			t.Errorf("expected interval to be floored at %v, got %v", MinInterval, config.Interval)
		}
	})

	t.Run("floors sub-microsecond interval to MinInterval", func(t *testing.T) {
		t.Parallel()
		config := Config{
			Rate:     100,
			Interval: 500 * time.Microsecond, // Below MinInterval
		}
		config.setDefaults()

		if config.Interval != MinInterval {
			t.Errorf("expected interval to be floored at %v, got %v", MinInterval, config.Interval)
		}
	})

	t.Run("preserves interval at MinInterval", func(t *testing.T) {
		t.Parallel()
		config := Config{
			Rate:     100,
			Interval: MinInterval,
		}
		config.setDefaults()

		if config.Interval != MinInterval {
			t.Errorf("expected interval to remain %v, got %v", MinInterval, config.Interval)
		}
	})
}

// TestWaitLatencyMetrics tests that Wait() records latency metrics.
func TestWaitLatencyMetrics(t *testing.T) {
	t.Run("records latency on success", func(t *testing.T) {
		metrics := &mockMetrics{}
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Metrics:  metrics,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.Wait(context.Background(), "test-key")
		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		metrics.mu.Lock()
		defer metrics.mu.Unlock()
		if metrics.lastOperation != "wait" {
			t.Errorf("expected operation 'wait', got %s", metrics.lastOperation)
		}
	})
}

// TestOnLimitCallbackWithContext tests that OnLimit receives context.
func TestOnLimitCallbackWithContext(t *testing.T) {
	t.Run("passes context to OnLimit callback", func(t *testing.T) {
		type ctxKey string
		const testKey ctxKey = "test"
		var receivedCtxValue interface{}

		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
			OnLimit: func(ctx context.Context, key string) {
				receivedCtxValue = ctx.Value(testKey)
			},
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.WithValue(context.Background(), testKey, "expected-value")

		// First request consumes token
		limiter.Allow(ctx, "test-key")

		// Second request triggers OnLimit
		limiter.Allow(ctx, "test-key")

		if receivedCtxValue != "expected-value" {
			t.Errorf("expected context value 'expected-value', got %v", receivedCtxValue)
		}
	})
}

// TestCallbackPanicRecovery tests that panics in callbacks are recovered.
func TestCallbackPanicRecovery(t *testing.T) {
	t.Run("recovers from panic in OnLimit callback", func(t *testing.T) {
		var logBuffer bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
			Logger:   logger,
			OnLimit: func(ctx context.Context, key string) {
				panic("test panic")
			},
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// First request consumes token
		limiter.Allow(ctx, "test-key")

		// Second request triggers OnLimit which panics
		// Should not crash
		allowed := limiter.Allow(ctx, "test-key")

		if allowed {
			t.Error("expected request to be denied")
		}

		// Verify panic was logged
		logOutput := logBuffer.String()
		if !strings.Contains(logOutput, "callback panic") {
			t.Errorf("expected panic log, got: %s", logOutput)
		}
	})
}

// TestRateLimiterClosedBehavior tests behavior after Close() is called.
func TestRateLimiterClosedBehavior(t *testing.T) {
	t.Run("Allow returns false after Close", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		_ = limiter.Close()

		if limiter.Allow(context.Background(), "test") {
			t.Error("expected Allow to return false after Close")
		}
	})

	t.Run("Wait returns error after Close", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		_ = limiter.Close()

		err := limiter.Wait(context.Background(), "test")
		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got %v", err)
		}
	})

	t.Run("Take returns false after Close", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		_ = limiter.Close()

		if limiter.Take(context.Background(), "test", 1) {
			t.Error("expected Take to return false after Close")
		}
	})

	t.Run("double Close is safe", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		if err := limiter.Close(); err != nil {
			t.Errorf("first Close failed: %v", err)
		}

		// Second close should not panic or error
		if err := limiter.Close(); err != nil {
			t.Errorf("second Close failed: %v", err)
		}
	})
}

// TestDeleteKeyLengthLimit tests that Delete() respects key length limits.
func TestDeleteKeyLengthLimit(t *testing.T) {
	t.Run("Delete rejects keys exceeding max length", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithMaxKeyLength(10), WithCleanupInterval(0))
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		longKey := "12345678901" // 11 chars
		err := store.Delete(ctx, longKey)

		if !errors.Is(err, ErrKeyTooLong) {
			t.Errorf("expected ErrKeyTooLong, got %v", err)
		}
	})
}

// TestMaxTokensPerRequestCap tests that MaxTokensPerRequest is capped.
func TestMaxTokensPerRequestCap(t *testing.T) {
	t.Run("caps MaxTokensPerRequest to maximum", func(t *testing.T) {
		config := Config{
			Rate:                100,
			Burst:               100,
			MaxTokensPerRequest: MaxMaxTokensPerRequest + 1,
		}
		config.setDefaults()

		if config.MaxTokensPerRequest != MaxMaxTokensPerRequest {
			t.Errorf("expected MaxTokensPerRequest to be capped at %d, got %d",
				MaxMaxTokensPerRequest, config.MaxTokensPerRequest)
		}
	})
}

// TestConcurrentWaitOnSameKey tests concurrent Wait() calls on the same key.
func TestConcurrentWaitOnSameKey(t *testing.T) {
	t.Run("handles concurrent Wait calls on same key", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		key := "concurrent-test"

		var wg sync.WaitGroup
		errors := make(chan error, 20)

		// Launch concurrent Wait calls
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := limiter.Wait(ctx, key)
				if err != nil {
					errors <- err
				}
			}()
		}

		wg.Wait()
		close(errors)

		// All should eventually succeed
		for err := range errors {
			t.Errorf("Wait() failed: %v", err)
		}
	})
}

// alwaysEmptyStore is a mock store that always returns 0 tokens,
// forcing Wait() to iterate until timeout.
type alwaysEmptyStore struct {
	callCount int64
}

func (s *alwaysEmptyStore) AtomicUpdate(ctx context.Context, key string, updateFn func(*BucketState) *BucketState) (*BucketState, error) {
	atomic.AddInt64(&s.callCount, 1)
	// Always return a state with 0 tokens
	state := &BucketState{
		Tokens:     0,
		LastRefill: time.Now(),
	}
	return state, nil
}

func (s *alwaysEmptyStore) Get(ctx context.Context, key string) (*BucketState, error) {
	return &BucketState{
		Tokens:     0,
		LastRefill: time.Now(),
	}, nil
}

func (s *alwaysEmptyStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *alwaysEmptyStore) Close() error {
	return nil
}

// TestWaitTimeoutLimits tests that Wait() respects iteration and time limits.
func TestWaitTimeoutLimits(t *testing.T) {
	// Note: Testing the full iteration limit (10,000 iterations) would take too long
	// (at least 100 seconds with 10ms minimum wait). Instead we test:
	// 1. Context cancellation works properly
	// 2. Wait keeps trying when tokens are unavailable
	// 3. HealthCheck respects closed state

	t.Run("Wait respects context cancellation during token wait", func(t *testing.T) {
		store := &alwaysEmptyStore{}
		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := limiter.Wait(ctx, "test")
		elapsed := time.Since(start)

		if err == nil {
			t.Error("expected error, got nil")
		}

		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}

		// Should have exited quickly due to context cancellation
		if elapsed > 500*time.Millisecond {
			t.Errorf("Wait took too long despite context cancellation: %v", elapsed)
		}

		// Verify iterations happened
		calls := atomic.LoadInt64(&store.callCount)
		if calls == 0 {
			t.Error("expected at least one iteration")
		}
	})

	t.Run("Wait eventually succeeds when tokens become available", func(t *testing.T) {
		// Use real memory store with low rate to force waiting
		limiter := New(Config{
			Rate:     10,
			Burst:    1,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// First call consumes the burst token
		if !limiter.Allow(ctx, "test") {
			t.Fatal("first Allow should succeed")
		}

		// Now Wait should eventually succeed when token refills
		start := time.Now()
		err := limiter.Wait(ctx, "test")
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("Wait should eventually succeed, got %v", err)
		}

		// Should take at least some time for token to refill
		// With Rate=10/sec, refill time is ~100ms per token
		if elapsed < 50*time.Millisecond {
			t.Errorf("Wait returned too quickly: %v", elapsed)
		}
	})

	t.Run("Wait returns immediately when tokens are available", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		start := time.Now()
		err := limiter.Wait(context.Background(), "test")
		elapsed := time.Since(start)

		if err != nil {
			t.Errorf("Wait should succeed, got %v", err)
		}

		// Should return very quickly when tokens are available
		if elapsed > 50*time.Millisecond {
			t.Errorf("Wait took too long when tokens available: %v", elapsed)
		}
	})

	t.Run("HealthCheck returns ErrRateLimiterClosed after Close", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		_ = limiter.Close()

		err := limiter.HealthCheck(context.Background())
		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got %v", err)
		}
	})
}

// TestWaitEdgeCases tests Wait() edge case behavior (H3).
func TestWaitEdgeCases(t *testing.T) {
	t.Run("Wait returns ErrWaitTimeout when iterations exceeded", func(t *testing.T) {
		// Create a store that never allows tokens to be consumed
		// but responds to AtomicUpdate calls
		store := &neverAllowStore{}
		limiter := New(Config{
			Rate:     1000000, // Very high rate so tokens should be available
			Burst:    1000000,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Create a context with long timeout so we hit iteration limit first
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
		defer cancel()

		err := limiter.Wait(ctx, "test")

		if !errors.Is(err, ErrWaitTimeout) {
			t.Errorf("expected ErrWaitTimeout, got %v", err)
		}
	})

	t.Run("Wait handles very low rate gracefully", func(t *testing.T) {
		// Very low rate (1 per minute) means long wait between tokens
		limiter := New(Config{
			Rate:     1, // 1 token per minute
			Burst:    1,
			Interval: time.Minute,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// First request should succeed (uses burst)
		if !limiter.Allow(ctx, "test") {
			t.Error("first request should succeed using burst")
		}

		// Wait with short timeout should fail (rate is too slow)
		ctx2, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		err := limiter.Wait(ctx2, "test")
		if err == nil {
			t.Error("expected timeout error due to slow rate")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}
	})

	t.Run("Wait with FailOpen on store error", func(t *testing.T) {
		failingStore := &mockStore{err: errors.New("storage error")}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: true,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.Wait(context.Background(), "test")
		if err != nil {
			t.Errorf("expected nil error with FailOpen, got %v", err)
		}
	})

	t.Run("Wait with FailClosed on store error uses backoff", func(t *testing.T) {
		failingStore := &mockStore{err: errors.New("storage error")}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: false,
		})
		defer func() { _ = limiter.Close() }()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		start := time.Now()
		err := limiter.Wait(ctx, "test")
		elapsed := time.Since(start)

		// Should fail with context deadline
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected context.DeadlineExceeded, got %v", err)
		}

		// Should have waited approximately the timeout duration
		if elapsed < 150*time.Millisecond {
			t.Errorf("Wait returned too quickly: %v", elapsed)
		}
	})
}

// neverAllowStore always returns state with 0 tokens and high refill rate
// but the refill calculation never results in sufficient tokens due to timing.
type neverAllowStore struct {
	iterations int64
}

func (s *neverAllowStore) AtomicUpdate(ctx context.Context, key string, updateFn func(*BucketState) *BucketState) (*BucketState, error) {
	atomic.AddInt64(&s.iterations, 1)
	// Return a state where tokens are never enough (0.5 tokens)
	// This will cause Wait() to keep iterating
	return &BucketState{
		Tokens:     0.5,
		LastRefill: time.Now(),
	}, nil
}

func (s *neverAllowStore) Get(ctx context.Context, key string) (*BucketState, error) {
	return &BucketState{
		Tokens:     0.5,
		LastRefill: time.Now(),
	}, nil
}

func (s *neverAllowStore) Delete(ctx context.Context, key string) error {
	return nil
}

func (s *neverAllowStore) Close() error {
	return nil
}

// TestMultiKeyConcurrency tests concurrent access across multiple keys (H4).
//
//nolint:gocyclo // test function with many subtests
func TestMultiKeyConcurrency(t *testing.T) {
	t.Run("independent key limits under high concurrency", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		numKeys := 100
		requestsPerKey := 20

		// Track allowed/denied per key
		type keyStats struct {
			allowed atomic.Int32
			denied  atomic.Int32
		}
		stats := make([]keyStats, numKeys)

		var wg sync.WaitGroup
		for keyIdx := 0; keyIdx < numKeys; keyIdx++ {
			for req := 0; req < requestsPerKey; req++ {
				wg.Add(1)
				go func(k int) {
					defer wg.Done()
					key := fmt.Sprintf("key-%d", k)
					if limiter.Allow(ctx, key) {
						stats[k].allowed.Add(1)
					} else {
						stats[k].denied.Add(1)
					}
				}(keyIdx)
			}
		}

		wg.Wait()

		// Verify each key had independent rate limiting
		for i := 0; i < numKeys; i++ {
			allowed := stats[i].allowed.Load()
			denied := stats[i].denied.Load()
			total := allowed + denied

			if total != int32(requestsPerKey) {
				t.Errorf("key-%d: expected %d total requests, got %d", i, requestsPerKey, total)
			}

			// Each key should allow approximately burst (10) requests
			// Allow some variance due to timing
			if allowed < 9 || allowed > 11 {
				t.Errorf("key-%d: expected ~10 allowed, got %d", i, allowed)
			}
		}
	})

	t.Run("concurrent key creation stress test", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(
			WithMaxKeys(1000),
			WithCleanupInterval(0), // Disable cleanup during test
		)
		defer func() { _ = store.Close() }()

		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		numGoroutines := 50
		keysPerGoroutine := 20

		var wg sync.WaitGroup
		errors := make(chan error, numGoroutines*keysPerGoroutine)

		for g := 0; g < numGoroutines; g++ {
			wg.Add(1)
			go func(goroutineID int) {
				defer wg.Done()
				for k := 0; k < keysPerGoroutine; k++ {
					key := fmt.Sprintf("g%d-k%d", goroutineID, k)
					// Each key should allow at least one request
					if !limiter.Allow(ctx, key) {
						errors <- fmt.Errorf("first request for %s denied", key)
					}
				}
			}(g)
		}

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Error(err)
		}

		// Verify key count
		keyCount := store.KeyCount()
		expectedKeys := int64(numGoroutines * keysPerGoroutine)
		if keyCount != expectedKeys {
			t.Errorf("expected %d keys, got %d", expectedKeys, keyCount)
		}
	})

	t.Run("concurrent Wait and Allow on same key", func(t *testing.T) {
		limiter := New(Config{
			Rate:     50,
			Burst:    5,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		key := "mixed-ops"

		var wg sync.WaitGroup
		allowCount := atomic.Int32{}
		waitErrors := atomic.Int32{}

		// Launch Allow goroutines
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 10; j++ {
					if limiter.Allow(ctx, key) {
						allowCount.Add(1)
					}
				}
			}()
		}

		// Launch Wait goroutines
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
				defer cancel()
				for j := 0; j < 5; j++ {
					if err := limiter.Wait(ctx, key); err != nil {
						waitErrors.Add(1)
					}
				}
			}()
		}

		wg.Wait()

		// Just verify no panics occurred - exact counts vary with timing
		t.Logf("Allow count: %d, Wait errors: %d", allowCount.Load(), waitErrors.Load())
	})
}

// TestRefillEdgeCases tests token refill edge cases (H5).
//
//nolint:gocyclo // test function with many subtests
func TestRefillEdgeCases(t *testing.T) {
	t.Run("handles clock backwards gracefully", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create initial state with a future timestamp
		futureTime := time.Now().Add(time.Hour)
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test", func(s *BucketState) *BucketState {
			return &BucketState{
				Tokens:     5,
				LastRefill: futureTime,
			}
		})

		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Should still work - tokens shouldn't go negative
		// Even with clock backwards, existing tokens should be usable
		if !limiter.Allow(ctx, "test") {
			t.Error("should allow request with existing tokens despite clock issue")
		}
	})

	t.Run("handles very small intervals without overflow", func(t *testing.T) {
		limiter := New(Config{
			Rate:     1000,
			Burst:    100,
			Interval: time.Millisecond, // Very small interval
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Exhaust burst
		for i := 0; i < 100; i++ {
			limiter.Allow(ctx, "test")
		}

		// Wait a tiny bit for some refill
		time.Sleep(5 * time.Millisecond)

		// Should have refilled ~5 tokens
		allowed := 0
		for i := 0; i < 10; i++ {
			if limiter.Allow(ctx, "test") {
				allowed++
			}
		}

		if allowed < 3 || allowed > 10 {
			t.Errorf("expected 3-10 refilled tokens, got %d", allowed)
		}
	})

	t.Run("handles very large elapsed time capped to maxElapsed", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create state with very old timestamp (simulating system sleep/wake)
		oldTime := time.Now().Add(-24 * time.Hour)
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test", func(s *BucketState) *BucketState {
			return &BucketState{
				Tokens:     0,
				LastRefill: oldTime,
			}
		})

		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Request should succeed (tokens refilled up to burst)
		if !limiter.Allow(ctx, "test") {
			t.Error("should allow request after large elapsed time refill")
		}

		// Should be capped at burst, not infinite
		allowed := 1
		for i := 0; i < 20; i++ {
			if limiter.Allow(ctx, "test") {
				allowed++
			}
		}

		// Should allow exactly burst tokens (10)
		if allowed != 10 {
			t.Errorf("expected exactly 10 allowed (burst cap), got %d", allowed)
		}
	})

	t.Run("handles fractional token accumulation precisely", func(t *testing.T) {
		limiter := New(Config{
			Rate:     3,  // 3 tokens per second
			Burst:    10, // Can hold up to 10
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Exhaust all tokens
		for i := 0; i < 10; i++ {
			limiter.Allow(ctx, "test")
		}

		// Wait for fractional token accumulation (333ms = ~1 token)
		time.Sleep(350 * time.Millisecond)

		// Should have ~1 token
		if !limiter.Allow(ctx, "test") {
			t.Error("should have 1 token after 350ms with rate 3/s")
		}

		// Should be denied (only had ~1 token)
		if limiter.Allow(ctx, "test") {
			t.Error("should be denied - only had 1 token")
		}
	})

	t.Run("burst cap prevents token overflow", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create state at burst limit
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test", func(s *BucketState) *BucketState {
			return &BucketState{
				Tokens:     10, // At burst limit
				LastRefill: time.Now().Add(-time.Hour),
			}
		})

		limiter := New(Config{
			Rate:     1000,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Try to refill beyond burst - should be capped
		limiter.Allow(ctx, "test") // Triggers refill calculation

		state, _ := store.Get(ctx, "test") //nolint:errcheck // test helper
		if state.Tokens > 10 {
			t.Errorf("tokens exceeded burst limit: %v", state.Tokens)
		}
	})

	t.Run("zero elapsed time returns same state", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Make two quick requests - the second should use cached state
		start := time.Now()
		allowed1 := limiter.Allow(ctx, "test")
		allowed2 := limiter.Allow(ctx, "test")
		elapsed := time.Since(start)

		if !allowed1 || !allowed2 {
			t.Error("first two requests should be allowed")
		}

		// Both should complete very quickly (no refill delay)
		if elapsed > 10*time.Millisecond {
			t.Errorf("requests took too long: %v", elapsed)
		}
	})

	t.Run("extreme rate and interval combo capped at burst", func(t *testing.T) {
		// HIGH-01: MaxRate with MinInterval after long sleep should not produce
		// NaN, Inf, or tokens exceeding burst.
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Create state with old timestamp to simulate long system sleep
		oldTime := time.Now().Add(-24 * time.Hour)
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test", func(s *BucketState) *BucketState {
			return &BucketState{
				Tokens:     0,
				LastRefill: oldTime,
			}
		})

		limiter := New(Config{
			Rate:     MaxRate,
			Burst:    100, // Small burst relative to rate
			Interval: MinInterval,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Should not panic and tokens should be capped at burst
		if !limiter.Allow(ctx, "test") {
			t.Error("should allow request after refill")
		}

		state, err := store.Get(ctx, "test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		// Tokens must never exceed burst (100)
		if state.Tokens > 100 {
			t.Errorf("tokens exceeded burst limit: got %v, want <= 100", state.Tokens)
		}

		// Tokens must not be NaN or Inf
		if math.IsNaN(state.Tokens) || math.IsInf(state.Tokens, 0) {
			t.Errorf("tokens is NaN or Inf: %v", state.Tokens)
		}
	})

	t.Run("refill with NaN-producing edge case stays safe", func(t *testing.T) {
		// Verify the NaN/Inf safety net in refill by testing with a
		// deliberately constructed limiter at boundary conditions.
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()
		ctx := context.Background()

		// Set up state just below burst
		//nolint:errcheck,gosec // test setup
		_, _ = store.AtomicUpdate(ctx, "test", func(s *BucketState) *BucketState {
			return &BucketState{
				Tokens:     9,
				LastRefill: time.Now().Add(-time.Minute),
			}
		})

		limiter := New(Config{
			Rate:     MaxRate,
			Burst:    10,
			Interval: MinInterval,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		// Should not panic
		if !limiter.Allow(ctx, "test") {
			t.Error("should allow request")
		}

		state, err := store.Get(ctx, "test")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if math.IsNaN(state.Tokens) || math.IsInf(state.Tokens, 0) {
			t.Errorf("tokens is NaN or Inf after refill: %v", state.Tokens)
		}

		if state.Tokens < 0 || state.Tokens > 10 {
			t.Errorf("tokens out of bounds: got %v, want [0, 10]", state.Tokens)
		}
	})
}

// TestTakeBoundaryConditions tests Take() edge cases.
//
//nolint:gocyclo // test function with many subtests
func TestTakeBoundaryConditions(t *testing.T) {
	t.Run("rejects zero tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		if limiter.Take(context.Background(), "test", 0) {
			t.Error("should reject zero tokens")
		}
	})

	t.Run("rejects negative tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		if limiter.Take(context.Background(), "test", -1) {
			t.Error("should reject negative tokens")
		}
		if limiter.Take(context.Background(), "test", -100) {
			t.Error("should reject large negative tokens")
		}
	})

	t.Run("allows exactly burst tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// Should allow taking exactly burst amount
		if !limiter.Take(context.Background(), "test", 10) {
			t.Error("should allow taking exactly burst tokens")
		}
	})

	t.Run("rejects tokens exceeding burst even if first request", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// Cannot take more than burst on first request
		if limiter.Take(context.Background(), "test", 11) {
			t.Error("should reject tokens exceeding burst")
		}
	})

	t.Run("Take at MaxTokensPerRequest boundary", func(t *testing.T) {
		limiter := New(Config{
			Rate:                1000,
			Burst:               1000,
			Interval:            time.Second,
			MaxTokensPerRequest: 100,
		})
		defer func() { _ = limiter.Close() }()

		// Exactly at limit should work
		if !limiter.Take(context.Background(), "test", 100) {
			t.Error("should allow exactly MaxTokensPerRequest tokens")
		}

		// One above limit should fail
		if limiter.Take(context.Background(), "test2", 101) {
			t.Error("should reject MaxTokensPerRequest+1 tokens")
		}
	})

	t.Run("partial consumption preserves remaining tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Take 5 tokens
		if !limiter.Take(ctx, "test", 5) {
			t.Fatal("should allow taking 5 tokens")
		}

		// Take 3 more tokens (5 remaining)
		if !limiter.Take(ctx, "test", 3) {
			t.Error("should allow taking 3 more tokens")
		}

		// Take 3 more should fail (only 2 remaining)
		if limiter.Take(ctx, "test", 3) {
			t.Error("should reject - only 2 tokens remaining")
		}

		// Take 2 should succeed (exactly 2 remaining)
		if !limiter.Take(ctx, "test", 2) {
			t.Error("should allow taking last 2 tokens")
		}
	})

	t.Run("failed Take does not consume tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    5,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Try to take more than burst (fails)
		if limiter.Take(ctx, "test", 10) {
			t.Fatal("should reject - exceeds burst")
		}

		// All 5 tokens should still be available
		if !limiter.Take(ctx, "test", 5) {
			t.Error("all tokens should still be available after failed Take")
		}
	})

	t.Run("Take returns false on closed limiter", func(t *testing.T) {
		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
		})

		_ = limiter.Close()

		if limiter.Take(context.Background(), "test", 1) {
			t.Error("Take should return false on closed limiter")
		}
	})

	t.Run("Take with store error and FailOpen", func(t *testing.T) {
		failingStore := &mockStore{err: errors.New("storage error")}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: true,
		})
		defer func() { _ = limiter.Close() }()

		// Should allow due to FailOpen
		if !limiter.Take(context.Background(), "test", 5) {
			t.Error("should allow when FailOpen and storage fails")
		}
	})

	t.Run("Take with store error and FailClosed", func(t *testing.T) {
		failingStore := &mockStore{err: errors.New("storage error")}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    failingStore,
			FailOpen: false,
		})
		defer func() { _ = limiter.Close() }()

		// Should deny due to FailClosed
		if limiter.Take(context.Background(), "test", 5) {
			t.Error("should deny when FailClosed and storage fails")
		}
	})

	t.Run("Take logs warning for excessive tokens", func(t *testing.T) {
		var logBuffer bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

		limiter := New(Config{
			Rate:                100,
			Burst:               100,
			Interval:            time.Second,
			MaxTokensPerRequest: 50,
			Logger:              logger,
		})
		defer func() { _ = limiter.Close() }()

		// Request exceeding MaxTokensPerRequest
		limiter.Take(context.Background(), "test", 100)

		// Should have logged warning
		logOutput := logBuffer.String()
		if !strings.Contains(logOutput, "excessive token request") {
			t.Errorf("expected warning log, got: %s", logOutput)
		}
	})
}

// TestStoreFailureRecovery tests store failure and recovery scenarios.
func TestStoreFailureRecovery(t *testing.T) {
	t.Run("intermittent store failures with FailOpen", func(t *testing.T) {
		store := &intermittentFailStore{
			failEvery: 3, // Fail every 3rd call
		}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    store,
			FailOpen: true,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Make several requests - some will hit failures
		allowedCount := 0
		for i := 0; i < 10; i++ {
			if limiter.Allow(ctx, "test") {
				allowedCount++
			}
		}

		// With FailOpen, all requests should be allowed
		// (successful ones consume tokens, failed ones allow through)
		if allowedCount != 10 {
			t.Errorf("expected all 10 allowed with FailOpen, got %d", allowedCount)
		}
	})

	t.Run("intermittent store failures with FailClosed", func(t *testing.T) {
		store := &intermittentFailStore{
			failEvery: 3, // Fail every 3rd call
		}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    store,
			FailOpen: false,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// Make several requests
		deniedCount := 0
		for i := 0; i < 10; i++ {
			if !limiter.Allow(ctx, "test") {
				deniedCount++
			}
		}

		// With FailClosed, failed store calls should deny
		// Expect ~3 denials (calls 3, 6, 9 fail)
		if deniedCount < 2 || deniedCount > 4 {
			t.Errorf("expected 2-4 denied with FailClosed, got %d", deniedCount)
		}
	})

	t.Run("store recovery after temporary failure", func(t *testing.T) {
		store := &recoverableStore{
			failUntilCall: 5, // First 5 calls fail
		}

		limiter := New(Config{
			Rate:     100,
			Burst:    100,
			Interval: time.Second,
			Store:    store,
			FailOpen: false,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()

		// First 5 should be denied
		for i := 0; i < 5; i++ {
			if limiter.Allow(ctx, "test") {
				t.Errorf("call %d should be denied (store failing)", i+1)
			}
		}

		// After recovery, should work
		if !limiter.Allow(ctx, "test") {
			t.Error("should allow after store recovery")
		}
	})

	t.Run("concurrent access during store failure", func(t *testing.T) {
		store := &intermittentFailStore{
			failEvery: 5,
		}

		limiter := New(Config{
			Rate:     1000,
			Burst:    1000,
			Interval: time.Second,
			Store:    store,
			FailOpen: true,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		var wg sync.WaitGroup

		// Concurrent requests during intermittent failures
		for i := 0; i < 100; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				limiter.Allow(ctx, "test")
			}()
		}

		wg.Wait()
		// Just verify no panics - with FailOpen all should "succeed"
	})
}

// intermittentFailStore fails every N calls.
type intermittentFailStore struct {
	states    map[string]*BucketState
	mu        sync.Mutex
	callCount int
	failEvery int
}

func (s *intermittentFailStore) AtomicUpdate(ctx context.Context, key string, updateFn func(*BucketState) *BucketState) (*BucketState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.callCount++
	if s.callCount%s.failEvery == 0 {
		return nil, errors.New("intermittent failure")
	}

	if s.states == nil {
		s.states = make(map[string]*BucketState)
	}

	current := s.states[key]
	newState := updateFn(current)
	if newState != nil {
		s.states[key] = newState
	}

	return newState, nil
}

func (s *intermittentFailStore) Get(ctx context.Context, key string) (*BucketState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.callCount++
	if s.callCount%s.failEvery == 0 {
		return nil, errors.New("intermittent failure")
	}

	if s.states == nil {
		return nil, nil
	}

	state := s.states[key]
	if state == nil {
		return nil, nil
	}

	return &BucketState{
		Tokens:     state.Tokens,
		LastRefill: state.LastRefill,
	}, nil
}

func (s *intermittentFailStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.callCount++
	if s.callCount%s.failEvery == 0 {
		return errors.New("intermittent failure")
	}

	delete(s.states, key)
	return nil
}

func (s *intermittentFailStore) Close() error {
	return nil
}

// recoverableStore fails until a certain call count, then recovers.
type recoverableStore struct {
	states        map[string]*BucketState
	mu            sync.Mutex
	callCount     int
	failUntilCall int
}

func (s *recoverableStore) AtomicUpdate(ctx context.Context, key string, updateFn func(*BucketState) *BucketState) (*BucketState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.callCount++
	if s.callCount <= s.failUntilCall {
		return nil, errors.New("temporary failure")
	}

	if s.states == nil {
		s.states = make(map[string]*BucketState)
	}

	current := s.states[key]
	newState := updateFn(current)
	if newState != nil {
		s.states[key] = newState
	}

	return newState, nil
}

func (s *recoverableStore) Get(ctx context.Context, key string) (*BucketState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.callCount <= s.failUntilCall {
		return nil, errors.New("temporary failure")
	}

	if s.states == nil {
		return nil, nil
	}

	state := s.states[key]
	if state == nil {
		return nil, nil
	}

	return &BucketState{
		Tokens:     state.Tokens,
		LastRefill: state.LastRefill,
	}, nil
}

func (s *recoverableStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.callCount <= s.failUntilCall {
		return errors.New("temporary failure")
	}

	delete(s.states, key)
	return nil
}

func (s *recoverableStore) Close() error {
	return nil
}

// TestWrapStorageError tests the WrapStorageError helper function.
func TestWrapStorageError(t *testing.T) {
	t.Parallel()
	t.Run("returns ErrStorageUnavailable for nil cause", func(t *testing.T) {
		t.Parallel()
		err := WrapStorageError(nil)
		if !errors.Is(err, ErrStorageUnavailable) {
			t.Errorf("expected ErrStorageUnavailable, got %v", err)
		}
	})

	t.Run("wraps error and supports errors.Is", func(t *testing.T) {
		cause := errors.New("redis connection failed")
		err := WrapStorageError(cause)

		// Should match ErrStorageUnavailable
		if !errors.Is(err, ErrStorageUnavailable) {
			t.Errorf("errors.Is should return true for ErrStorageUnavailable")
		}

		// Should also match the cause
		if !errors.Is(err, cause) {
			t.Errorf("errors.Is should return true for the cause error")
		}

		// Error message should contain both
		if !strings.Contains(err.Error(), "storage unavailable") {
			t.Errorf("error message should contain 'storage unavailable', got: %s", err.Error())
		}
		if !strings.Contains(err.Error(), "redis connection failed") {
			t.Errorf("error message should contain cause, got: %s", err.Error())
		}
	})

	t.Run("supports errors.Unwrap chain", func(t *testing.T) {
		innerCause := errors.New("timeout")
		wrappedCause := fmt.Errorf("connection error: %w", innerCause)
		err := WrapStorageError(wrappedCause)

		// Should match all errors in the chain
		if !errors.Is(err, ErrStorageUnavailable) {
			t.Errorf("should match ErrStorageUnavailable")
		}
		if !errors.Is(err, wrappedCause) {
			t.Errorf("should match wrappedCause")
		}
		if !errors.Is(err, innerCause) {
			t.Errorf("should match innerCause through unwrap chain")
		}
	})
}

// TestRateLimiterExecute tests the Execute method.
func TestRateLimiterExecute(t *testing.T) {
	t.Parallel()
	t.Run("executes operation when allowed", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		executed := false
		err := limiter.Execute(context.Background(), "test-key", func() error {
			executed = true
			return nil
		})

		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
		if !executed {
			t.Error("operation should have been executed")
		}
	})

	t.Run("returns ErrLimitExceeded when rate limited", func(t *testing.T) {
		limiter := New(Config{
			Rate:     1,
			Burst:    1,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// First call should succeed
		err := limiter.Execute(context.Background(), "test-key", func() error {
			return nil
		})
		if err != nil {
			t.Errorf("first call should succeed, got: %v", err)
		}

		// Second call should be rate limited
		executed := false
		err = limiter.Execute(context.Background(), "test-key", func() error {
			executed = true
			return nil
		})

		if !errors.Is(err, ErrLimitExceeded) {
			t.Errorf("expected ErrLimitExceeded, got: %v", err)
		}
		if executed {
			t.Error("operation should not have been executed when rate limited")
		}
	})

	t.Run("returns operation error", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		opErr := errors.New("operation failed")
		err := limiter.Execute(context.Background(), "test-key", func() error {
			return opErr
		})

		if !errors.Is(err, opErr) {
			t.Errorf("expected operation error, got: %v", err)
		}
	})

	t.Run("returns ErrRateLimiterClosed when closed", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		_ = limiter.Close()

		err := limiter.Execute(context.Background(), "test-key", func() error {
			return nil
		})

		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got: %v", err)
		}
	})
}

// TestRateLimiterExecuteN tests the ExecuteN method.
func TestRateLimiterExecuteN(t *testing.T) {
	t.Parallel()
	t.Run("executes operation when enough tokens", func(t *testing.T) {
		t.Parallel()
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		executed := false
		err := limiter.ExecuteN(context.Background(), "test-key", 5, func() error {
			executed = true
			return nil
		})

		if err != nil {
			t.Errorf("expected no error, got: %v", err)
		}
		if !executed {
			t.Error("operation should have been executed")
		}
	})

	t.Run("returns ErrLimitExceeded when not enough tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// Take all tokens
		if !limiter.Take(context.Background(), "test-key", 10) {
			t.Fatal("should be able to take all tokens")
		}

		// Now ExecuteN should fail
		executed := false
		err := limiter.ExecuteN(context.Background(), "test-key", 5, func() error {
			executed = true
			return nil
		})

		if !errors.Is(err, ErrLimitExceeded) {
			t.Errorf("expected ErrLimitExceeded, got: %v", err)
		}
		if executed {
			t.Error("operation should not have been executed")
		}
	})

	t.Run("returns ErrInvalidTokenCount for zero tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.ExecuteN(context.Background(), "test-key", 0, func() error {
			return nil
		})

		if !errors.Is(err, ErrInvalidTokenCount) {
			t.Errorf("expected ErrInvalidTokenCount, got: %v", err)
		}
	})

	t.Run("returns ErrInvalidTokenCount for negative tokens", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.ExecuteN(context.Background(), "test-key", -1, func() error {
			return nil
		})

		if !errors.Is(err, ErrInvalidTokenCount) {
			t.Errorf("expected ErrInvalidTokenCount, got: %v", err)
		}
	})

	t.Run("returns ErrExcessiveTokens when exceeding MaxTokensPerRequest", func(t *testing.T) {
		limiter := New(Config{
			Rate:                10,
			Burst:               10,
			Interval:            time.Second,
			MaxTokensPerRequest: 5,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.ExecuteN(context.Background(), "test-key", 10, func() error {
			return nil
		})

		if !errors.Is(err, ErrExcessiveTokens) {
			t.Errorf("expected ErrExcessiveTokens, got: %v", err)
		}
	})

	t.Run("returns operation error", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		opErr := errors.New("operation failed")
		err := limiter.ExecuteN(context.Background(), "test-key", 1, func() error {
			return opErr
		})

		if !errors.Is(err, opErr) {
			t.Errorf("expected operation error, got: %v", err)
		}
	})

	t.Run("returns ErrRateLimiterClosed when closed", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		_ = limiter.Close()

		err := limiter.ExecuteN(context.Background(), "test-key", 1, func() error {
			return nil
		})

		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got: %v", err)
		}
	})
}

// TestRateLimiterReset tests the Reset method.
func TestRateLimiterReset(t *testing.T) {
	t.Run("clears all buckets", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// Create some buckets
		limiter.Allow(context.Background(), "key1")
		limiter.Allow(context.Background(), "key2")
		limiter.Allow(context.Background(), "key3")

		// Verify buckets exist
		if count := limiter.BucketCount(); count != 3 {
			t.Errorf("expected 3 buckets before reset, got: %d", count)
		}

		// Reset
		err := limiter.Reset(context.Background())
		if err != nil {
			t.Errorf("reset failed: %v", err)
		}

		// Verify buckets are cleared
		if count := limiter.BucketCount(); count != 0 {
			t.Errorf("expected 0 buckets after reset, got: %d", count)
		}
	})

	t.Run("returns ErrRateLimiterClosed when closed", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		_ = limiter.Close()

		err := limiter.Reset(context.Background())
		if !errors.Is(err, ErrRateLimiterClosed) {
			t.Errorf("expected ErrRateLimiterClosed, got: %v", err)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := limiter.Reset(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})

	t.Run("warns when store does not implement Resetter", func(t *testing.T) {
		var logBuffer bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&logBuffer, nil))

		store := &mockStore{}
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
			Logger:   logger,
		})
		defer func() { _ = limiter.Close() }()

		err := limiter.Reset(context.Background())
		if err != nil {
			t.Errorf("expected nil error, got: %v", err)
		}

		logOutput := logBuffer.String()
		if !strings.Contains(logOutput, "Store does not implement Resetter") {
			t.Errorf("expected warning log, got: %s", logOutput)
		}
	})
}

// TestMemoryStoreReset tests the MemoryStore Reset method.
func TestMemoryStoreReset(t *testing.T) {
	t.Run("clears all buckets", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithCleanupInterval(0))
		defer func() { _ = store.Close() }()

		// Create some buckets
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key-%d", i)
			_, err := store.AtomicUpdate(context.Background(), key, func(state *BucketState) *BucketState {
				return &BucketState{Tokens: 10, LastRefill: time.Now()}
			})
			if err != nil {
				t.Fatalf("failed to create bucket: %v", err)
			}
		}

		// Verify buckets exist
		if count := store.BucketCount(); count != 5 {
			t.Errorf("expected 5 buckets before reset, got: %d", count)
		}

		// Reset
		err := store.Reset(context.Background())
		if err != nil {
			t.Errorf("reset failed: %v", err)
		}

		// Verify buckets are cleared
		if count := store.BucketCount(); count != 0 {
			t.Errorf("expected 0 buckets after reset, got: %d", count)
		}
	})

	t.Run("returns ErrStoreClosed when closed", func(t *testing.T) {
		store := NewMemoryStore()
		_ = store.Close()

		err := store.Reset(context.Background())
		if !errors.Is(err, ErrStoreClosed) {
			t.Errorf("expected ErrStoreClosed, got: %v", err)
		}
	})

	t.Run("respects context cancellation", func(t *testing.T) {
		store := NewMemoryStore()
		defer func() { _ = store.Close() }()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.Reset(ctx)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	})
}

// TestRateLimiterBucketCount tests the BucketCount method.
func TestRateLimiterBucketCount(t *testing.T) {
	t.Run("returns correct bucket count", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		defer func() { _ = limiter.Close() }()

		// Initially no buckets
		if count := limiter.BucketCount(); count != 0 {
			t.Errorf("expected 0 buckets initially, got: %d", count)
		}

		// Create some buckets
		limiter.Allow(context.Background(), "key1")
		limiter.Allow(context.Background(), "key2")

		if count := limiter.BucketCount(); count != 2 {
			t.Errorf("expected 2 buckets, got: %d", count)
		}

		// Same key should not create new bucket
		limiter.Allow(context.Background(), "key1")
		if count := limiter.BucketCount(); count != 2 {
			t.Errorf("expected still 2 buckets, got: %d", count)
		}
	})

	t.Run("returns -1 when closed", func(t *testing.T) {
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
		})
		_ = limiter.Close()

		if count := limiter.BucketCount(); count != -1 {
			t.Errorf("expected -1 when closed, got: %d", count)
		}
	})

	t.Run("returns -1 when store does not support BucketCounter", func(t *testing.T) {
		store := &mockStore{}
		limiter := New(Config{
			Rate:     10,
			Burst:    10,
			Interval: time.Second,
			Store:    store,
		})
		defer func() { _ = limiter.Close() }()

		if count := limiter.BucketCount(); count != -1 {
			t.Errorf("expected -1 for store without BucketCounter, got: %d", count)
		}
	})
}

// TestMemoryStoreBucketCount tests the MemoryStore BucketCount method.
func TestMemoryStoreBucketCount(t *testing.T) {
	t.Run("returns correct count", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithCleanupInterval(0))
		defer func() { _ = store.Close() }()

		// Initially 0
		if count := store.BucketCount(); count != 0 {
			t.Errorf("expected 0 initially, got: %d", count)
		}

		// Add some buckets
		for i := 0; i < 3; i++ {
			key := fmt.Sprintf("key-%d", i)
			//nolint:errcheck // test setup
			_, _ = store.AtomicUpdate(context.Background(), key, func(state *BucketState) *BucketState {
				return &BucketState{Tokens: 10, LastRefill: time.Now()}
			})
		}

		if count := store.BucketCount(); count != 3 {
			t.Errorf("expected 3, got: %d", count)
		}

		// Delete one
		_ = store.Delete(context.Background(), "key-0") //nolint:errcheck // test helper

		if count := store.BucketCount(); count != 2 {
			t.Errorf("expected 2 after delete, got: %d", count)
		}
	})

	t.Run("KeyCount is alias for BucketCount", func(t *testing.T) {
		store := NewMemoryStoreWithOptions(WithCleanupInterval(0))
		defer func() { _ = store.Close() }()

		//nolint:errcheck // test setup
		_, _ = store.AtomicUpdate(context.Background(), "test", func(state *BucketState) *BucketState {
			return &BucketState{Tokens: 10, LastRefill: time.Now()}
		})

		if store.KeyCount() != store.BucketCount() {
			t.Error("KeyCount and BucketCount should return same value")
		}
	})
}

// TestSanitizeLogKey tests the log injection prevention function.
//
//nolint:gocyclo // test function with many subtests
func TestSanitizeLogKey(t *testing.T) {
	t.Parallel()

	t.Run("returns clean key unchanged", func(t *testing.T) {
		t.Parallel()
		cleanKeys := []string{
			"user-123",
			"api-key-abc",
			"192.168.1.1",
			"fe80::1",
			"normal_key_with_underscores",
			"MixedCase123",
		}

		for _, key := range cleanKeys {
			result := sanitizeLogKey(key)
			if result != key {
				t.Errorf("sanitizeLogKey(%q) = %q, want unchanged", key, result)
			}
		}
	})

	t.Run("replaces newline characters", func(t *testing.T) {
		t.Parallel()
		// Newline injection attack: attacker tries to inject fake log entry
		maliciousKey := "user-123\nERROR: admin logged in"
		result := sanitizeLogKey(maliciousKey)

		if strings.Contains(result, "\n") {
			t.Errorf("sanitizeLogKey should remove newlines, got: %q", result)
		}
		if result != "user-123_ERROR: admin logged in" {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", maliciousKey, result, "user-123_ERROR: admin logged in")
		}
	})

	t.Run("replaces carriage return characters", func(t *testing.T) {
		t.Parallel()
		// CRLF injection attack
		maliciousKey := "user-123\r\nFake-Header: injected"
		result := sanitizeLogKey(maliciousKey)

		if strings.Contains(result, "\r") || strings.Contains(result, "\n") {
			t.Errorf("sanitizeLogKey should remove CR/LF, got: %q", result)
		}
		expected := "user-123__Fake-Header: injected"
		if result != expected {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", maliciousKey, result, expected)
		}
	})

	t.Run("replaces tab characters", func(t *testing.T) {
		t.Parallel()
		keyWithTab := "user\t123"
		result := sanitizeLogKey(keyWithTab)

		if strings.Contains(result, "\t") {
			t.Errorf("sanitizeLogKey should remove tabs, got: %q", result)
		}
		if result != "user_123" {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", keyWithTab, result, "user_123")
		}
	})

	t.Run("replaces null bytes", func(t *testing.T) {
		t.Parallel()
		// Null byte injection
		maliciousKey := "user\x00admin"
		result := sanitizeLogKey(maliciousKey)

		if strings.Contains(result, "\x00") {
			t.Errorf("sanitizeLogKey should remove null bytes, got: %q", result)
		}
		if result != "user_admin" {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", maliciousKey, result, "user_admin")
		}
	})

	t.Run("replaces DEL character (127)", func(t *testing.T) {
		t.Parallel()
		keyWithDEL := "user\x7Fadmin"
		result := sanitizeLogKey(keyWithDEL)

		if strings.Contains(result, "\x7F") {
			t.Errorf("sanitizeLogKey should remove DEL character, got: %q", result)
		}
		if result != "user_admin" {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", keyWithDEL, result, "user_admin")
		}
	})

	t.Run("replaces all control characters", func(t *testing.T) {
		t.Parallel()
		// Test all control characters (0-31 and 127)
		for i := 0; i < 32; i++ {
			key := fmt.Sprintf("test%ckey", rune(i))
			result := sanitizeLogKey(key)

			for _, r := range result {
				if r < 32 || r == 127 {
					t.Errorf("sanitizeLogKey should remove control char %d, got: %q", i, result)
				}
			}
		}

		// Also test DEL (127)
		key := "test\x7Fkey"
		result := sanitizeLogKey(key)
		for _, r := range result {
			if r == 127 {
				t.Errorf("sanitizeLogKey should remove DEL (127), got: %q", result)
			}
		}
	})

	t.Run("handles empty string", func(t *testing.T) {
		t.Parallel()
		result := sanitizeLogKey("")
		if result != "" {
			t.Errorf("sanitizeLogKey(\"\") = %q, want empty string", result)
		}
	})

	t.Run("handles string with only control characters", func(t *testing.T) {
		t.Parallel()
		onlyControlChars := "\x00\x01\x02\n\r\t"
		result := sanitizeLogKey(onlyControlChars)

		// Should replace all with underscores
		expected := "______"
		if result != expected {
			t.Errorf("sanitizeLogKey(%q) = %q, want %q", onlyControlChars, result, expected)
		}
	})

	t.Run("preserves unicode characters", func(t *testing.T) {
		t.Parallel()
		unicodeKey := "用户-123-émojis-🔥"
		result := sanitizeLogKey(unicodeKey)

		if result != unicodeKey {
			t.Errorf("sanitizeLogKey(%q) = %q, want unchanged (unicode preserved)", unicodeKey, result)
		}
	})

	t.Run("handles long keys efficiently", func(t *testing.T) {
		t.Parallel()
		// 10KB key - should not cause issues
		longKey := strings.Repeat("a", 10000)
		result := sanitizeLogKey(longKey)

		if result != longKey {
			t.Errorf("sanitizeLogKey should handle long keys, got length %d", len(result))
		}
	})

	t.Run("same IP different zones produces same sanitized key", func(t *testing.T) {
		t.Parallel()
		// These keys might come from different network interfaces
		keys := []string{
			"fe80::1%eth0",
			"fe80::1%eth1",
			"fe80::1%wlan0",
		}

		results := make(map[string]bool)
		for _, key := range keys {
			result := sanitizeLogKey(key)
			results[result] = true
			// Zone identifiers don't contain control characters, so should be unchanged
			if result != key {
				t.Errorf("sanitizeLogKey(%q) = %q, want unchanged (no control chars)", key, result)
			}
		}
	})
}

// TestRateLimiterRatePerInterval pins the meaning of Rate as "tokens per
// Interval" rather than "tokens per second". Widening Interval is how
// sustained rates below one request per second are expressed, so quotas
// published per minute or per hour need no fractional Rate.
func TestRateLimiterRatePerInterval(t *testing.T) {
	t.Parallel()

	t.Run("burst bounds the initial spike regardless of interval", func(t *testing.T) {
		t.Parallel()
		tests := []struct {
			name     string
			interval time.Duration
			rate     int
			burst    int
			want     int
		}{
			{name: "10 per second", interval: time.Second, rate: 10, burst: 10, want: 10},
			{name: "50 per minute", interval: time.Minute, rate: 50, burst: 50, want: 50},
			{name: "50 per minute, burst capped", interval: time.Minute, rate: 50, burst: 10, want: 10},
			{name: "500 per hour", interval: time.Hour, rate: 500, burst: 500, want: 500},
			{name: "1 per minute", interval: time.Minute, rate: 1, burst: 1, want: 1},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				limiter := New(Config{
					Rate:     tt.rate,
					Burst:    tt.burst,
					Interval: tt.interval,
				})
				defer func() { _ = limiter.Close() }()

				ctx := context.Background()
				got := 0
				for i := 0; i < tt.want*2+10; i++ {
					if limiter.Allow(ctx, "quota-key") {
						got++
					}
				}

				if got != tt.want {
					t.Errorf("allowed %d requests, want %d", got, tt.want)
				}
			})
		}
	})

	t.Run("sub-one-per-second sustained rate refills continuously", func(t *testing.T) {
		t.Parallel()
		// 50 requests/minute is 0.83/sec: one token roughly every 1.2s.
		// An int Rate with Interval=time.Second could express neither 0.83
		// nor a rounded-down alternative, so this is the case Interval exists for.
		limiter := New(Config{
			Rate:     50,
			Burst:    1,
			Interval: time.Minute,
		})
		defer func() { _ = limiter.Close() }()

		ctx := context.Background()
		if !limiter.Allow(ctx, "slow-key") {
			t.Fatal("first request should drain the single-token bucket")
		}
		if limiter.Allow(ctx, "slow-key") {
			t.Fatal("second request should be denied before any refill")
		}

		// Well short of a full Interval: refill is continuous, not stepped.
		time.Sleep(1300 * time.Millisecond)

		if !limiter.Allow(ctx, "slow-key") {
			t.Error("a token should be back after ~1.2s at 50/minute")
		}
		if limiter.Allow(ctx, "slow-key") {
			t.Error("only one token should have refilled")
		}
	})
}
