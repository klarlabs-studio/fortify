// Package circuitbreaker provides a generic circuit breaker implementation
// for preventing cascading failures in distributed systems.
//
// A circuit breaker prevents an application from repeatedly trying to execute
// an operation that's likely to fail, allowing it to continue without waiting
// for the fault to be fixed or wasting resources.
//
// The circuit breaker has three states:
//   - Closed: Requests pass through normally. Failures are counted.
//   - Open: Requests fail immediately without execution. After a timeout, transitions to Half-Open.
//   - Half-Open: A limited number of test requests are allowed. Success transitions to Closed, failure to Open.
//
// Example usage:
//
//	cb := circuitbreaker.New[*Response](circuitbreaker.Config{
//	    MaxRequests: 5,
//	    Interval:    10 * time.Second,
//	    Timeout:     60 * time.Second,
//	    ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(5),
//	})
//
//	result, err := cb.Execute(ctx, func(ctx context.Context) (*Response, error) {
//	    return callExternalAPI(ctx)
//	})
//
// # Choosing a trip policy
//
// Two predicates cover the large majority of real ReadyToTrip
// implementations, and both take plain ints so no uint32 conversion (and
// no gosec G115 suppression) is needed at the call site:
//
//   - TripOnConsecutiveFailures(n) — opens after n failures in a row. Any
//     success resets the streak, so this detects a downstream that is
//     down, not one that is degraded.
//   - TripOnFailureRatio(r, minRequests) — opens at a failure rate of r
//     once minRequests have been recorded. This catches the partially
//     failing downstream that consecutive counting misses.
//
// Callers needing something bespoke still receive the raw Counts.
//
// # Sliding-window failure rate
//
// Consecutive-failure counting only detects a downstream that is down.
// A downstream that is merely degraded — failing, say, half its calls —
// never accumulates a streak, because any success resets it, so the
// circuit stays Closed however long the degradation lasts. Config.Interval
// does not help: it is a tumbling window, so a burst of failures spanning
// a reset is split into two sub-threshold halves.
//
// Set Config.SlidingWindowType to trip on a failure rate over a sliding
// window instead, as Resilience4j and Polly do by default:
//
//	cb := circuitbreaker.New[*Response](circuitbreaker.Config{
//	    SlidingWindowType:    circuitbreaker.SlidingWindowCount,
//	    SlidingWindowSize:    100, // last 100 calls
//	    MinimumCalls:         20,  // ...but not before 20 are in
//	    FailureRateThreshold: 0.5, // open at 50%
//	})
//
// SlidingWindowCount aggregates the last SlidingWindowSize calls in a
// circular buffer; SlidingWindowTime aggregates the last
// SlidingWindowSize seconds in one-second buckets, and is the better
// default for high-traffic services where a count window fills fast
// enough to make the breaker jumpy. Neither has a boundary for a burst to
// straddle.
//
// Config.ReadyToTrip overrides the sliding window when both are set.
package circuitbreaker

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"go.klarlabs.de/fortify/ferrors"
)

// ErrCircuitOpen is returned when the circuit is open and the call was
// rejected without running.
//
// Same value as ferrors.ErrCircuitOpen, re-exported for the reason
// bulkhead.ErrBulkheadFull is: Execute's documentation names it, so
// errors.Is(err, circuitbreaker.ErrCircuitOpen) should compile. Matching on
// either spelling works.
var ErrCircuitOpen = ferrors.ErrCircuitOpen

// CircuitBreaker is a generic interface for circuit breaker pattern implementation.
// It protects against cascading failures by monitoring request outcomes and
// temporarily blocking requests when a failure threshold is reached.
type CircuitBreaker[T any] interface {
	// Execute runs the given function if the circuit breaker allows it.
	// If the circuit is open, it returns ErrCircuitOpen without executing the function.
	// The function receives the context which may be cancelled.
	Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error)

	// State returns the current state of the circuit breaker.
	State() State

	// Reset manually resets the circuit breaker to the Closed state and clears all counts.
	Reset()

	// Close releases resources held by the circuit breaker, including the
	// internal callback dispatcher goroutine. After Close, OnStateChange
	// callbacks no longer fire. It is safe to call Close multiple times.
	Close() error
}

// stateChange is an event emitted when the circuit breaker transitions between states.
type stateChange struct {
	from State
	to   State
}

// stateChangeBufferSize bounds the in-flight state change queue to prevent
// unbounded memory growth under callback storms (e.g., a flapping breaker
// combined with a slow user callback).
const stateChangeBufferSize = 64

// circuitBreaker is the concrete implementation of CircuitBreaker.
type circuitBreaker[T any] struct {
	expiry        time.Time
	halfOpenStart time.Time
	config        Config
	mu            sync.RWMutex
	counts        Counts
	generation    uint64
	state         State

	// window aggregates recent outcomes for failure-rate tripping. nil
	// when no sliding window is configured, in which case tripping goes
	// through config.ReadyToTrip. Guarded by mu.
	window slidingWindow

	// fastState/fastExpiry/fastGen mirror state/expiry/generation under
	// atomic semantics. They allow beforeRequest to short-circuit when the
	// breaker is in steady-state Closed without acquiring mu, restoring the
	// "<1µs" hot-path claim under contention. Mirrors are updated by
	// syncFastFields, always called with mu held.
	fastState  atomic.Int32
	fastExpiry atomic.Int64
	fastGen    atomic.Uint64

	// stateChanges receives state transition events for sequenced delivery
	// to OnStateChange. nil if no callback is configured.
	stateChanges chan stateChange
	// done signals the dispatcher goroutine to exit.
	done      chan struct{}
	closeOnce sync.Once
}

// syncFastFields refreshes the atomic mirrors from the locked fields.
// Must be called with cb.mu held.
func (cb *circuitBreaker[T]) syncFastFields() {
	cb.fastGen.Store(cb.generation)
	cb.fastExpiry.Store(cb.expiry.UnixNano())
	cb.fastState.Store(int32(cb.state))
}

// New creates a new CircuitBreaker with the given configuration.
// The circuit breaker starts in the Closed state.
func New[T any](config Config) CircuitBreaker[T] {
	// ReadyToTrip is the documented override, so note whether the caller
	// supplied one before setDefaults populates it.
	callerSetReadyToTrip := config.ReadyToTrip != nil

	config.setDefaults()

	usesWindow := !callerSetReadyToTrip && config.SlidingWindowType != SlidingWindowDisabled
	if callerSetReadyToTrip && config.SlidingWindowType != SlidingWindowDisabled && config.Logger != nil {
		config.Logger.Warn("circuit breaker: ReadyToTrip overrides the sliding-window configuration",
			slog.String("sliding_window_type", config.SlidingWindowType.String()),
		)
	}

	cb := &circuitBreaker[T]{
		config:     config,
		state:      StateClosed,
		generation: 0,
		expiry:     time.Now().Add(config.Interval),
	}
	// Seed atomic mirrors so the fast path sees consistent initial values
	// without requiring a lock acquisition first.
	cb.fastGen.Store(0)
	cb.fastExpiry.Store(cb.expiry.UnixNano())
	cb.fastState.Store(int32(StateClosed))

	if usesWindow {
		cb.window = newSlidingWindow(&config)
	}

	if config.OnStateChange != nil {
		cb.stateChanges = make(chan stateChange, stateChangeBufferSize)
		cb.done = make(chan struct{})
		go cb.dispatchStateChanges()
	}

	return cb
}

// dispatchStateChanges delivers state transitions to OnStateChange in
// the order they were emitted. It exits when Close signals via done.
func (cb *circuitBreaker[T]) dispatchStateChanges() {
	for {
		select {
		case ev := <-cb.stateChanges:
			cb.safeCallback(func() { cb.config.OnStateChange(ev.from, ev.to) })
		case <-cb.done:
			return
		}
	}
}

// emitStateChange enqueues a state change for serialized delivery.
// If the queue is full (callback consumer is slow) or the breaker has been
// closed, the event is dropped and (when full) a warning is logged.
func (cb *circuitBreaker[T]) emitStateChange(from, to State) {
	if cb.stateChanges == nil {
		return
	}
	select {
	case cb.stateChanges <- stateChange{from: from, to: to}:
	case <-cb.done:
		// Breaker closed - silently drop further events.
	default:
		if cb.config.Logger != nil {
			cb.config.Logger.Warn("circuit breaker state change dropped: callback queue full",
				slog.String("from", from.String()),
				slog.String("to", to.String()),
			)
		}
	}
}

// Close implements the CircuitBreaker interface.
// It signals the dispatcher goroutine to exit. Subsequent state changes
// do not fire OnStateChange callbacks. It is safe to call Close multiple times.
func (cb *circuitBreaker[T]) Close() error {
	cb.closeOnce.Do(func() {
		if cb.done != nil {
			close(cb.done)
		}
	})
	return nil
}

// Execute implements the CircuitBreaker interface.
func (cb *circuitBreaker[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	// Check context first
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	// Try a lock-free fast path: if the breaker is in steady-state Closed,
	// no lock is needed to admit the request. This is the dominant case in
	// healthy systems and keeps Execute hot-path-cheap.
	if generation, ok := cb.fastAdmit(); ok {
		result, err := fn(ctx)
		cb.afterRequest(generation, cb.config.IsSuccessful(err))
		return result, err
	}

	// Slow path: full state machine under mu.
	generation, err := cb.beforeRequest()
	if err != nil {
		return zero, err
	}

	result, err := fn(ctx)
	cb.afterRequest(generation, cb.config.IsSuccessful(err))
	return result, err
}

// fastAdmit returns (generation, true) when the breaker is in Closed state
// and no counts-reset is due. It performs only atomic loads, no mutex.
// Returns (0, false) when the slow path must be taken.
func (cb *circuitBreaker[T]) fastAdmit() (uint64, bool) {
	if State(cb.fastState.Load()) != StateClosed {
		return 0, false
	}
	// Interval == 0 means counts never reset in Closed state, so the
	// expiry check is a no-op. Skip it.
	if cb.config.Interval > 0 {
		if time.Now().UnixNano() >= cb.fastExpiry.Load() {
			return 0, false
		}
	}
	// Re-check state after expiry load to defend against a transition that
	// landed between the two atomic loads. Slow path handles that case.
	if State(cb.fastState.Load()) != StateClosed {
		return 0, false
	}
	return cb.fastGen.Load(), true
}

// State implements the CircuitBreaker interface.
func (cb *circuitBreaker[T]) State() State {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	now := time.Now()
	state, _ := cb.currentState(now)
	return state
}

// Reset implements the CircuitBreaker interface.
func (cb *circuitBreaker[T]) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	prev := cb.state
	cb.logStateChange(prev, StateClosed)
	cb.state = StateClosed
	cb.generation++
	cb.counts.reset()
	if cb.window != nil {
		cb.window.reset()
	}
	cb.expiry = time.Now().Add(cb.config.Interval)
	cb.syncFastFields()

	cb.emitStateChange(prev, StateClosed)
}

// beforeRequest checks if the request should be allowed and returns the current generation.
// Returns a *ferrors.CircuitOpenError if the circuit is open and not ready for half-open
// trial requests. The structured error carries the breaker state, retry-after, and counts;
// errors.Is(err, ferrors.ErrCircuitOpen) continues to match.
func (cb *circuitBreaker[T]) beforeRequest() (uint64, error) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, cb.openError(state, now)
	}

	if state == StateHalfOpen && cb.counts.Requests >= cb.config.MaxRequests {
		return generation, cb.openError(state, now)
	}

	return generation, nil
}

// openError builds a structured error describing why the breaker rejected.
// Caller must hold cb.mu.
func (cb *circuitBreaker[T]) openError(state State, now time.Time) error {
	var retryAfter time.Duration
	if state == StateOpen && cb.expiry.After(now) {
		retryAfter = cb.expiry.Sub(now)
	}
	return &ferrors.CircuitOpenError{
		State:                state.String(),
		RetryAfter:           retryAfter,
		TotalRequests:        cb.counts.Requests,
		TotalFailures:        cb.counts.TotalFailures,
		ConsecutiveFailures:  cb.counts.ConsecutiveFailures,
		ConsecutiveSuccesses: cb.counts.ConsecutiveSuccesses,
	}
}

// afterRequest updates the circuit breaker state after a request completes.
// It must be called with the generation returned by beforeRequest.
func (cb *circuitBreaker[T]) afterRequest(generation uint64, success bool) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	state, currentGen := cb.currentState(now)

	// Ignore results from old generations
	if generation != currentGen {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

// currentState determines the current state based on time and counts.
// Must be called with lock held.
func (cb *circuitBreaker[T]) currentState(now time.Time) (state State, generation uint64) {
	switch cb.state {
	case StateClosed:
		if cb.config.Interval > 0 && cb.expiry.Before(now) {
			cb.counts.reset()
			cb.expiry = now.Add(cb.config.Interval)
			// Refresh fast-path mirror so subsequent admits skip the lock.
			cb.syncFastFields()
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}

	return cb.state, cb.generation
}

// onSuccess handles a successful request outcome.
// Must be called with lock held.
func (cb *circuitBreaker[T]) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onSuccess()
		if cb.window != nil {
			// Successes must enter the window too, or the failure rate
			// has no denominator. Evaluating here as well as on failure
			// matters only at the MinimumCalls boundary, where a success
			// can be the call that makes the window eligible.
			cb.window.record(true, now)
			if cb.windowTrips(now) {
				cb.setState(StateOpen, now)
			}
		}
	case StateHalfOpen:
		cb.counts.onSuccess()
		if cb.counts.ConsecutiveSuccesses >= cb.config.MaxRequests {
			cb.setState(StateClosed, now)
		}
	}
}

// onFailure handles a failed request outcome.
// Must be called with lock held.
func (cb *circuitBreaker[T]) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.onFailure()
		if cb.window != nil {
			cb.window.record(false, now)
			if cb.windowTrips(now) {
				cb.setState(StateOpen, now)
			}
			return
		}
		if cb.config.ReadyToTrip(cb.counts) {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

// setState transitions to a new state and updates related fields.
// Must be called with lock held.
func (cb *circuitBreaker[T]) setState(newState State, now time.Time) {
	if cb.state == newState {
		return
	}

	oldState := cb.state
	cb.state = newState
	cb.generation++
	cb.counts.reset()
	// The window describes the previous regime; a breaker re-entering
	// Closed after a trial should judge the downstream on fresh evidence.
	if cb.window != nil {
		cb.window.reset()
	}

	switch newState {
	case StateClosed:
		cb.expiry = now.Add(cb.config.Interval)
	case StateOpen:
		cb.expiry = now.Add(cb.config.Timeout)
	case StateHalfOpen:
		cb.halfOpenStart = now
	}
	cb.syncFastFields()

	cb.logStateChange(oldState, newState)
	cb.emitStateChange(oldState, newState)
}

// safeCallback executes a callback with panic recovery.
func (cb *circuitBreaker[T]) safeCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if cb.config.Logger != nil {
				cb.config.Logger.Error("circuit breaker callback panic",
					slog.String("pattern", "circuit_breaker"),
					slog.Any("panic", r),
				)
			}
		}
	}()
	fn()
}

// logStateChange logs state transitions using structured logging.
func (cb *circuitBreaker[T]) logStateChange(from, to State) {
	if cb.config.Logger != nil {
		cb.config.Logger.Info("circuit breaker state changed",
			slog.String("from", from.String()),
			slog.String("to", to.String()),
		)
	}
}

// String returns a string representation of the circuit breaker for debugging.
func (cb *circuitBreaker[T]) String() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	return fmt.Sprintf("CircuitBreaker[state=%s, generation=%d, counts=%+v]",
		cb.state, cb.generation, cb.counts)
}
