package circuitbreaker

import (
	"log/slog"
	"time"
)

// Config holds the configuration for a CircuitBreaker.
type Config struct {
	// ReadyToTrip is called with a copy of Counts whenever a request fails in the Closed state.
	// If ReadyToTrip returns true, the circuit breaker transitions from Closed to Open.
	// If ReadyToTrip is nil, the circuit breaker uses
	// TripOnConsecutiveFailures(DefaultFailuresToTrip).
	//
	// Prefer the shipped predicates over a hand-written closure: they take
	// an int threshold, so no uint32 conversion (and no gosec G115
	// suppression) is needed at the call site.
	//
	//	ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(cfg.FailuresToTrip)
	//	ReadyToTrip: circuitbreaker.TripOnFailureRatio(0.5, 20)
	//
	// Write your own when neither shape fits; Counts stays available.
	//
	// Setting ReadyToTrip overrides the sliding-window fields below: it is
	// the escape hatch for policies neither shape expresses. Configuring
	// both is a mistake, and one that is logged when a Logger is set.
	ReadyToTrip func(counts Counts) bool

	// OnStateChange is called whenever the state of the circuit breaker changes.
	// It receives the previous state and the new state.
	OnStateChange func(from, to State)

	// IsSuccessful is called with the error returned from a request.
	// If IsSuccessful returns true, the request is considered successful;
	// otherwise, it is considered a failure.
	// If IsSuccessful is nil, any non-nil error is considered a failure.
	IsSuccessful func(err error) bool

	// Logger is used for structured logging. If nil, no logging is performed.
	Logger *slog.Logger

	// Interval is the cyclic period of the Closed state for the circuit breaker
	// to clear the internal Counts. If Interval is 0, the circuit breaker doesn't
	// clear internal Counts during the Closed state.
	Interval time.Duration

	// Timeout is the period of the Open state after which the state transitions
	// to Half-Open. If Timeout is 0, the circuit breaker uses a default timeout of 60 seconds.
	Timeout time.Duration

	// MaxRequests is the maximum number of requests allowed to pass through
	// when the circuit breaker is in the Half-Open state.
	// If MaxRequests is 0, the circuit breaker allows only 1 request.
	MaxRequests uint32

	// SlidingWindowType enables failure-rate tripping over a sliding
	// window and selects how outcomes are aggregated. The zero value,
	// SlidingWindowDisabled, keeps the ReadyToTrip behaviour above.
	//
	// A sliding window is what Resilience4j and Polly default to, and it
	// fixes two blind spots in consecutive counting:
	//
	//   - A downstream failing part of the time never accumulates a
	//     consecutive-failure streak, because any success resets it. A
	//     service failing half its calls can stay that way indefinitely
	//     with the circuit closed.
	//   - Interval is a tumbling window: it clears counts wholesale, so a
	//     burst spanning a reset is split into two sub-threshold halves
	//     and never trips. A sliding window has no boundary to straddle.
	//
	// It also decouples the trip threshold from retry's MaxAttempts. With
	// consecutive counting and retry outside the breaker, one flaky call
	// contributes up to MaxAttempts consecutive failures, so any
	// threshold <= MaxAttempts opens the circuit on a single recovered
	// blip. Two failures and a success inside a window of twenty is a 10%
	// failure rate, nowhere near a sensible threshold.
	SlidingWindowType SlidingWindowType

	// SlidingWindowSize is the window's extent: a number of calls when
	// SlidingWindowType is SlidingWindowCount, or a number of seconds
	// when it is SlidingWindowTime.
	//
	// Defaults to DefaultCountWindowSize (100) or
	// DefaultTimeWindowSeconds (60) respectively. Clamped to
	// MaxSlidingWindowSize. Ignored when no sliding window is configured.
	SlidingWindowSize int

	// MinimumCalls is how many calls must be recorded in the window
	// before the failure rate is evaluated, so a cold breaker cannot open
	// on one unlucky request.
	//
	// Defaults to DefaultMinimumCalls (10). For a count-based window it
	// is capped at SlidingWindowSize, since a larger value could never be
	// reached and the breaker would silently never trip.
	MinimumCalls int

	// FailureRateThreshold is the failure ratio, in (0, 1], at which the
	// circuit opens once MinimumCalls have been recorded. The comparison
	// is inclusive, so 0.5 opens at exactly 50%.
	//
	// Defaults to DefaultFailureRateThreshold (0.5). Values outside
	// (0, 1] fall back to the default.
	FailureRateThreshold float64
}

// setDefaults applies default values to unset configuration fields
// and clamps invalid values to safe defaults.
func (c *Config) setDefaults() {
	if c.MaxRequests == 0 {
		c.MaxRequests = 1
	}

	if c.Timeout <= 0 {
		c.Timeout = 60 * time.Second
	}

	if c.Interval < 0 {
		c.Interval = 0
	}

	c.normalizeSlidingWindow()

	// ReadyToTrip stays populated even when a sliding window is active,
	// so the consecutive-failure path is never nil-dispatched; the
	// breaker simply consults the window instead.
	if c.ReadyToTrip == nil {
		c.ReadyToTrip = TripOnConsecutiveFailures(DefaultFailuresToTrip)
	}

	if c.IsSuccessful == nil {
		c.IsSuccessful = func(err error) bool {
			return err == nil
		}
	}
}
