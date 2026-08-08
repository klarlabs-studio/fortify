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

	if c.ReadyToTrip == nil {
		c.ReadyToTrip = TripOnConsecutiveFailures(DefaultFailuresToTrip)
	}

	if c.IsSuccessful == nil {
		c.IsSuccessful = func(err error) bool {
			return err == nil
		}
	}
}
