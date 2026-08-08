package circuitbreaker

import "math"

// DefaultFailuresToTrip is the consecutive-failure count used by the
// breaker when Config.ReadyToTrip is nil.
const DefaultFailuresToTrip = 5

// TripOnConsecutiveFailures returns a ReadyToTrip predicate that opens the
// circuit once n consecutive failures have been observed in the Closed
// state. A single success resets the streak.
//
// This is the most common ReadyToTrip in practice, and shipping it spares
// callers a conversion. A threshold held in application config is
// naturally an int, while Counts.ConsecutiveFailures is a uint32, so the
// hand-written form needs a narrowing cast:
//
//	ReadyToTrip: func(c circuitbreaker.Counts) bool {
//	    return c.ConsecutiveFailures >= uint32(cfg.FailuresToTrip) // gosec G115
//	}
//
// gosec reports that as G115 (integer overflow conversion int -> uint32),
// so every consumer running gosec meets a finding on the most ordinary use
// of this package, and the cheap fix — //nolint:gosec — builds the habit of
// suppressing G115 in places where it is catching something real. Passing
// an int here removes the cast and the suppression:
//
//	cb := circuitbreaker.New[T](circuitbreaker.Config{
//	    ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(cfg.FailuresToTrip),
//	})
//
// n is clamped to [1, math.MaxUint32]: values below 1 would trip the
// breaker before any failure, and values above the counter's range would
// otherwise wrap to a threshold far lower than intended.
//
// Consecutive counting cannot detect a downstream that fails only part of
// the time, because any success resets the streak — a service failing 50%
// of calls produces F S F F S F and never trips. Use TripOnFailureRatio
// for that shape.
func TripOnConsecutiveFailures(n int) func(Counts) bool {
	threshold := clampToUint32(n)
	return func(counts Counts) bool {
		return counts.ConsecutiveFailures >= threshold
	}
}

// TripOnFailureRatio returns a ReadyToTrip predicate that opens the circuit
// when the failure ratio reaches r, once at least minRequests requests have
// been recorded in the current counting window.
//
// Unlike TripOnConsecutiveFailures, this notices a downstream that is
// degraded rather than dead: a service failing half its calls trips a
// ratio predicate but never accumulates a consecutive-failure streak.
//
//	cb := circuitbreaker.New[T](circuitbreaker.Config{
//	    Interval:    30 * time.Second,
//	    ReadyToTrip: circuitbreaker.TripOnFailureRatio(0.5, 20),
//	})
//
// minRequests keeps a cold breaker from opening on one unlucky request;
// it is clamped to at least 1. r is clamped to [0, 1], with NaN treated as
// 0. The comparison is inclusive, so r = 0.5 trips at exactly 50% and
// r = 1 trips only when every recorded request failed.
//
// The window is the breaker's counting window, so this is a *tumbling*
// window governed by Config.Interval, not a sliding one: counts reset
// wholesale every Interval, and a burst that straddles a reset is split
// into two sub-threshold halves. Choose Interval with that in mind.
func TripOnFailureRatio(r float64, minRequests int) func(Counts) bool {
	switch {
	case math.IsNaN(r) || r < 0:
		r = 0
	case r > 1:
		r = 1
	}

	minCalls := clampToUint32(minRequests)

	return func(counts Counts) bool {
		if counts.Requests < minCalls || counts.Requests == 0 {
			return false
		}
		return float64(counts.TotalFailures)/float64(counts.Requests) >= r
	}
}

// clampToUint32 maps an int threshold onto the range of the Counts
// counters, guaranteeing at least 1 so a predicate cannot fire before any
// request is recorded. Doing the narrowing here, once and provably in
// range, is what lets callers pass a plain int.
func clampToUint32(n int) uint32 {
	if n < 1 {
		return 1
	}
	if int64(n) > int64(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(n)
}
