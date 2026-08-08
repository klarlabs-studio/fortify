package circuitbreaker

import "time"

// SlidingWindowType selects how the breaker aggregates outcomes when
// failure-rate tripping is enabled. The zero value leaves the sliding
// window off, so Config.ReadyToTrip governs tripping as before.
type SlidingWindowType int

const (
	// SlidingWindowDisabled is the zero value: no sliding window. The
	// breaker trips via Config.ReadyToTrip, which defaults to
	// TripOnConsecutiveFailures(DefaultFailuresToTrip).
	SlidingWindowDisabled SlidingWindowType = iota

	// SlidingWindowCount aggregates the last SlidingWindowSize calls,
	// as a circular buffer of outcomes. Equivalent to Resilience4j's
	// COUNT_BASED window.
	SlidingWindowCount

	// SlidingWindowTime aggregates calls recorded in the last
	// SlidingWindowSize seconds, in one-second buckets. Equivalent to
	// Resilience4j's TIME_BASED window. Preferred for high-traffic
	// services, where a count window fills fast enough to make the
	// breaker jumpy.
	SlidingWindowTime
)

// String returns the string representation of the SlidingWindowType.
func (t SlidingWindowType) String() string {
	switch t {
	case SlidingWindowDisabled:
		return "disabled"
	case SlidingWindowCount:
		return "count-based"
	case SlidingWindowTime:
		return "time-based"
	default:
		return "unknown"
	}
}

// Sliding-window defaults, matching Resilience4j where it has an opinion.
const (
	// DefaultCountWindowSize is the number of calls aggregated when
	// SlidingWindowCount is selected without a SlidingWindowSize.
	DefaultCountWindowSize = 100

	// DefaultTimeWindowSeconds is the number of seconds aggregated when
	// SlidingWindowTime is selected without a SlidingWindowSize.
	DefaultTimeWindowSeconds = 60

	// DefaultMinimumCalls is the number of calls that must be recorded
	// before a failure rate is evaluated, so a cold breaker cannot open
	// on one unlucky request.
	DefaultMinimumCalls = 10

	// DefaultFailureRateThreshold is the failure ratio at which the
	// circuit opens when none is configured.
	DefaultFailureRateThreshold = 0.5

	// MaxSlidingWindowSize bounds the window so a mistyped config cannot
	// allocate an unreasonable buffer.
	MaxSlidingWindowSize = 1_000_000
)

// slidingWindow aggregates recent call outcomes. Implementations are not
// safe for concurrent use; the breaker calls them under its own mutex.
type slidingWindow interface {
	// record adds one outcome to the window.
	record(success bool, now time.Time)
	// stats returns the calls currently inside the window and how many
	// of them failed.
	stats(now time.Time) (calls, failures int)
	// reset clears the window, discarding all recorded outcomes.
	reset()
}

// newSlidingWindow builds the window described by cfg, or nil when no
// sliding window is configured. cfg must already have been normalized.
func newSlidingWindow(cfg *Config) slidingWindow {
	switch cfg.SlidingWindowType {
	case SlidingWindowCount:
		return newCountWindow(cfg.SlidingWindowSize)
	case SlidingWindowTime:
		return newTimeWindow(cfg.SlidingWindowSize)
	case SlidingWindowDisabled:
		return nil
	default:
		return nil
	}
}

// countWindow keeps the last size outcomes in a circular buffer, so there
// is no boundary for a burst of failures to straddle. Failures are
// tracked incrementally, making both record and stats O(1).
type countWindow struct {
	outcomes []bool // true == failure
	size     int
	next     int
	filled   int
	failures int
}

func newCountWindow(size int) *countWindow {
	return &countWindow{
		outcomes: make([]bool, size),
		size:     size,
	}
}

func (w *countWindow) record(success bool, _ time.Time) {
	if w.filled == w.size && w.outcomes[w.next] {
		// Evicting a failure; drop it from the running total.
		w.failures--
	}
	failure := !success
	w.outcomes[w.next] = failure
	if failure {
		w.failures++
	}
	w.next = (w.next + 1) % w.size
	if w.filled < w.size {
		w.filled++
	}
}

func (w *countWindow) stats(_ time.Time) (calls, failures int) {
	return w.filled, w.failures
}

func (w *countWindow) reset() {
	clear(w.outcomes)
	w.next = 0
	w.filled = 0
	w.failures = 0
}

// timeBucket aggregates one second of outcomes. epoch identifies which
// second, so a stale bucket is recognised and cleared rather than
// double-counted when the ring wraps.
type timeBucket struct {
	epoch    int64
	calls    int
	failures int
}

// timeWindow keeps the last size seconds of outcomes as a ring of
// one-second buckets (Resilience4j's partial aggregation). Buckets older
// than the window are ignored by stats and overwritten by record, so
// nothing is lost across a boundary the way a tumbling reset loses it.
type timeWindow struct {
	buckets []timeBucket
	size    int
}

func newTimeWindow(sizeSeconds int) *timeWindow {
	return &timeWindow{
		buckets: make([]timeBucket, sizeSeconds),
		size:    sizeSeconds,
	}
}

// bucketFor returns the bucket owning now, clearing it first if it still
// holds a previous lap around the ring.
func (w *timeWindow) bucketFor(now time.Time) *timeBucket {
	epoch := now.Unix()
	idx := int(((epoch % int64(w.size)) + int64(w.size)) % int64(w.size))
	b := &w.buckets[idx]
	if b.epoch != epoch {
		b.epoch = epoch
		b.calls = 0
		b.failures = 0
	}
	return b
}

func (w *timeWindow) record(success bool, now time.Time) {
	b := w.bucketFor(now)
	b.calls++
	if !success {
		b.failures++
	}
}

func (w *timeWindow) stats(now time.Time) (calls, failures int) {
	oldest := now.Unix() - int64(w.size) + 1
	for i := range w.buckets {
		b := &w.buckets[i]
		if b.calls == 0 || b.epoch < oldest || b.epoch > now.Unix() {
			continue
		}
		calls += b.calls
		failures += b.failures
	}
	return calls, failures
}

func (w *timeWindow) reset() {
	clear(w.buckets)
}

// normalizeSlidingWindow clamps the sliding-window fields to usable
// values. It is a no-op when no sliding window is configured.
func (c *Config) normalizeSlidingWindow() {
	if c.SlidingWindowType == SlidingWindowDisabled {
		return
	}
	if c.SlidingWindowType != SlidingWindowCount && c.SlidingWindowType != SlidingWindowTime {
		c.SlidingWindowType = SlidingWindowDisabled
		return
	}

	if c.SlidingWindowSize <= 0 {
		if c.SlidingWindowType == SlidingWindowCount {
			c.SlidingWindowSize = DefaultCountWindowSize
		} else {
			c.SlidingWindowSize = DefaultTimeWindowSeconds
		}
	}
	if c.SlidingWindowSize > MaxSlidingWindowSize {
		c.SlidingWindowSize = MaxSlidingWindowSize
	}

	if c.MinimumCalls <= 0 {
		c.MinimumCalls = DefaultMinimumCalls
	}
	// A count window holds at most SlidingWindowSize outcomes, so a
	// larger MinimumCalls could never be reached and the breaker would
	// silently never trip.
	if c.SlidingWindowType == SlidingWindowCount && c.MinimumCalls > c.SlidingWindowSize {
		c.MinimumCalls = c.SlidingWindowSize
	}

	if c.FailureRateThreshold <= 0 || c.FailureRateThreshold > 1 {
		c.FailureRateThreshold = DefaultFailureRateThreshold
	}
}

// windowTrips reports whether the recorded window now justifies opening
// the circuit. Must be called with cb.mu held.
func (cb *circuitBreaker[T]) windowTrips(now time.Time) bool {
	calls, failures := cb.window.stats(now)
	if calls < cb.config.MinimumCalls {
		return false
	}
	return float64(failures)/float64(calls) >= cb.config.FailureRateThreshold
}
