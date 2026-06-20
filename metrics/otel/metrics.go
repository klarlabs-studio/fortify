// Package otel provides an OpenTelemetry metrics adapter for Fortify
// resilience patterns.
//
// It is the metrics counterpart to the tracing-only go.klarlabs.de/fortify/otel
// package: where that package emits spans, this one emits counters,
// histograms, and gauges for circuit breakers, retries, rate limiters,
// timeouts, and bulkheads via the OpenTelemetry metric API.
//
// The instrument set mirrors the Prometheus collector in
// go.klarlabs.de/fortify/metrics so the two backends report the same signals
// under provider-appropriate names (dotted for OTel, snake_case for
// Prometheus).
//
// Sensitive payloads: like the tracing package, instruments carry only
// pattern names, bucket keys, and state labels. They never carry operation
// arguments, results, or wrapped payloads. Keep prompts, request bodies, PII,
// and credentials out of any custom attributes you add downstream.
//
// Example usage:
//
//	import (
//	    fortifyotel "go.klarlabs.de/fortify/metrics/otel"
//	    "go.opentelemetry.io/otel/sdk/metric"
//	)
//
//	reader := metric.NewManualReader()
//	provider := metric.NewMeterProvider(metric.WithReader(reader))
//	m, err := fortifyotel.NewMeter(provider)
//	// ...
//	m.RecordRetryDuration("planner", elapsed.Seconds())
package otel

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instrumentationName is the OTel instrumentation scope for all Fortify
// metrics.
const instrumentationName = "go.klarlabs.de/fortify/metrics/otel"

// Meter records Fortify resilience-pattern metrics through the OpenTelemetry
// metric API. Construct it with NewMeter and call the Record* methods from
// pattern callbacks. All methods are safe for concurrent use (the underlying
// OTel instruments are).
type Meter struct {
	// Circuit breaker.
	cbState        metric.Int64Gauge
	cbRequests     metric.Int64Counter
	cbFailures     metric.Int64Counter
	cbSuccesses    metric.Int64Counter
	cbStateChanges metric.Int64Counter

	// Retry.
	retryAttempts  metric.Float64Histogram
	retrySuccesses metric.Int64Counter
	retryFailures  metric.Int64Counter
	retryDuration  metric.Float64Histogram

	// Rate limit.
	rlAllowed  metric.Int64Counter
	rlDenied   metric.Int64Counter
	rlWaitTime metric.Float64Histogram

	// Timeout.
	timeoutExecutions metric.Int64Counter
	timeoutExceeded   metric.Int64Counter
	timeoutDuration   metric.Float64Histogram

	// Bulkhead.
	bulkheadActive    metric.Int64Gauge
	bulkheadQueued    metric.Int64Gauge
	bulkheadRejected  metric.Int64Counter
	bulkheadSuccesses metric.Int64Counter
	bulkheadFailures  metric.Int64Counter
	bulkheadDuration  metric.Float64Histogram
}

// NewMeter creates a Meter backed by the given MeterProvider. If provider is
// nil, the global MeterProvider (otel.GetMeterProvider) is used, so a process
// that has already configured OTel can call NewMeter(nil) and have metrics
// flow to the same pipeline.
//
// It returns an error only if instrument creation fails, which the OTel SDK
// reserves for genuinely invalid configurations.
func NewMeter(provider metric.MeterProvider) (*Meter, error) {
	if provider == nil {
		provider = otel.GetMeterProvider()
	}
	meter := provider.Meter(instrumentationName)

	m := &Meter{}
	var err error

	// errwrap captures the first instrument-creation error so the constructor
	// fails fast without a wall of repetitive nil checks.
	c := func(ctr metric.Int64Counter, e error) metric.Int64Counter {
		if err == nil {
			err = e
		}
		return ctr
	}
	g := func(gge metric.Int64Gauge, e error) metric.Int64Gauge {
		if err == nil {
			err = e
		}
		return gge
	}
	h := func(his metric.Float64Histogram, e error) metric.Float64Histogram {
		if err == nil {
			err = e
		}
		return his
	}

	m.cbState = g(meter.Int64Gauge("fortify.circuit_breaker.state",
		metric.WithDescription("Current circuit breaker state (0=closed, 1=open, 2=half-open)")))
	m.cbRequests = c(meter.Int64Counter("fortify.circuit_breaker.requests",
		metric.WithDescription("Total circuit breaker requests")))
	m.cbFailures = c(meter.Int64Counter("fortify.circuit_breaker.failures",
		metric.WithDescription("Total failed circuit breaker requests")))
	m.cbSuccesses = c(meter.Int64Counter("fortify.circuit_breaker.successes",
		metric.WithDescription("Total successful circuit breaker requests")))
	m.cbStateChanges = c(meter.Int64Counter("fortify.circuit_breaker.state_changes",
		metric.WithDescription("Total circuit breaker state changes")))

	m.retryAttempts = h(meter.Float64Histogram("fortify.retry.attempts",
		metric.WithDescription("Number of retry attempts made")))
	m.retrySuccesses = c(meter.Int64Counter("fortify.retry.successes",
		metric.WithDescription("Total successful retries")))
	m.retryFailures = c(meter.Int64Counter("fortify.retry.failures",
		metric.WithDescription("Total failed retries")))
	m.retryDuration = h(meter.Float64Histogram("fortify.retry.duration",
		metric.WithDescription("Duration of retry operations"), metric.WithUnit("s")))

	m.rlAllowed = c(meter.Int64Counter("fortify.rate_limit.allowed",
		metric.WithDescription("Total allowed rate-limited requests")))
	m.rlDenied = c(meter.Int64Counter("fortify.rate_limit.denied",
		metric.WithDescription("Total denied rate-limited requests")))
	m.rlWaitTime = h(meter.Float64Histogram("fortify.rate_limit.wait_duration",
		metric.WithDescription("Time spent waiting for a rate-limit token"), metric.WithUnit("s")))

	m.timeoutExecutions = c(meter.Int64Counter("fortify.timeout.executions",
		metric.WithDescription("Total timeout-guarded executions")))
	m.timeoutExceeded = c(meter.Int64Counter("fortify.timeout.exceeded",
		metric.WithDescription("Total executions that exceeded their timeout")))
	m.timeoutDuration = h(meter.Float64Histogram("fortify.timeout.duration",
		metric.WithDescription("Duration of timeout-guarded operations"), metric.WithUnit("s")))

	m.bulkheadActive = g(meter.Int64Gauge("fortify.bulkhead.active",
		metric.WithDescription("Current number of active bulkhead requests")))
	m.bulkheadQueued = g(meter.Int64Gauge("fortify.bulkhead.queued",
		metric.WithDescription("Current number of queued bulkhead requests")))
	m.bulkheadRejected = c(meter.Int64Counter("fortify.bulkhead.rejected",
		metric.WithDescription("Total rejected bulkhead requests")))
	m.bulkheadSuccesses = c(meter.Int64Counter("fortify.bulkhead.successes",
		metric.WithDescription("Total successful bulkhead requests")))
	m.bulkheadFailures = c(meter.Int64Counter("fortify.bulkhead.failures",
		metric.WithDescription("Total failed bulkhead requests")))
	m.bulkheadDuration = h(meter.Float64Histogram("fortify.bulkhead.duration",
		metric.WithDescription("Duration of bulkhead operations"), metric.WithUnit("s")))

	if err != nil {
		return nil, err
	}
	return m, nil
}

// nameAttr builds the common single-name attribute set.
func nameAttr(name string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("name", name))
}

// --- Circuit breaker ---

// RecordCircuitBreakerState records the current state as a gauge value
// (0=closed, 1=open, 2=half-open).
func (m *Meter) RecordCircuitBreakerState(name string, state int64) {
	m.cbState.Record(context.Background(), state, nameAttr(name))
}

// RecordCircuitBreakerRequest increments the request counter, labeled by the
// breaker's state at admission time.
func (m *Meter) RecordCircuitBreakerRequest(name, state string) {
	m.cbRequests.Add(context.Background(), 1,
		metric.WithAttributes(attribute.String("name", name), attribute.String("state", state)))
}

// RecordCircuitBreakerFailure increments the failure counter.
func (m *Meter) RecordCircuitBreakerFailure(name string) {
	m.cbFailures.Add(context.Background(), 1, nameAttr(name))
}

// RecordCircuitBreakerSuccess increments the success counter.
func (m *Meter) RecordCircuitBreakerSuccess(name string) {
	m.cbSuccesses.Add(context.Background(), 1, nameAttr(name))
}

// RecordCircuitBreakerStateChange increments the state-change counter, labeled
// with the from/to states.
func (m *Meter) RecordCircuitBreakerStateChange(name, from, to string) {
	m.cbStateChanges.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("name", name),
		attribute.String("from", from),
		attribute.String("to", to),
	))
}

// --- Retry ---

// RecordRetryAttempts records the number of attempts a retry sequence made.
func (m *Meter) RecordRetryAttempts(name string, attempts float64) {
	m.retryAttempts.Record(context.Background(), attempts, nameAttr(name))
}

// RecordRetrySuccess increments the retry-success counter.
func (m *Meter) RecordRetrySuccess(name string) {
	m.retrySuccesses.Add(context.Background(), 1, nameAttr(name))
}

// RecordRetryFailure increments the retry-failure counter.
func (m *Meter) RecordRetryFailure(name string) {
	m.retryFailures.Add(context.Background(), 1, nameAttr(name))
}

// RecordRetryDuration records the wall-clock duration of a retry sequence in
// seconds.
func (m *Meter) RecordRetryDuration(name string, seconds float64) {
	m.retryDuration.Record(context.Background(), seconds, nameAttr(name))
}

// --- Rate limit ---

// RecordRateLimitAllowed increments the allowed counter for the given key.
func (m *Meter) RecordRateLimitAllowed(name, key string) {
	m.rlAllowed.Add(context.Background(), 1, keyAttr(name, key))
}

// RecordRateLimitDenied increments the denied counter for the given key.
func (m *Meter) RecordRateLimitDenied(name, key string) {
	m.rlDenied.Add(context.Background(), 1, keyAttr(name, key))
}

// RecordRateLimitWaitTime records the time spent waiting for a token in
// seconds.
func (m *Meter) RecordRateLimitWaitTime(name, key string, seconds float64) {
	m.rlWaitTime.Record(context.Background(), seconds, keyAttr(name, key))
}

// keyAttr builds the (name, key) attribute set used by rate-limit instruments.
func keyAttr(name, key string) metric.MeasurementOption {
	return metric.WithAttributes(attribute.String("name", name), attribute.String("key", key))
}

// --- Timeout ---

// RecordTimeoutExecution increments the timeout-guarded execution counter.
func (m *Meter) RecordTimeoutExecution(name string) {
	m.timeoutExecutions.Add(context.Background(), 1, nameAttr(name))
}

// RecordTimeoutExceeded increments the timeout-exceeded counter.
func (m *Meter) RecordTimeoutExceeded(name string) {
	m.timeoutExceeded.Add(context.Background(), 1, nameAttr(name))
}

// RecordTimeoutDuration records the duration of a timeout-guarded operation in
// seconds, labeled by whether it exceeded the deadline.
func (m *Meter) RecordTimeoutDuration(name string, exceeded bool, seconds float64) {
	m.timeoutDuration.Record(context.Background(), seconds, metric.WithAttributes(
		attribute.String("name", name),
		attribute.Bool("exceeded", exceeded),
	))
}

// --- Bulkhead ---

// RecordBulkheadActive records the current number of active requests.
func (m *Meter) RecordBulkheadActive(name string, count int64) {
	m.bulkheadActive.Record(context.Background(), count, nameAttr(name))
}

// RecordBulkheadQueued records the current number of queued requests.
func (m *Meter) RecordBulkheadQueued(name string, count int64) {
	m.bulkheadQueued.Record(context.Background(), count, nameAttr(name))
}

// RecordBulkheadRejected increments the rejected counter.
func (m *Meter) RecordBulkheadRejected(name string) {
	m.bulkheadRejected.Add(context.Background(), 1, nameAttr(name))
}

// RecordBulkheadSuccess increments the success counter.
func (m *Meter) RecordBulkheadSuccess(name string) {
	m.bulkheadSuccesses.Add(context.Background(), 1, nameAttr(name))
}

// RecordBulkheadFailure increments the failure counter.
func (m *Meter) RecordBulkheadFailure(name string) {
	m.bulkheadFailures.Add(context.Background(), 1, nameAttr(name))
}

// RecordBulkheadDuration records the duration of a bulkhead operation in
// seconds.
func (m *Meter) RecordBulkheadDuration(name string, seconds float64) {
	m.bulkheadDuration.Record(context.Background(), seconds, nameAttr(name))
}
