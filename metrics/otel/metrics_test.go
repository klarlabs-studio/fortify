package otel_test

import (
	"context"
	"testing"

	otelmetrics "go.klarlabs.de/fortify/metrics/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collect gathers the current metric set from a manual reader.
func collect(t *testing.T, reader metric.Reader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	return rm
}

// findInstrument returns the metricdata for the named instrument, or fails.
func findInstrument(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("instrument %q not found in collected metrics", name)
	return metricdata.Metrics{}
}

func newMeter(t *testing.T) (*otelmetrics.Meter, metric.Reader) {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	m, err := otelmetrics.NewMeter(provider)
	if err != nil {
		t.Fatalf("NewMeter: %v", err)
	}
	return m, reader
}

func TestNewMeter_NilProviderUsesGlobal(t *testing.T) {
	if _, err := otelmetrics.NewMeter(nil); err != nil {
		t.Fatalf("NewMeter(nil) should succeed using the global provider, got %v", err)
	}
}

func TestMeter_RecordsCircuitBreakerCounters(t *testing.T) {
	m, reader := newMeter(t)

	m.RecordCircuitBreakerSuccess("svc")
	m.RecordCircuitBreakerSuccess("svc")
	m.RecordCircuitBreakerFailure("svc")
	m.RecordCircuitBreakerStateChange("svc", "closed", "open")

	rm := collect(t, reader)

	succ := findInstrument(t, rm, "fortify.circuit_breaker.successes")
	sum, ok := succ.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("successes is not Sum[int64]: %T", succ.Data)
	}
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	if total != 2 {
		t.Errorf("circuit breaker successes = %d, want 2", total)
	}

	findInstrument(t, rm, "fortify.circuit_breaker.failures")
	findInstrument(t, rm, "fortify.circuit_breaker.state_changes")
}

func TestMeter_RecordsRetryHistogram(t *testing.T) {
	m, reader := newMeter(t)

	m.RecordRetryAttempts("svc", 3)
	m.RecordRetryDuration("svc", 0.5)
	m.RecordRetrySuccess("svc")

	rm := collect(t, reader)

	attempts := findInstrument(t, rm, "fortify.retry.attempts")
	hist, ok := attempts.Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("retry attempts is not Histogram[float64]: %T", attempts.Data)
	}
	if len(hist.DataPoints) == 0 {
		t.Fatalf("retry attempts has no data points")
	}
	if hist.DataPoints[0].Count != 1 {
		t.Errorf("retry attempts count = %d, want 1", hist.DataPoints[0].Count)
	}

	findInstrument(t, rm, "fortify.retry.duration")
	findInstrument(t, rm, "fortify.retry.successes")
}

func TestMeter_RecordsRateLimitAndTimeout(t *testing.T) {
	m, reader := newMeter(t)

	m.RecordRateLimitAllowed("svc", "user-1")
	m.RecordRateLimitDenied("svc", "user-1")
	m.RecordRateLimitWaitTime("svc", "user-1", 0.01)
	m.RecordTimeoutExecution("svc")
	m.RecordTimeoutExceeded("svc")
	m.RecordTimeoutDuration("svc", true, 1.0)

	rm := collect(t, reader)

	findInstrument(t, rm, "fortify.rate_limit.allowed")
	findInstrument(t, rm, "fortify.rate_limit.denied")
	findInstrument(t, rm, "fortify.rate_limit.wait_duration")
	findInstrument(t, rm, "fortify.timeout.executions")
	findInstrument(t, rm, "fortify.timeout.exceeded")
	findInstrument(t, rm, "fortify.timeout.duration")
}

func TestMeter_RecordsBulkheadGaugesAndCounters(t *testing.T) {
	m, reader := newMeter(t)

	m.RecordBulkheadActive("svc", 5)
	m.RecordBulkheadQueued("svc", 2)
	m.RecordBulkheadRejected("svc")
	m.RecordBulkheadSuccess("svc")
	m.RecordBulkheadFailure("svc")
	m.RecordBulkheadDuration("svc", 0.25)

	rm := collect(t, reader)

	findInstrument(t, rm, "fortify.bulkhead.active")
	findInstrument(t, rm, "fortify.bulkhead.queued")
	findInstrument(t, rm, "fortify.bulkhead.rejected")
	findInstrument(t, rm, "fortify.bulkhead.successes")
	findInstrument(t, rm, "fortify.bulkhead.failures")
	findInstrument(t, rm, "fortify.bulkhead.duration")
}

func TestMeter_RecordsCircuitBreakerState(t *testing.T) {
	m, reader := newMeter(t)

	m.RecordCircuitBreakerState("svc", 1)
	m.RecordCircuitBreakerRequest("svc", "closed")

	rm := collect(t, reader)
	findInstrument(t, rm, "fortify.circuit_breaker.state")
	findInstrument(t, rm, "fortify.circuit_breaker.requests")
}
