// Sample app for the observability demo: drives synthetic load
// through a Fortify chain, records the resulting metrics with the
// fortify/metrics collector, and exposes /metrics for Prometheus.
//
// Run via the docker-compose stack in this directory:
//
//	docker compose up --build
//
// then watch the "Fortify overview" dashboard at http://localhost:3000.
package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/ferrors"
	"go.klarlabs.de/fortify/metrics"
	"go.klarlabs.de/fortify/middleware"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
)

const cbName = "downstream"

// healthcheck probes the running app's /healthz endpoint. The runtime image
// is distroless (no shell, wget or curl), so container health checks invoke
// the binary itself: `/sample-app healthcheck`.
func healthcheck() int {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:8080/healthz")
	if err != nil {
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	collector := metrics.NewCollector()
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector)

	cb := circuitbreaker.New[string](circuitbreaker.Config{
		MaxRequests: 3,
		Interval:    30 * time.Second,
		Timeout:     5 * time.Second,
		ReadyToTrip: func(c circuitbreaker.Counts) bool { return c.ConsecutiveFailures >= 4 },
		OnStateChange: func(from, to circuitbreaker.State) {
			collector.RecordCircuitBreakerStateChange(cbName, from.String(), to.String())
			collector.RecordCircuitBreakerState(cbName, float64(to))
		},
	})

	r := retry.New[string](retry.Config{
		MaxAttempts:   3,
		InitialDelay:  50 * time.Millisecond,
		BackoffPolicy: retry.BackoffExponential,
		Multiplier:    2.0,
		Jitter:        true,
	})

	tm := timeout.New[string](timeout.Config{DefaultTimeout: 250 * time.Millisecond})

	chain := middleware.New[string]().
		WithCircuitBreaker(cb).
		WithRetry(r).
		WithTimeout(tm, 250*time.Millisecond)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              ":8080",
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go driveLoad(chain, collector)

	log.Println("listening on :8080")
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

// driveLoad fires one synthetic call per tick. The downstream
// occasionally errors and occasionally stalls, exercising every panel
// on the demo dashboard within a few seconds.
func driveLoad(chain *middleware.Chain[string], collector *metrics.Collector) {
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()

	for range tick.C {
		ctx := context.Background()
		start := time.Now()

		_, err := chain.Execute(ctx, func(ctx context.Context) (string, error) {
			roll := rand.IntN(10)
			switch {
			case roll < 2:
				return "", errors.New("simulated downstream 5xx")
			case roll < 3:
				select {
				case <-ctx.Done():
					return "", ctx.Err()
				case <-time.After(time.Second):
					return "ok", nil
				}
			default:
				return "ok", nil
			}
		})

		dur := time.Since(start).Seconds()
		collector.RecordCircuitBreakerRequest(cbName, "closed")
		if err == nil {
			collector.RecordCircuitBreakerSuccess(cbName)
			collector.RecordRetrySuccess(cbName)
		} else {
			collector.RecordCircuitBreakerFailure(cbName)
			collector.RecordRetryFailure(cbName)
			if errors.Is(err, ferrors.ErrTimeout) {
				collector.RecordTimeoutExceeded(cbName)
			}
		}
		collector.RecordRetryDuration(cbName, dur)
	}
}
