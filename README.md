<div align="center">
  <img src="assets/fortify.png" alt="Fortify Logo" width="200"/>
  <h1>Fortify</h1>
</div>

[![Go Reference](https://pkg.go.dev/badge/go.klarlabs.de/fortify.svg)](https://pkg.go.dev/go.klarlabs.de/fortify)
[![Go Report Card](https://goreportcard.com/badge/go.klarlabs.de/fortify)](https://goreportcard.com/report/go.klarlabs.de/fortify)
[![CI Status](https://github.com/klarlabs-studio/fortify/workflows/CI/badge.svg)](https://github.com/klarlabs-studio/fortify/actions/workflows/ci.yml)
[![Coverage](./assets/coverage-badge.svg)](./assets/coverage-badge.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Go Version](https://img.shields.io/github/go-mod/go-version/klarlabs-studio/fortify)](https://github.com/klarlabs-studio/fortify)
[![Release](https://img.shields.io/github/v/release/klarlabs-studio/fortify)](https://github.com/klarlabs-studio/fortify/releases)

Composable resilience patterns for Go: circuit breaker, retry, rate limit, timeout, bulkhead, fallback, hedge, adaptive concurrency. First-class observability via OpenTelemetry, Prometheus, and `slog`. Zero dependencies in the core.

## Install

```bash
go get go.klarlabs.de/fortify
```

Minimum Go version is declared in [`go.mod`](./go.mod). The Go Version badge above always reflects the current value.

## 60-second quick start

Wrap an outbound call with circuit breaker + retry + timeout in one line, using a preset.

```go
package main

import (
    "context"
    "log"
    "time"

    "go.klarlabs.de/fortify/middleware"
)

type Response struct {
    Body string
}

func callDownstream(ctx context.Context) (Response, error) {
    // your real client call here
    return Response{Body: "ok"}, nil
}

func main() {
    chain, err := middleware.RPCDownstream[Response](middleware.RPCDownstreamConfig{
        CallTimeout: time.Second,
    })
    if err != nil {
        log.Fatal(err)
    }

    result, err := chain.Execute(context.Background(), callDownstream)
    log.Printf("result=%+v err=%v", result, err)
}
```

A `Response` struct is used instead of a bare `string` so the example mirrors what real services actually return — your handler will likely look closer to this than to the toy `[string]` form.

For a hand-rolled chain combining all eight patterns, see [`examples/composition`](./examples/composition/). For deciding which pattern fits which symptom, see the [pattern decision tree](docs/concepts.md#pattern-decision-tree).

## Why Fortify

Most Go resilience libraries cover a single pattern. Stitching together a circuit breaker (`sony/gobreaker`), a retry policy (`hashicorp/go-retryablehttp`), and a rate limiter (`golang.org/x/time/rate`) means three different APIs, three different observability stories, and ad-hoc composition.

Fortify is the resilience library for teams that want **all of it under one roof**, with consistent ergonomics and observability built in.

See [docs/COMPARISON.md](docs/COMPARISON.md) for a detailed comparison against `sony/gobreaker`, `failsafe-go`, `uber-go/ratelimit`, `golang.org/x/time/rate`, and `hashicorp/go-retryablehttp`. See [docs/POSITIONING.md](docs/POSITIONING.md) for the project's wedge and validation gates.

## Patterns at a glance

| Pattern         | Package           | When to use                                                       |
| --------------- | ----------------- | ----------------------------------------------------------------- |
| Circuit breaker | `circuitbreaker/` | Stop hammering an unhealthy downstream                            |
| Retry           | `retry/`          | Recover from transient failures with backoff                      |
| Rate limit      | `ratelimit/`      | Cap requests per key (token bucket, pluggable storage)            |
| Timeout         | `timeout/`        | Bound operation latency                                           |
| Bulkhead        | `bulkhead/`       | Cap concurrency to prevent resource exhaustion                    |
| Fallback        | `fallback/`       | Graceful degradation when the primary path fails                  |
| Hedge           | `hedge/`          | Reduce tail latency by firing parallel attempts on slow primary   |
| Adaptive concurrency | `adaptive/`  | AIMD / Vegas / Gradient2 auto-tuning of concurrency cap            |

For the semantics behind each pattern see [docs/concepts.md](docs/concepts.md).

## Pre-built bundles

For common shapes, use a preset instead of hand-rolling a chain:

```go
// Outbound HTTP client with CB + retry + timeout
chain, _ := middleware.HTTPClient(middleware.HTTPClientConfig{Timeout: 5 * time.Second})

// As an http.RoundTripper, mountable on http.Client.Transport
rt, _ := middleware.HTTPRoundTripper(nil, middleware.HTTPClientConfig{Timeout: 5 * time.Second})

// Database query with conservative retry
chain, _ := middleware.DatabaseQuery(middleware.DatabaseQueryConfig{QueryTimeout: 200 * time.Millisecond})

// Per-downstream RPC chain (one chain per downstream)
chain, _ := middleware.RPCDownstream[Response](middleware.RPCDownstreamConfig{CallTimeout: 1 * time.Second})

// Server-side handler wrapper (rate limit + CB + timeout)
h, _ := middleware.HTTPHandler(myHandler, middleware.HTTPHandlerConfig{Timeout: 1 * time.Second})
```

Presets are starting points. Build your own `middleware.Chain` when the preset doesn't fit.

## Composition

Combine patterns via `middleware.Chain`:

```go
import "go.klarlabs.de/fortify/middleware"

chain := middleware.New[Response]().
    WithBulkhead(bh).
    WithRateLimit(rl, "user-key").
    WithTimeout(tm, 5*time.Second).
    WithCircuitBreaker(cb).
    WithRetry(r)

result, err := chain.Execute(ctx, func(ctx context.Context) (Response, error) {
    return makeRequest(ctx)
})
```

Order matters. Outer-to-inner: `Bulkhead → RateLimit → Timeout → CircuitBreaker → Retry → operation`. Rationale and pitfalls in [docs/how-to-compose.md](docs/how-to-compose.md).

## Integrations

- HTTP middleware (`fortify/http`): `RateLimit`, `Timeout`, `CircuitBreaker` decorators
- gRPC interceptors (`fortify/grpc`): unary + streaming
- OpenTelemetry tracing (`fortify/otel`) and metrics (`fortify/metrics/otel`)
- Prometheus metrics (`fortify/metrics`)
- Structured logging (`fortify/slog`)
- Chaos testing (`fortify/testing`)

See [docs/integrations.md](docs/integrations.md) for HTTP and gRPC, [docs/how-to-observe.md](docs/how-to-observe.md) for telemetry.

## Performance

Fast paths are designed to be sub-microsecond and zero-alloc. Apple M5, Go 1.25:

| Pattern (steady-state) | Overhead | Allocs |
| ---------------------- | -------- | ------ |
| Circuit breaker (Closed, lock-free) | ~70ns | 0 |
| Retry (no retry needed) | ~25ns | 0 |
| Rate limit `Allow` (in-process Store) | ~200ns | 3 |
| Timeout | ~50ns | 0 |
| Bulkhead `Execute` (slot available) | ~39ns | 0 |

The circuit breaker takes a lock-free fast path in steady-state Closed (atomic mirrors of state, expiry, generation). Concurrent measurements (10 goroutines): ~187ns/op, 0 allocs.

## Documentation

- **Concepts** — [docs/concepts.md](docs/concepts.md) — what each pattern does and when to use it
- **How-to: compose** — [docs/how-to-compose.md](docs/how-to-compose.md) — chain ordering, pitfalls
- **How-to: observe** — [docs/how-to-observe.md](docs/how-to-observe.md) — `slog`, OTel, Prometheus
- **How-to: rate limit** — [docs/how-to-rate-limit.md](docs/how-to-rate-limit.md) — per-key, custom Store, KeyFunc
- **How-to: test** — [docs/how-to-test.md](docs/how-to-test.md) — chaos utilities, regression testing
- **Integrations** — [docs/integrations.md](docs/integrations.md) — HTTP and gRPC
- **Production checklist** — [docs/PRODUCTION.md](docs/PRODUCTION.md)
- **Error handling** — [docs/ERROR_HANDLING.md](docs/ERROR_HANDLING.md)
- **Migration notes** — [docs/MIGRATION.md](docs/MIGRATION.md)
- **API reference** — [pkg.go.dev](https://pkg.go.dev/go.klarlabs.de/fortify)

## Project governance

- [GOVERNANCE.md](GOVERNANCE.md) — maintainership, decision-making, semver policy
- [ADOPTERS.md](ADOPTERS.md) — production users; PRs welcome
- [SECURITY.md](SECURITY.md) — vulnerability disclosure
- [CHANGELOG.md](CHANGELOG.md) — release notes

## Examples

- [Basic patterns](./examples/basic/) — one file per pattern
- [HTTP server](./examples/http/) — middleware integration
- [Composition](./examples/composition/) — full chain in production-shape
- [MCP server](./examples/mcp-server/) — resilience for an MCP tool handler
- [Eino + LLMCall](./examples/eino/) — wrap an Eino chat model with cost-budgeted resilience
- [Observability demo](./examples/observability-demo/) — Prometheus + Grafana stack with a pre-built Fortify dashboard (`docker compose up --build`)

## Contributing

PRs welcome. Please:

1. Open an issue for non-trivial changes before writing code
2. Add tests with `-race` for new functionality
3. Run `go test -race ./...` and `golangci-lint run` before pushing

## License

MIT — see [LICENSE](LICENSE).

## Acknowledgments

Concepts borrowed from [Hystrix](https://github.com/Netflix/Hystrix) (Java/Netflix), [resilience4j](https://github.com/resilience4j/resilience4j) (Java), and [Polly](https://github.com/App-vNext/Polly) (.NET). Closest Go analogue: [failsafe-go](https://github.com/failsafe-go/failsafe-go); see the [comparison](docs/COMPARISON.md).

## Support

- [Issues](https://github.com/klarlabs-studio/fortify/issues) — bug reports and feature requests
- [Discussions](https://github.com/klarlabs-studio/fortify/discussions) — questions and design conversations
- [API reference](https://pkg.go.dev/go.klarlabs.de/fortify) — pkg.go.dev
