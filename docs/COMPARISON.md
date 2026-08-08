# Fortify vs. Alternatives

A pragmatic comparison of Fortify against the most commonly used Go resilience libraries. The goal is to help you pick the right tool for your situation, not to claim Fortify is universally better.

## TL;DR

| Tool                                | Patterns                                  | Generics | OTel built-in | Prometheus built-in | Composable middleware | Deps (core) | Status      |
| ----------------------------------- | ----------------------------------------- | -------- | ------------- | ------------------- | --------------------- | ----------- | ----------- |
| **Fortify**                         | CB, Retry, RL, Timeout, Bulkhead, Fallback | Yes      | Yes (`otel/`) | Yes (`metrics/`)    | Yes (`middleware/`)   | 0           | v1.x active |
| [`sony/gobreaker`][gb]              | CB                                        | No       | No            | No                  | No                    | 0           | v1.x stable |
| [`failsafe-go`][fs]                 | CB, Retry, RL, Timeout, Bulkhead, Fallback, Hedge | Generics | Hooks-only    | Hooks-only          | Yes (`Compose`)       | 0           | v0.x active |
| [`uber-go/ratelimit`][ul]           | RL                                        | No       | No            | No                  | No                    | 0           | v1.x stable |
| [`hashicorp/go-retryablehttp`][hr]  | Retry (HTTP-only)                         | No       | No            | No                  | n/a                   | 0           | v0.x stable |
| [`golang.org/x/time/rate`][xr]      | RL (token bucket)                         | No       | No            | No                  | n/a                   | stdlib      | stable      |

[gb]: https://github.com/sony/gobreaker
[fs]: https://github.com/failsafe-go/failsafe-go
[ul]: https://github.com/uber-go/ratelimit
[hr]: https://github.com/hashicorp/go-retryablehttp
[xr]: https://pkg.go.dev/golang.org/x/time/rate

## When to use what

**Use `sony/gobreaker`** if you need a circuit breaker only, want the smallest possible API surface, and don't need OTel/Prometheus integration. It is the de-facto Go CB and a fine choice when "CB only" is the brief.

**Use `golang.org/x/time/rate`** if you need a single-process token-bucket rate limiter and nothing else. It is in the standard module ecosystem, well-known, and dependency-free.

**Use `uber-go/ratelimit`** if you specifically need leaky-bucket (smoothed) rate limiting and don't care about distributed coordination.

**Use `failsafe-go`** if you want most of what Fortify provides plus hedged requests, and you're OK with a v0.x API and bring-your-own observability wiring.

**Use Fortify** if you want all six patterns under one roof with first-class observability (OTel spans, Prom histograms, slog correlation IDs) and a `middleware.Chain` API for composing them. Fortify's wedge is *composability + observability*, not "the best CB" or "the best RL" individually.

## Pattern-by-pattern

### Circuit breaker

| Feature                       | Fortify | gobreaker | failsafe-go |
| ----------------------------- | ------- | --------- | ----------- |
| Closed/Open/HalfOpen FSM      | Yes     | Yes       | Yes         |
| Generics on result type       | Yes     | No        | Yes         |
| Custom `IsSuccessful` predicate | Yes   | Yes       | Yes         |
| Sliding-window counts         | Yes (count- and time-based) | No | Yes (count- and time-based) |
| Failure-rate threshold + minimum calls | Yes | No   | Yes         |
| Lock-free fast path (steady-state Closed) | Yes  | No (RWMutex)  | n/a |
| Structured error (state, retry-after, counts) | Yes (`*CircuitOpenError`) | No (sentinel only) | No |
| OTel span attrs               | Yes (`otel/`) | No  | No        |
| Prometheus histogram          | Yes (`metrics/`) | No | No       |

### Retry

| Feature                       | Fortify | retryablehttp | failsafe-go |
| ----------------------------- | ------- | ------------- | ----------- |
| Generics on result type       | Yes     | n/a (HTTP)    | Yes         |
| Backoff: const/linear/exp     | Yes     | Yes           | Yes         |
| Jitter                        | Yes     | Yes           | Yes         |
| Retryable / non-retryable error lists | Yes | No        | Yes         |
| `RetryableError` interface    | Yes     | No            | No          |
| Multiplier overflow safe      | Yes (cap @ 100) | n/a       | n/a         |
| `time.NewTimer` reuse (no per-attempt alloc) | Yes | No | No |

### Rate limit

| Feature                       | Fortify | uber-go/ratelimit | x/time/rate | failsafe-go |
| ----------------------------- | ------- | ----------------- | ----------- | ----------- |
| Token bucket                  | Yes     | No (leaky bucket) | Yes         | Yes         |
| Per-key buckets               | Yes     | No                | No          | Yes         |
| Pluggable `Store` (Redis-able) | Yes    | No                | No          | No          |
| Distributed-aware (atomic update protocol) | Yes | No   | No          | No          |
| `MaxTokensPerRequest` cap     | Yes     | No                | No          | No          |
| Fail-open / fail-closed       | Yes (configurable) | n/a    | n/a         | n/a         |
| `Burst` cap on fail-open      | Yes (DoS-safe)     | n/a    | n/a         | n/a         |
| Structured error with `RetryAfter` | Yes | No               | No          | No          |
| Unicode NFC key normalization | Yes     | No                | No          | No          |
| Log-injection key sanitization| Yes     | No                | No          | No          |

### Timeout / Bulkhead / Fallback

These three patterns exist in Fortify and failsafe-go. `gobreaker`/`retryablehttp`/`uber-go/ratelimit` do not provide them.

## Composability

Fortify's `middleware.Chain` lets you compose patterns explicitly with a fluent API:

```go
chain := middleware.Chain[Response]().
    Bulkhead(bh).      // outermost: shed load before any other work
    CircuitBreaker(cb).// next: fail fast when downstream is unhealthy
    RateLimit(rl).     // next: cap throughput
    Retry(r).          // next: retry transient failures
    Timeout(tm)        // innermost: cap individual call latency

result, err := chain.Execute(ctx, key, func(ctx context.Context) (Response, error) {
    return callDownstream(ctx)
})
```

`failsafe-go` has a similar `Compose` function. `gobreaker` requires you to hand-roll the composition.

## Observability

Fortify ships first-class adapters:

- **`otel/`** — auto-creates spans for each pattern with named attributes (`fortify.cb.state`, `fortify.retry.attempt`, etc.)
- **`metrics/`** — Prometheus collectors for request counts, durations, state transitions, retry attempts, denied tokens
- **`slog/`** — structured logging with correlation IDs propagated via context
- **`testing/`** — chaos utilities (latency injection, error injection) for verifying resilience under fault conditions

`failsafe-go` exposes hooks (`OnFailure`, `OnRetry`, etc.) that you wire to your own OTel/Prom code. `gobreaker` and `uber-go/ratelimit` have no observability story beyond the user-supplied callback.

## Deliberate non-goals

- **Hedged requests** — failsafe-go has them; Fortify does not (yet).
- **Adaptive concurrency** — Netflix-style concurrency-limits are not implemented; bulkhead provides static limits only.
- **Service mesh integration** — Fortify is a Go library, not an Envoy/Istio replacement.
- **Cross-process coordination** — rate limit `Store` interface admits Redis/DynamoDB backends, but Fortify ships only the in-memory `MemoryStore`. Distributed CB is out of scope.

## Maturity & risk

- **API stability:** Fortify is at v1.x. The recent commits include breaking changes; treat the API as still-iterating until 1.x is settled. Pin a minor version.
- **Bus factor:** single maintainer. See [GOVERNANCE.md](../GOVERNANCE.md).
- **Production use:** see [ADOPTERS.md](../ADOPTERS.md). PRs welcome.
- **Security:** see [SECURITY.md](../SECURITY.md). Releases ship CycloneDX + SPDX SBOMs; CI runs nox security scanning + Dependabot + dependency-review-action.

## When Fortify is the wrong choice

- You need only a circuit breaker, and `sony/gobreaker` is already in your import graph. Adding Fortify just to wrap a CB is overkill.
- You're operating under hard binary-size or import-graph constraints. `gobreaker` core is smaller.
- You need hedged requests today. Use `failsafe-go`.
- You require a battle-tested distributed rate limiter. Fortify's distributed `Store` interface exists but the production-grade Redis adapter is not bundled; `redis_rate` may be a faster path.
