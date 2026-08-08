# Concepts

How each Fortify pattern works, when to reach for it, and what its configuration knobs mean. If you want a recipe, see the how-to guides; if you want the API surface, see [pkg.go.dev](https://pkg.go.dev/go.klarlabs.de/fortify).

## Pattern decision tree

| Symptom in production                                          | Pattern        |
| -------------------------------------------------------------- | -------------- |
| Downstream is unhealthy and we're piling on more requests      | Circuit breaker |
| Calls fail intermittently with transient errors                | Retry          |
| Caller is sending more traffic than we can serve fairly        | Rate limit     |
| Caller never returns; we want to bound latency                 | Timeout        |
| Concurrent calls saturate a shared resource (DB pool, threads) | Bulkhead / Adaptive |
| Primary path failed; we have a degraded but acceptable substitute | Fallback   |
| p99 latency too high; primary call is slow but not failed         | Hedge      |

A composed chain typically uses several at once. See the [composition guide](how-to-compose.md) for ordering rationale.

## Circuit breaker

Prevents cascading failures by temporarily stopping requests to a failing dependency.

### States

```
   ┌──────┐  failures ≥ threshold   ┌──────┐
   │Closed├────────────────────────▶│ Open │
   └───▲──┘                         └───┬──┘
       │                                │
       │ all trial requests succeed     │ Timeout elapses
       │                                ▼
       │                          ┌──────────┐
       └──────────────────────────┤Half-Open │
              any failure         └──────────┘
```

- **Closed:** requests pass through; `Counts.ConsecutiveFailures` accumulates.
- **Open:** requests fail fast with `*ferrors.CircuitOpenError` (wraps `ferrors.ErrCircuitOpen`).
- **Half-Open:** at most `MaxRequests` trial requests are admitted. All success → Closed; any failure → Open.

### Knobs

| Field           | Meaning                                                                                  |
| --------------- | ---------------------------------------------------------------------------------------- |
| `MaxRequests`   | Trial requests allowed in Half-Open before deciding the verdict.                         |
| `Interval`      | Closed-state window for counting. Counts reset every `Interval`. Zero = never reset.     |
| `Timeout`       | How long the breaker stays Open before transitioning to Half-Open. Default 60s.          |
| `ReadyToTrip`   | Predicate called on each Closed-state failure; return `true` to trip Open. Defaults to `TripOnConsecutiveFailures(5)`. |
| `IsSuccessful`  | Predicate that classifies an `error` as success/failure (e.g., treat `404` as success). |
| `OnStateChange` | Async callback for transitions. Delivered in order via dispatcher goroutine.             |

### Trip policies

Two shipped predicates cover most `ReadyToTrip` implementations. Both take plain
`int` thresholds, so a threshold read from application config needs no `uint32`
conversion — and therefore no `//nolint:gosec` for
[G115](https://gosec.io/rules/#g115) — at the call site:

```go
// Opens after 3 failures in a row. Any success resets the streak.
ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(3)

// Opens at a 50% failure rate, once at least 20 requests are recorded.
ReadyToTrip: circuitbreaker.TripOnFailureRatio(0.5, 20)
```

Pick by failure shape:

| Downstream behaviour | Consecutive | Ratio |
| --- | --- | --- |
| Hard down, every call fails | trips | trips |
| Degraded, ~50% of calls fail | **never trips** — a success resets the streak | trips |
| One-off blip | does not trip | does not trip (below `minRequests`) |

`TripOnConsecutiveFailures` cannot see a partially failing downstream: a service
returning `F S F F S F` never accumulates a streak, so the breaker stays Closed
however long the degradation lasts. Reach for `TripOnFailureRatio` when
"degraded but not dead" is a state you need to catch.

Both evaluate over the breaker's counting window, which `Interval` resets
wholesale — a *tumbling* window, not a sliding one. A burst straddling a reset is
split into two sub-threshold halves.

Callers needing something else still get the raw `Counts`.

### When to **not** use a circuit breaker

- The downstream is owned by you and idempotent — a bounded retry may be more appropriate.
- The dependency has its own back-pressure (e.g., gRPC `RESOURCE_EXHAUSTED` with backoff). Stacking another CB only delays recovery.

## Retry

Retries an operation on retryable errors with configurable backoff.

### Backoff policies

- `BackoffConstant`: fixed delay every attempt.
- `BackoffLinear`: `delay = InitialDelay × attempt`.
- `BackoffExponential`: `delay = InitialDelay × Multiplier^(attempt-1)`. Capped at `MaxDelay` and at 24h to defend against `+Inf` overflow. `Multiplier` itself is capped at 100 in `setDefaults`.

`Jitter: true` adds 0–10% per-call randomness using `math/rand/v2` (lock-free, per-goroutine).

### Retryability rules

In priority order:

1. `Config.IsRetryable(err)` if set — wins outright.
2. `Config.NonRetryableErrors` — match via `errors.Is`; if matched, do not retry.
3. `Config.RetryableErrors` — match via `errors.Is`; if matched, retry; if list set but no match, do not retry.
4. `ferrors.IsRetryable(err)` — implements the `RetryableError` interface.
5. Default: retry all errors.

Wrap an error to mark it retryable: `ferrors.AsRetryable(err)`.

### When **not** to retry

- The operation is non-idempotent and side-effects matter on partial success (e.g., charging a card).
- The error is a 4xx (client-side) — retry won't change the outcome.
- The dependency is already a CB-protected one — retry-inside-CB is fine; retry-outside-CB causes retries to count against the breaker.

## Rate limit

Token bucket rate limiter with pluggable storage. Each key gets its own bucket; tokens refill at `Rate / Interval`, capped at `Burst`.

### Allow vs Wait vs Take vs Execute

| Method      | Blocking? | Tokens | Returns                   | When to use                                  |
| ----------- | --------- | ------ | ------------------------- | -------------------------------------------- |
| `Allow`     | No        | 1      | `bool`                    | Reject immediately if over budget.           |
| `Wait`      | Yes       | 1      | `error` (ctx or timeout)  | Block caller until budget free.              |
| `Take`      | No        | N      | `bool`                    | Bulk operations consuming N tokens.          |
| `Execute`   | No        | 1      | error (operation or `*rateLimitError`) | Gate a closure; uses Allow.       |
| `ExecuteN`  | No        | N      | error                     | Gate a closure consuming N tokens.           |

### Key handling

- **Caller-supplied key:** the simple case. `rl.Allow(ctx, "user-123")`.
- **`KeyFunc(ctx, key) string`:** transformer/extractor. Called with both the request context and the caller-supplied key. Use to derive from context, namespace, or override. See the [rate-limit guide](how-to-rate-limit.md).

### Fail-open vs fail-closed

When the underlying `Store` errors (e.g., Redis is unreachable):

- `FailOpen: false` (default): deny all requests. Favors consistency.
- `FailOpen: true`: allow requests up to `Burst` tokens. Favors availability. Oversize `Take` requests are still denied to prevent DoS amplification during outages.

## Timeout

Wraps a function with `context.WithTimeout`. The function must respect `ctx.Done()`; non-cooperative functions cannot be cancelled.

Returns `*ferrors.TimeoutError` on its own deadline; propagates the parent's error verbatim if the parent context cancelled first. `errors.Is(err, context.DeadlineExceeded)` continues to match either case.

## Bulkhead

Caps the number of in-flight operations using a semaphore. Optional bounded queue with per-request timeout.

### Capacity model

- `MaxConcurrent` worker slots; `MaxQueue` queue slots; total in-system = `MaxConcurrent + MaxQueue`.
- Requests above capacity reject with `*ferrors.ErrBulkheadFull`.
- `QueueTimeout` bounds how long a queued request waits before rejecting.

### When **not** to use a bulkhead

- The downstream already has a connection pool that does the same work — adding bulkhead just doubles queueing.
- You want fairness across keys — bulkhead is FIFO, not key-aware. Combine with rate limit instead.

## Fallback

Runs a primary; on failure (filtered by `ShouldFallback`), runs a fallback function with the original error in context.

### When fallback is the wrong shape

- The fallback returns stale data and your callers can't tolerate it.
- The fallback's failure mode is the same as the primary's — you're hiding the real problem.

## Hedge

Reduces tail latency by firing a parallel attempt if the primary doesn't return within `HedgeDelay`. First success wins; in-flight attempts are cancelled via shared context.

### When **not** to use hedge

- The operation is **non-idempotent**. Hedge attempts may run to completion in parallel before cancellation propagates; each must be safe to repeat.
- Latency is dominated by network throughput, not server queueing — hedging multiplies network load with no latency win.
- Cost-per-call is high (paid API, expensive compute). Hedge trades work for latency.

## Adaptive concurrency

Auto-tunes a concurrency cap at runtime instead of using a fixed bulkhead. Two algorithms.

### AIMD (default)

Failure-driven, like TCP congestion control:

- Every `SuccessThreshold` consecutive successes → `limit++` (additive increase).
- Any failure → `limit /= 2` (multiplicative decrease).

Predictable, lock-free, ignores latency. Reacts only when the downstream actually fails. Good when failures are a clean overload signal.

### Vegas (RTT-aware)

Latency-driven, inspired by TCP Vegas:

- Tracks `minRTT` (no-load baseline) and `emaRTT` (exponential moving average of recent latencies, halflife ≈ 8 samples).
- Computes a queue-depth estimate: `queue = currentLimit × (emaRTT − minRTT) / emaRTT`.
- `queue < VegasAlpha` (default 3) → `limit++`. System is underutilized.
- `queue > VegasBeta` (default 6) → `limit--`. Queueing is forming downstream.
- Failure still halves the limit (same as AIMD).

Reacts to rising latency *before* failures appear. Slightly more overhead per call (one `time.Now` + EMA update). Good when latency is a leading indicator of saturation; less appropriate when downstream RTT is naturally bimodal.

### When **not** to use adaptive

- You have a hard external constraint (DB pool size, license count) — overshoot is unacceptable. Use a static `bulkhead`.
- Failures aren't a saturation signal (e.g., business-logic errors). AIMD will halve the limit on every business error.
- RTT is naturally noisy or bimodal. Vegas's EMA may oscillate.

## Composition order

Outer-to-inner: `Bulkhead → RateLimit → CircuitBreaker → Retry → Timeout → operation`.

Rationale and edge cases (retry inside CB vs outside, etc.) in the [composition guide](how-to-compose.md).
