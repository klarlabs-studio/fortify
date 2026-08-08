# How to compose patterns

Patterns are most useful in combination. Fortify's `middleware.Chain` is a fluent builder for layering them in the right order.

## The layering rule

**The middleware added first is the outermost layer.** Each subsequent `With…`
call nests one level closer to the operation, which runs innermost. A chain
built as A, B, C executes `A → B → C → operation → C → B → A`.

This is worth stating plainly because "applied in the order they were added"
reads both ways, and getting it backwards is silent: the chain still compiles,
still returns correct results while the downstream is healthy, and only
misbehaves under load — as circuits that trip when they should not.

## Recommended chain

```go
import "go.klarlabs.de/fortify/middleware"

chain := middleware.New[Response]().
    WithBulkhead(bh).                       // outermost
    WithRateLimit(rl, "user-key").
    WithCircuitBreaker(cb).
    WithRetry(r).
    WithTimeout(tm, 5*time.Second)          // innermost (closest to operation)

result, err := chain.Execute(ctx, func(ctx context.Context) (Response, error) {
    return makeRequest(ctx)
})
```

Outer → inner: `Bulkhead → RateLimit → CircuitBreaker → Retry → Timeout → operation`.
This is the layering the shipped presets (`HTTPClient`, `DatabaseQuery`,
`RPCDownstream`, `LLMCall`) already apply — prefer one of those when it fits.

## Why this order

Reading outer → inner:

1. **Bulkhead first** — shed load before doing any other work. Cheapest rejection.
2. **Rate limit next** — enforce quota before consuming downstream budget. Above the breaker, so that blocking for a quota slot is not charged against an attempt's timeout budget.
3. **Circuit breaker before retry** — the breaker sees one outcome per *logical* call, so a transient failure that retry recovers from is recorded as a success, not as several failures.
4. **Retry inside the breaker** — retries re-run only the operation, not the surrounding patterns. A retry won't take a second bulkhead slot or a second rate-limit token.
5. **Timeout innermost** — bounds each individual attempt rather than the whole retry sequence, so a slow first attempt cannot consume the budget the remaining attempts need.

### What timeout innermost costs you

Worst-case chain latency becomes roughly `MaxAttempts × timeout` plus accumulated
backoff, not `timeout`. When the caller needs a hard ceiling on the whole
operation, put a deadline on the context you pass to `Execute`; `timeout.Execute`
detects a nearer parent deadline and propagates the parent's error verbatim.

The alternative — timeout *outside* the breaker — buys a guaranteed deadline even
if a Half-Open trial request stalls, at the cost of letting one slow attempt eat
the budget the retries needed. Fortify's presets take the per-attempt side of
that trade.

### Retry and `ErrCircuitOpen`

With retry inside the breaker, `ferrors.ErrCircuitOpen` never reaches retry: the
breaker rejects before the retry layer is entered. If you invert the order, check
how your `retry.Config` classifies it — retrying a rejection the breaker issued
on purpose only adds load and latency to a decision already made. See the
[retryability rules](concepts.md#retryability-rules).

## Order pitfalls

### Retry outside CB (wrong)

```go
chain := middleware.New[Response]().
    WithRetry(r).            // BAD: retries count against the CB
    WithCircuitBreaker(cb).
```

Each retry is a separate `cb.Execute` call. A flaky downstream causes the breaker to trip on the *retries*, not the originating failure rate.

Concretely, with `FailuresToTrip: 5` and `MaxAttempts: 3`: in the recommended
order two recovered blips are two successes and nothing happens. Inverted, they
are six consecutive failures and the circuit opens on a downstream that is in
fact healthy. Same config, opposite outcome — and `MaxAttempts` and
`FailuresToTrip` are chosen in different files by different people, so the
coupling is invisible unless you already know the layering. Any
`FailuresToTrip <= MaxAttempts` opens the circuit on a single flaky call.

### Bulkhead inside retry (wrong)

```go
chain := middleware.New[Response]().
    WithRetry(r).
    WithBulkhead(bh).        // BAD: each retry queues a new slot
```

Retries enqueue separately, defeating the bulkhead's "shed load on saturation" intent.

### Two timeouts (redundant)

If the surrounding context already has a deadline, the inner `Timeout` only adds value when its duration is shorter than the parent context's remaining time. Otherwise the parent cancels first and timeout's structured error is bypassed. (Fortify's `timeout.Execute` detects this and propagates the parent error verbatim.)

## Direct composition (without `middleware.Chain`)

If you don't need the fluent API:

```go
result, err := bh.Execute(ctx, func(ctx context.Context) (Response, error) {
    if !rl.Allow(ctx, key) {
        var zero Response
        return zero, ratelimit.ErrLimitExceeded
    }
    return cb.Execute(ctx, func(ctx context.Context) (Response, error) {
        return r.Execute(ctx, func(ctx context.Context) (Response, error) {
            return tm.Execute(ctx, 5*time.Second, makeRequest)
        })
    })
})
```

`middleware.Chain` is sugar over this. Use whichever is clearer at the call site.

## Pre-built bundles

Currently there are no pre-built bundles. If you find yourself recreating the same chain across services, consider extracting a helper in your codebase that returns a configured `middleware.Chain`.
