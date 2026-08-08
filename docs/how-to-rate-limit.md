# How to rate-limit

Recipes for the most common rate-limiting layouts. For pattern semantics see [concepts.md](concepts.md).

## Global limit (single bucket)

```go
rl := ratelimit.New(ratelimit.Config{
    Rate:     1000,
    Burst:    1500,
    Interval: time.Second,
})

if !rl.Allow(ctx, "global") {
    // throttled
}
```

## Per-minute (and per-hour) quotas

`Rate` is a count **per `Interval`**, not per second. `Interval` carries the
unit, so a quota published per minute — the form most LLM and third-party APIs
use — transcribes directly, with no per-second arithmetic:

```go
// Provider tier documented as "50 RPM".
rl := ratelimit.New(ratelimit.Config{
    Rate:     50,
    Interval: time.Minute,
    Burst:    10, // cap how much of the quota a single spike may claim
})
```

| Quota            | `Rate` | `Interval`    |
| ---------------- | ------ | ------------- |
| 10 req/second    | `10`   | `time.Second` |
| 50 req/minute    | `50`   | `time.Minute` |
| 4000 req/minute  | `4000` | `time.Minute` |
| 500 req/hour     | `500`  | `time.Hour`   |

Widening `Interval` is how sustained rates **below one request per second** are
expressed; `Rate` never needs to be fractional. `Rate: 50, Interval: time.Minute`
is 0.83 req/sec, which no integer `Rate` at `Interval: time.Second` could state.

Refill is continuous, not stepped: the example above returns roughly one token
every 1.2s rather than 50 tokens at the top of each minute. `Burst` is the bucket
capacity and therefore the largest spike allowed — set it equal to `Rate` for a
strict quota, or lower to smooth traffic further.

`Interval` is clamped to `[MinInterval, MaxInterval]` (1ms to 24h).

## Per-client limit

```go
if !rl.Allow(ctx, clientID) {
    return errors.New("client throttled")
}
```

Each distinct `clientID` gets its own bucket. With `MemoryStore`, bucket count is bounded by `WithMaxKeys` (default 100k); excess keys are rejected with `ErrKeyLimitExceeded`.

## Per-endpoint limit

```go
key := fmt.Sprintf("%s:%s", method, path)
if !rl.Allow(ctx, key) { /* ... */ }
```

## Combined per-client-per-endpoint

```go
key := fmt.Sprintf("%s:%s:%s", clientID, method, path)
if !rl.Allow(ctx, key) { /* ... */ }
```

## Tier-based limits (free vs paid)

Use separate limiters per tier:

```go
freeTier := ratelimit.New(ratelimit.Config{Rate: 10, Burst: 20, Interval: time.Second})
paidTier := ratelimit.New(ratelimit.Config{Rate: 1000, Burst: 2000, Interval: time.Second})

func limiterFor(user User) ratelimit.RateLimiter {
    if user.IsPaid {
        return paidTier
    }
    return freeTier
}
```

## Dynamic key extraction with `KeyFunc`

`KeyFunc` is a transformer/extractor: called with both the request context and the caller-supplied key. Use it when the key is best derived from context (auth middleware, headers).

### Always derive from context

```go
rl := ratelimit.New(ratelimit.Config{
    Rate: 10, Burst: 20, Interval: time.Second,
    KeyFunc: func(ctx context.Context, _ string) string {
        if uid, ok := ctx.Value(userIDKey).(string); ok {
            return uid
        }
        return "anonymous"
    },
})

// Caller-supplied key is ignored when KeyFunc returns a value.
rl.Allow(ctx, "")
```

### Fall back to caller-supplied key

```go
KeyFunc: func(ctx context.Context, key string) string {
    if uid, ok := ctx.Value(userIDKey).(string); ok {
        return uid
    }
    return key
}
```

### Namespace the caller's key

```go
KeyFunc: func(ctx context.Context, key string) string {
    tenant, _ := ctx.Value(tenantKey).(string)
    return tenant + ":" + key
}
```

## Custom storage backend (Redis, DynamoDB, etc.)

Implement the `Store` interface:

```go
type Store interface {
    AtomicUpdate(ctx context.Context, key string, fn func(*BucketState) *BucketState) (*BucketState, error)
    Get(ctx context.Context, key string) (*BucketState, error)
    Delete(ctx context.Context, key string) error
    Close() error
}
```

`AtomicUpdate` must be atomic: the read, the call to `fn`, and the write must not interleave with another `AtomicUpdate` for the same key. For Redis, use `WATCH/MULTI/EXEC` or a Lua script.

```go
rl := ratelimit.New(ratelimit.Config{
    Rate:     100,
    Burst:    200,
    Interval: time.Second,
    Store:    myRedisStore,
    FailOpen: true,  // allow on storage errors (capped at Burst tokens)
})
```

### Optional Store interfaces

Implement these for richer functionality:

- `HealthChecker` — `RateLimiter.HealthCheck` delegates here.
- `Resetter` — `RateLimiter.Reset` delegates here.
- `BucketCounter` — `RateLimiter.BucketCount` delegates here.

`MemoryStore` implements all three.

## Bulk operations: `Take` for N tokens

If a single operation costs N tokens (e.g., a write that consumes 5 budget):

```go
if !rl.Take(ctx, key, 5) {
    return errors.New("not enough tokens")
}
```

`MaxTokensPerRequest` (default `Burst × 10`) caps single-call grants to prevent DoS via huge-N requests. On fail-open, oversize requests are still denied.

## Blocking caller until budget free: `Wait`

```go
if err := rl.Wait(ctx, key); err != nil {
    // ctx cancelled or wait timeout
    return err
}
// proceed
```

`Wait` is bounded by `defaultMaxTotalWaitTime` (5 minutes). For shorter bounds, use `context.WithTimeout`.

## Structured error on denial

`Execute`/`ExecuteN` return `*rateLimitError`:

```go
if err := rl.Execute(ctx, key, op); err != nil {
    var rle *ratelimit.rateLimitError
    if errors.As(err, &rle) {
        log.Printf("limit hit: key=%s retry_after=%s", rle.Key(), rle.RetryAfter())
    }
}
```

`errors.Is(err, ratelimit.ErrLimitExceeded)` continues to match.
