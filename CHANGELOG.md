# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **13 MB of committed binaries no longer ship in the module.** `go build ./...` inside an example writes a binary named after its directory; those are extensionless, so none of `.gitignore`'s patterns (`*.exe`, `*.dll`, `*.so`, `*.dylib`, `*.test`) matched them, and five had been committed. Two — `examples/http/http` and `examples/composition/composition` — are inside the root module, so they went out in the module zip: measured against the published v1.8.1, **13 MB of a 16 MB download**, fetched by every consumer on every platform. `.gitignore` now excludes example build output by path rather than by extension.

  The published zips still contain them; module versions are immutable.

### ⚠ Breaking

- **`retry` no longer retries `ferrors.ErrCircuitOpen` on the default path.** An
  open circuit is a decision, not a transient failure: the breaker has already
  concluded the downstream must be left alone, so retrying its rejection
  multiplies load on the rejection path and multiplies the latency before the
  caller sees an error that was known at attempt 1 — during the incident the
  breaker exists to contain. With retry composed *outside* a breaker, each
  logical call against an open circuit previously cost `MaxAttempts` rejections
  spaced by backoff. Explicit classification still wins, so the escape hatch
  needs no new API: set `IsRetryable`, list the sentinel in `RetryableErrors`,
  or wrap with `ferrors.AsRetryable`. `ErrBulkheadFull` and
  `ErrRateLimitExceeded` stay retryable — both describe a queue that drains on
  its own (#73, closes #69).

### Added

- **circuitbreaker: `TripOnConsecutiveFailures(n)` and
  `TripOnFailureRatio(rate, minRequests)`.** `Counts.ConsecutiveFailures` is a
  `uint32` while a threshold from application config is naturally an `int`, so
  the most ordinary `ReadyToTrip` in existence needed a narrowing cast that
  gosec reports as G115 — and the cheap resolution, `//nolint:gosec`, builds the
  habit of suppressing G115 where it is catching something real. Both shipped
  predicates take plain `int`s and do the narrowing once, provably in range,
  inside the library (#75).
- **`fortify.Execute[T]` and `Chain.WithFallback` / `Composer.WithFallback`.**
  The spec's core API chains `.WithFallback(...)` on the composed policy and
  names a typed `fortify.Execute[T](ctx, policy, fn)` free function; the Go
  `Composer` exposed fallback only standalone and had no `Execute[T]`, so the
  spec-named spelling now exists and returns the typed result without an
  `any`-cast (#49).
- **`metrics/otel`: OpenTelemetry metrics adapter** (#49).
- **circuitbreaker: sliding-window failure-rate tripping.** Consecutive-failure
  counting only detects a downstream that is *down*; one that is merely degraded
  never accumulates a streak, because any success resets it, so `F S F F S F`
  leaves the circuit Closed however long the degradation lasts. `Interval` does
  not rescue this — it is a *tumbling* window, so a burst spanning a reset is
  split into two sub-threshold halves. `Config.SlidingWindowType`
  (`SlidingWindowCount` / `SlidingWindowTime`), `SlidingWindowSize`,
  `MinimumCalls` and `FailureRateThreshold` add the window both Resilience4j and
  Polly default to: count-based keeps the last N outcomes in a circular buffer,
  time-based the last N seconds in one-second buckets. `ReadyToTrip` still
  overrides it, and setting both is logged when a `Logger` is configured
  (#70).

### Fixed

- **security**: `pymdown-extensions` 11.0.0 → 11.0.1 (GHSA-gm37-52c6-37mw). The
  earlier bump (#63) landed on 11.0.0, which is the vulnerable version (#77).
- **security**: `grpc` and `x/text` bumped to patched versions (#61).
- **ci**: run CI on docs-only PRs, so required checks can report instead of
  leaving a docs-only PR blocked forever on a check that never starts (#62).

### Docs

- **middleware**: state the layering rule, and fix the timeout contradiction
  between the composition guide and the reference (#74).
- **ratelimit**: document `Rate` as per-`Interval`, so sub-1/sec quotas are
  findable (#72).

### CI / Chore

- **security**: nox remediation batches — dependency and action-pin updates
  (#60, #64, #65).
- **nox**: refresh the baseline to fingerprint v2 (nox 1.7.0) (#58).
- **ci**: adopt the reusable `nox-remediate` workflow and retire dependabot,
  which it supersedes (#56).
- Adopt warden — `.warden.yaml` plus the provenance check in CI.
- Drop cross-platform matrix + redundant benchmark from every-push CI (#53).

## [1.7.0] - 2026-06-20

Cost-budget pattern stays on the v1 line. (The v2.0.0/v2.1.0 tags were
withdrawn — fortify's module path is `go.klarlabs.de/fortify`, which Go
requires to remain on the v1 major; v2 tags were not `go get`-able.)

### Added

- **Cost budget** — `fortify.New[T]().WithCostBudget(CostBudgetConfig{MaxCost, CostFunc, ResetAfter})` plus `fortify.ErrBudgetExceeded`: the spec's public cost-budget surface, a thin wrapper over the `budget` package. `ResetAfter` adds a rolling time-window auto-reset and a public injectable `Clock`. The root `Composer` also delegates the full pattern set (circuit breaker, retry, timeout, bulkhead, rate limit, …) so it is not a dead end.

### Fixed

- Money conversion guards NaN/Inf/overflow; `WithCostBudget` panics on an invalid `MaxCost` instead of silently disabling the spend cap.
- `ResetAfter` reset is now atomic under concurrent `Execute` (windowed budget state serialized).

### CI / Chore

- Adopt the shared reusable Go CI workflow; enable nox taint-analysis SAST; coverage-badge + dependency housekeeping.

## [1.5.0] - 2026-05-12

First-class SSE / chunked-response support in `fortify/http`. Two complementary changes: the existing `CircuitBreaker` and `Timeout` middlewares no longer drop `http.Flusher`/`http.Hijacker`, and a new `CircuitBreakerStream` middleware pairs the breaker with `streamtimeout` so long-lived streams get per-chunk health signals instead of a single after-the-fact outcome. Backwards-compatible; v1.4.x callers compile unchanged.

### Fixed

- **http**: `responseRecorder` (used by `CircuitBreaker` and `Timeout`) now forwards `http.Flusher` and `http.Hijacker` to the underlying writer. SSE clients, chunked transfer responses, and `httputil.ReverseProxy`'s `FlushInterval` mechanism rely on per-chunk `Flush()`; the prior recorder silently buffered every response until `ServeHTTP` returned. `Hijack` returns `http.ErrNotSupported` when the underlying writer is not hijackable, matching net/http convention.

### Added

- **http**: new `CircuitBreakerStream(cb, streamtimeout.Config) (func(http.Handler) http.Handler, error)` middleware. Pairs a `circuitbreaker.CircuitBreaker[*http.Response]` with a `streamtimeout.StreamTimeout` so the FirstByte / Idle / Total watchdogs feed the breaker's failure signal. A streaming response writer:
  - forwards `Flusher` / `Hijacker` / `Pusher` to the underlying writer,
  - pings `streamtimeout.Mark` on every `Write` so per-chunk activity satisfies the idle watchdog without handler changes.

  A fired watchdog cancels the handler's context. Cooperative handlers abort the stream; the breaker records a failure. When the watchdog fires before any headers are written, the middleware synthesises a 504; once headers have flushed, the connection is closed silently (no misleading status overwrite). Client disconnects (`context.Canceled`) return without writing a synthetic error.

  Example — SSE pass-through with 5s TTFB / 2s idle / 5m total cap:

  ```go
  cb := circuitbreaker.New[*http.Response](circuitbreaker.Config{
      MaxRequests: 100, Interval: time.Minute,
  })
  mw, err := fortifyhttp.CircuitBreakerStream(cb, streamtimeout.Config{
      FirstByteTimeout: 5 * time.Second,
      IdleTimeout:      2 * time.Second,
      TotalTimeout:     5 * time.Minute,
  })
  if err != nil { log.Fatal(err) }
  mux.Handle("/v1/messages", mw(upstreamProxy))
  ```

### Tests

- `TestCircuitBreakerPreservesFlusher` — regression test for the Flusher buffering bug. Handler blocks after each flush until the client reads the corresponding event, so the test fails when `Flush` is dropped.
- `TestCircuitBreakerStreamHappyPath` / `IdleTimeout` / `FirstByteTimeout` / `Concurrent` / `ConfigValidation` / `HandlerError` — full coverage of the new streaming middleware.

### Acknowledgements

- Originated from the felixgeelhaar/tokenops integration (fronting OpenAI/Anthropic/Gemini SSE streams via a fortify-wrapped reverse proxy). Issue #32, PR #33.

## [1.4.0] - 2026-05-09

The AI/agent wedge lands. New primitives (`budget`, `streamtimeout`) and middleware presets (`LLMCall`, `LLMHedge`) wrap LLM and tool calls with cost ceilings, streaming-aware timeouts, and provider hedging. Three new examples — MCP server, Eino, and an observability demo. No breaking changes; v1.3.x callers compile unchanged.

### Patterns

- New **`budget/`** package — cost-budget primitive enforcing per-chain caps in three independent dimensions (tokens, micro-USD, calls). Caller-supplied `Charge` callback converts each operation result to a `Cost`; aggregation is atomic; first breach short-circuits remaining work via `*BudgetExceededError` (wraps `ErrBudgetExceeded`, exposes `Consumed`/`Max`, implements `slog.LogValuer`). Generic on result type.
- New **`streamtimeout/`** package — three-dimensional timeout for streaming work (LLM completions, server-sent events, gRPC streams). `FirstByteTimeout`, idle-between-chunks, and total-wall-clock are enforced independently via a `Mark` callback supplied to the user's function. `*StreamTimeoutError` reports which stage fired and unwraps to both `ferrors.ErrTimeout` and `context.DeadlineExceeded`. Implements `slog.LogValuer`.

### Middleware presets

- New **`middleware.LLMCall[T]`** — first AI-wedge preset. Chain: `Bulkhead → CircuitBreaker → Retry → Budget → Timeout → operation`. Budget sits inside Retry so each attempt is charged, capping the total cost of a retry storm during a provider incident. Defaults assume non-idempotent semantics; `IsRetryable` retries only on `ferrors.IsRetryable`-marked errors and `ferrors.ErrRateLimitExceeded`. Hard non-retryables: `budget.ErrBudgetExceeded`, `ferrors.ErrCircuitOpen`, `context.Canceled`, `context.DeadlineExceeded`. Set `AssumeIdempotent=true` to match HTTPClient retry semantics when an idempotency-key mechanism is wired.
- New **`middleware.LLMHedge[T]`** — races a primary attempt against one or more hedges; first non-error result wins, losers cancelled via context. Default `HedgeAfter` 800ms. Cross-vendor hedging (e.g. OpenAI vs Anthropic) requires explicit opt-in (`AllowCrossVendor=true`) or attestation (`AllAttemptsSameVendor=true`); the runner cannot detect provider identity, so the gate is documented attestation. Each `Attempt` is an already-wrapped invocation, typically an `LLMCall` chain bound to a specific provider/model.
- **`middleware.Chain[T]`** gained `WithBudget`, mirroring the other `With*` methods.

### Examples

- New **`examples/mcp-server/`** — Model Context Protocol tool-handler reference wiring Fortify into a tool handler. Chain: `Adaptive → Bulkhead → per-peer RateLimit → Timeout → operation`. Does not pin a specific MCP SDK (a minimal local `ToolHandler` interface stands in for the SDK shape) so CI stays green; the integration shape does not change when readers swap in the official `modelcontextprotocol` Go SDK or `mark3labs/mcp-go`. Demonstrates structured-error logging via `slog.LogValuer` without inspecting request payloads.
- New **`examples/eino/`** — Eino integration reference using `LLMCall` with a token+USD budget driven by the response `Usage` field. Demonstrates Fortify's stance vs Go agent frameworks: complementary, not competitive — Eino owns prompts, tools, planning, and memory; Fortify wraps resilience around each model call. Avoids a hard dependency on `github.com/cloudwego/eino`.
- New **`examples/observability-demo/`** — self-contained docker-compose stack (Prometheus + Grafana + sample app driving synthetic load through CB → Retry → Timeout). Grafana is provisioned with a pre-built Fortify overview dashboard covering CB state, request rate, retry attempts, rate-limit allow/deny, bulkhead active/queued, and timeout durations.

### Documentation

- `docs/POSITIONING.md` — Fortify's anchored wedge (Go services calling LLMs/tools), April Dunford 5-component breakdown, and validation gate.
- `docs/PRODUCTION.md` "Observability and sensitive payloads" — PII policy: metric/span labels are pattern names and outcomes only, never request bodies or LLM prompts.
- README, godoc, and pattern-count claims aligned with the Go version declared in `go.mod`; `slog.LogValuer` integration documented.
- Draft blog post staged on hedging LLM tool calls in Go.

### Chore

- Roady-derived backlog committed; runtime state ignored.
- `gofmt` field-padding alignment in `budget/budget.go` (no behaviour change).
- Coverage badge refreshes.

## [1.3.1] - 2026-05-04

Patch release: CI hardening, docs site online, flaky-test fix. No API changes.

### Fixed

- **hedge**: `TestHedge_FiresHedgeWhenPrimarySlow` was flaky on Linux. Primary
  returned `(1, ctx.Err())` which could resolve to `(1, nil)` and race the hedge.
  Primary now returns an explicit error so the hedge is the only possible winner.
- **lint**: errcheck on `Close()` deferreds wrapped in `func() { _ = ...Close() }()`
  across `circuitbreaker/property_test.go`, `grpc/client_test.go`,
  `middleware/http_test.go`.
- **docs site**: now builds and deploys to GitHub Pages. `docs/requirements.txt`
  pins MkDocs deps; root governance docs staged into `docs/` at build time so
  cross-links resolve; dropped `--strict` (mkdocs flags `[bracket]` fragments
  inside code blocks as broken links — false positives).
- **changelog workflow**: switched from push-on-main to weekly cron +
  `workflow_dispatch`. The push trigger created a runaway loop (each merged
  PR opened a fresh refresh PR).
- **nox CLI install**: bumped to source path `github.com/nox-hq/nox/cli@v0.8.1`
  (binary lives at the `/cli` subpackage), renamed `cli → nox` after install.
  CI tolerates source-built CLI exit-1-on-baselined-findings via `|| true` and
  asserts SARIF/SBOM artifact existence as the real gate.

### Security

- New `.nox.yaml` excludes `.nox/baseline.json` (entropy self-flag), `go.sum`
  (module digests), `.github/workflows/*.yml` (pinned SHAs), README/CHANGELOG/
  docs (token snippets), `assets/grafana/*.json` (Prometheus metric names).
  Local rescan: 0 findings, 76 suppressed, exit 0.
- `.nox/baseline.json` committed for cross-run finding suppression.
- `.golangci.yml` suppresses `gosec G115` package-wide (atomic.Int32 mirrors
  in `adaptive/` are deliberate, sources are config-clamped) and `dupl` on
  `middleware/presets.go` (intentional structural similarity per preset).

### Dependencies

- Action SHA bumps merged: `actions/upload-artifact` 4.6.2 → 7.0.1,
  `actions/upload-pages-artifact` 3.0.1 → 5.0.0, `actions/deploy-pages` 4.0.5
  → 5.0.0, `actions/configure-pages` 5.0.0 → 6.0.0, `softprops/action-gh-release`
  2.6.2 → 3.0.0 (resolves Node 20 deprecation), `github/codeql-action` SHA bump.
- `golang.org/x/text` 0.33.0 → 0.36.0.
- `pymdown-extensions` 10.12 → 10.16.1 (in `docs/requirements.txt`).

## [1.3.0] - 2026-05-03

### ⚠ BREAKING CHANGES

This release contains source-incompatible changes across multiple packages.
Migration is mechanical; full diff at `docs/MIGRATION.md`.

- **`ratelimit.New`** now takes `Config` by value, not `*Config`. Drop the `&`:
  - Before: `ratelimit.New(&ratelimit.Config{...})`
  - After: `ratelimit.New(ratelimit.Config{...})`
- **`retry.Retry[T].Do(...)` renamed to `Execute(...)`** for verb consistency with `cb.Execute`, `timeout.Execute`, `bulkhead.Execute`, `fallback.Execute`.
- **`circuitbreaker.CircuitBreaker[T]` interface** gained a `Close() error` method. Existing implementations of the interface (e.g. test doubles) need to add a no-op Close.
- **`ratelimit.Config.KeyFunc`** signature: `func(ctx) string` → `func(ctx, key string) string`. The caller-supplied key is now passed in; KeyFunc may use, transform, or override it. Removes the silent argument-shadow footgun.
- **Structured errors** replace bare sentinels:
  - `cb.Execute` returns `*ferrors.CircuitOpenError` (with `State`, `RetryAfter`, `Counts`).
  - `ratelimit.Execute`/`ExecuteN` return `*ratelimit.rateLimitError` (with `Key()`, `RetryAfter()`).
  - `timeout.Execute` returns `*ferrors.TimeoutError` (with `Timeout`).
  - All wrap their existing sentinels via `Unwrap`, so `errors.Is(err, ferrors.ErrCircuitOpen)`, `errors.Is(err, ratelimit.ErrLimitExceeded)`, `errors.Is(err, ferrors.ErrTimeout)`, and `errors.Is(err, context.DeadlineExceeded)` all continue to match.

### Fixed

#### Critical concurrency bugs

- **bulkhead:** worker no longer leaks goroutines on shutdown. Queued requests receive `ErrBulkheadFull` via `drainQueue`. `Close()` no longer closes the queue channel (which racily panicked in-flight senders); senders observe `b.done` in a new inner-select case instead. (`bulkhead/bulkhead.go`)
- **bulkhead:** `enqueue` inner select gained `<-b.done` case so callers waiting on `resultCh` exit cleanly when the bulkhead closes mid-flight.
- **ratelimit:** fail-open path now caps grants at `Config.Burst` for `Take`/`ExecuteN`. Previously, on `Store` error, oversize token requests bypassed `MaxTokensPerRequest` enforcement, enabling DoS amplification during storage outages. (`ratelimit/limiter.go`)
- **http.CircuitBreaker middleware:** no longer writes a second `WriteHeader(503)` when the downstream handler already wrote a response. Status read protected by the recorder's mutex to defend against handler-spawned goroutines. (`http/middleware.go`)
- **circuitbreaker:** `OnStateChange` callbacks delivered in transition order via a single dispatcher goroutine + bounded channel (default 64). Replaces per-event `go safeCallback` which reordered notifications under rapid flapping. New `Close()` method drains the dispatcher; idempotent. (`circuitbreaker/breaker.go`)

#### High-priority correctness

- **gRPC `KeyFromMetadata` / `StreamKeyFromMetadata`** now sanitize and truncate (256-byte cap) client-supplied metadata. Previously raw values flowed to the rate-limit Store, triggering `ErrKeyTooLong` per request and exploding metric label cardinality. (`grpc/interceptor.go`)
- **retry:** switched jitter source from `math/rand` (global mutex) to `math/rand/v2` (lock-free, per-goroutine). Removed contention spike under concurrent retries. (`retry/backoff.go`)
- **retry:** `calculateBackoff` now caps at 24h before float→`time.Duration` conversion, defending against `math.Pow` producing `+Inf` (which silently became negative `time.Duration` and broke `time.NewTimer`). `Config.Multiplier` capped at 100 in `setDefaults`. (`retry/backoff.go`, `retry/config.go`)
- **retry:** replaced `time.After` in the retry loop with a reusable `time.NewTimer` + drain-on-cancel. Eliminates per-attempt timer leak under high QPS with fast cancellation. (`retry/retry.go`)

### Performance

- **circuitbreaker:** lock-free fast path for steady-state Closed admission. `Execute` now skips the mutex when `state == Closed && time.Now() < expiry`, using atomic mirrors (`fastState`, `fastExpiry`, `fastGen`). Mirrors are refreshed under `mu` in `setState`, `currentState` (Closed reset path), and `Reset`. Restores the "<1µs hot path" claim under contention.
  - Apple M5, 10 cores: 70 ns/op steady-state; 187 ns/op concurrent (10 goroutines); 0 allocs.

### Supply chain

- Replaced `verdictsec` security scan with `nox` (v0.8.1, pinned); SARIF uploaded to GitHub code scanning. Dropped `|| true` failure-swallowing in CI. CI uses new v0.8 flag-before-subcommand syntax (`nox -format X -output Y scan .`); SARIF artifact renamed to `results.sarif` (was `scan.sarif` in v0.7).
- All GitHub Actions pinned to commit SHAs with `# vX.Y.Z` annotations (closes 22 IAC-013 findings).
- Added `actions/dependency-review-action` PR gate (fails on high-severity vulnerable additions).
- Added `.github/dependabot.yml` with grouped weekly updates for gomod (otel/grpc/prometheus groups) and GitHub Actions.
- `release.yml` Go version bumped 1.24 → 1.25; release artifacts now include CycloneDX (`sbom.cdx.json`) and SPDX (`sbom.spdx.json`) SBOMs.
- `performance.yml` Go version bumped 1.24 → 1.25.

### Patterns

- New **`hedge/`** package — hedged-request execution for tail-latency reduction. Fires the primary attempt immediately; if not returned within `HedgeDelay`, fires a second (and optionally third, ...) attempt in parallel up to `MaxAttempts`. First success wins; remaining attempts are cancelled via shared context. `MaxAttempts` capped at 16. Generic on `T`. Use only with idempotent operations.
- New **`adaptive/`** package — AIMD-tuning concurrency limiter. Starts at `InitialLimit`; every `SuccessThreshold` consecutive successes raises the cap by 1 (additive); every failure halves the cap (multiplicative), bounded by `MinLimit`/`MaxLimit`. CAS-based, lock-free hot path. Generic on `T`. Use when downstream capacity is unknown or shifts over time.

### Middleware presets

- New **`middleware.HTTPClient`** — preset chain for outbound HTTP: CB → retry → timeout. Configurable failure threshold, retry count, timeouts.
- New **`middleware.DatabaseQuery`** — preset for DB queries: conservative retry (default 2 attempts), late breaker trip (default 10 consecutive failures).
- New **`middleware.RPCDownstream[T]`** — preset for per-downstream RPC chains, generic on result type.
- New **`middleware.HTTPRoundTripper`** — wraps any `http.RoundTripper` with the HTTPClient preset chain. Returns an `http.RoundTripper` ready to mount on `http.Client.Transport`.
- New **`middleware.HTTPRoundTripperFromChain`** — same shape but accepts an arbitrary user-built `*Chain[*http.Response]`.
- `middleware.Chain[T]` gained `WithAdaptive` and `WithHedge` builders.

### gRPC client-side interceptors

- New **`UnaryClientCircuitBreakerInterceptor`** — wraps unary client calls; returns `Unavailable` when the breaker is open.
- New **`UnaryClientRateLimitInterceptor`** + key extractors `KeyFromMethod`, `KeyFromOutgoingMetadata` — returns `ResourceExhausted` when over budget.
- New **`UnaryClientTimeoutInterceptor`** — returns `DeadlineExceeded` on Fortify timeout.
- New **`StreamClientCircuitBreakerInterceptor`** + **`StreamClientRateLimitInterceptor`** — gate stream creation only.
- All client-side metadata extractors share the sanitization + 256-byte truncation already applied to server-side ones.

### Tests

- New **property-based tests** for the circuit breaker state machine using `pgregory.net/rapid`. Asserts: state always recognized, counts coherent in long-running Closed, generation monotonic across Execute/Reset, Open rejects within Timeout window. Test-only dependency; not pulled by consumers.

### Examples

- New **`examples/ratelimit-redis/`** — reference Redis-backed `ratelimit.Store`. Atomic update via Lua (server-side TIME, scaled-integer tokens, automatic TTL). Lives in its own Go module to avoid pulling `go-redis` into core fortify.
- New **`examples/circuitbreaker-redis/`** — reference distributed circuit breaker. State (Closed/Open/HalfOpen, generation, expiry, counters) lives in a single Redis Hash; admission and recording happen atomically via Lua. Implements `circuitbreaker.CircuitBreaker[T]`. Trade-offs vs. in-process CB documented in the package README.

### Operations

- New **`assets/grafana/fortify-dashboard.json`** — importable Grafana dashboard with panels for CB state, retry attempts, rate-limit allow/deny, timeout duration, bulkhead utilization. Datasource variable; Grafana 10.x compatible.
- New **`.github/workflows/changelog.yml`** + `cliff.toml` — git-cliff auto-refresh of the Unreleased section from conventional commits, opens a PR on each push to main.
- New **`.github/workflows/docs.yml`** + `mkdocs.yml` — MkDocs Material site published to GitHub Pages from `docs/`. Builds with `--strict` to fail on broken links.

### Algorithm options

- **`adaptive.AlgorithmVegas`** — RTT-aware concurrency tuning. Tracks the minimum observed call latency (no-load baseline) and an EMA of recent latencies (halflife ≈ 8 samples). Computes a queue-depth estimate (`limit × (emaRTT − minRTT) / emaRTT`); raises the limit when the estimate is below `VegasAlpha` (default 3), lowers when above `VegasBeta` (default 6). Reacts to rising latency before failures appear. AIMD is still the default. Configurable via `Algorithm`, `VegasAlpha`, `VegasBeta`, `VegasMinSamples`.
- **`adaptive.AlgorithmGradient2`** — smoothed gradient-of-RTT controller (Netflix concurrency-limits naming). `gradient = clamp(minRTT/longEMA, 0.5, 1.0)`; `newLimit = floor(currentLimit × gradient + √currentLimit)`. Reacts proportionally to RTT inflation instead of waiting on water marks. Configurable via `GradientMinSamples`, `GradientSmoothing`. Highest per-call overhead of the three algorithms (one sqrt + one float multiply + two atomic time samples).

### Fuzz tests

- New `FuzzSanitizeLogKey` (`ratelimit/`), `FuzzSanitizeKey` (`http/`), `FuzzSanitizeMetadataKey` (`grpc/`). Properties: idempotency, length bounds, no control characters, UTF-8 validity preservation, clean inputs unchanged. Each runs 70k–250k execs/3s in CI sample runs.

### Property tests

- `adaptive/property_test.go` — limit always within `[MinLimit, MaxLimit]`, in-flight settles to zero after sequential Execute, persistent failure floors at `MinLimit`.
- `ratelimit/property_test.go` — bucket tokens within `[0, Burst]`, `BucketCount` ≤ `MaxKeys`, `Allow(k)` and `Take(k, 1)` agree, `Reset` empties the store.

### HTTP server preset

- New **`middleware.HTTPHandler`** — wraps any `http.Handler` with `RateLimit (optional) → CircuitBreaker → Timeout → inner`. Returns 429/503/504 to clients on the corresponding pattern errors. Server-side equivalent of `HTTPRoundTripper`.

### Benchmarks

- `hedge_bench_test.go` and `adaptive_bench_test.go` cover the new patterns. Apple M5 single-thread numbers:
  - `BenchmarkAIMDSuccess` — 8 ns/op, 0 allocs
  - `BenchmarkAIMDFailure` — 18 ns/op, 1 alloc (the structured error)
  - `BenchmarkVegasSuccess` — 58 ns/op, 0 allocs (extra cost = `time.Now` + EMA update)
  - `BenchmarkAIMDSuccessParallel` — 203 ns/op (10 cores), 0 allocs
  - `BenchmarkHedgePrimaryWins` — 740 ns/op, 9 allocs (inherent to the goroutine + channel + timer machinery; hedging trades work for tail-latency reduction)

### Slimmed

- **`slog/` package** trimmed: dropped redundant constructor wrappers (`NewLogger`, `NewTextLogger`, `NewJSONLogger`) and pattern-event helpers (`LogPatternEvent`, `LogPatternError`, `LogPatternMetrics`). Kept the `Pattern` enum, `WithPattern`, and `LogContext`. New patterns added to enum: `PatternFallback`, `PatternHedge`, `PatternAdaptive`.

### Docs

- New `docs/COMPARISON.md` — Fortify vs `sony/gobreaker`, `failsafe-go`, `uber-go/ratelimit`, `golang.org/x/time/rate`, `hashicorp/go-retryablehttp`. Per-pattern feature matrices and "when Fortify is the wrong choice" guidance.
- New `GOVERNANCE.md` — solo-maintainer disclosure, semver policy, release process, security disclosure.
- New `ADOPTERS.md` — bootstrap with PR-add instructions.
- README rewritten in Diataxis style: lean landing (~180 lines) + dedicated `docs/concepts.md`, `docs/how-to-compose.md`, `docs/how-to-observe.md`, `docs/how-to-rate-limit.md`, `docs/how-to-test.md`, `docs/integrations.md`.

### Added

#### Pluggable Storage Interface
- **Store interface** for custom storage backends (`ratelimit/store.go`)
  - `AtomicUpdate` method for atomic read-modify-write operations
  - `Get` method for read-only state access without side effects
  - `Delete` and `Close` methods for resource management
  - `BucketState` struct for serializable token bucket state
- **MemoryStore** default in-memory implementation (`ratelimit/memory.go`)
  - Uses `sync.Map` with per-key mutex for thread-safe atomicity
  - Zero-configuration default for single-instance deployments
  - TTL-based automatic cleanup of stale entries (default: 1 hour)
  - Configurable maximum key limit (default: 100,000) for memory protection
  - Configurable maximum key length (default: 1,024) to prevent memory exhaustion
  - Functional options: `WithMaxKeys()`, `WithCleanupInterval()`, `WithEntryTTL()`, `WithMaxKeyLength()`

#### Configuration Options
- **Store** field in Config for custom storage backends
- **FailOpen** field for configurable failure behavior
  - `FailOpen: false` (default) - deny requests when storage fails (consistency)
  - `FailOpen: true` - allow requests when storage fails (availability)
- **MaxTokensPerRequest** field (default: Burst × 10) to prevent DoS via excessive token requests
- **Metrics** field for observability integration

#### Interfaces
- **HealthChecker** interface for distributed store health monitoring
  - `HealthCheck(ctx context.Context) error` method
  - MemoryStore implements HealthChecker by default
- **Metrics** interface for observability hooks (all methods now receive context)
  - `OnAllow(ctx, key)` - called when request is allowed
  - `OnDeny(ctx, key)` - called when request is denied
  - `OnError(ctx, key, err)` - called on storage errors
  - `OnStoreLatency(ctx, operation, duration)` - storage latency tracking
- **RateLimiter.HealthCheck()** method for health monitoring
- **RateLimiter.Close()** method for proper resource cleanup
- **RateLimiter closed flag** - Prevents operations after Close() is called

#### Error Handling
- **Sentinel errors** for better error handling (`ratelimit/errors.go`)
  - `ErrLimitExceeded` - rate limit exceeded
  - `ErrStorageUnavailable` - storage backend unavailable
  - `ErrInvalidTokenCount` - invalid token count in Take()
  - `ErrExcessiveTokens` - token request exceeds MaxTokensPerRequest
  - `ErrStoreClosed` - operation attempted on closed store
  - `ErrKeyLimitExceeded` - maximum key limit reached
  - `ErrKeyTooLong` - key exceeds maximum length
  - `ErrWaitTimeout` - Wait() exceeded maximum iterations or time limit
  - `ErrRateLimiterClosed` - operation attempted on closed rate limiter

#### Documentation
- **Package documentation** moved to `ratelimit/doc.go` per Go conventions
- Comprehensive usage examples in doc.go
- **Store interface stability guarantee** - Documented interface stability and extension patterns
- **KeyFunc key length constraints** - Documented that keys must respect Store's maximum length

### Changed
- Rate limiter now uses pluggable `Store` interface instead of internal `sync.Map`
- Token bucket algorithm logic moved from separate file to `limiter.go`
- In-memory storage now uses `Store` interface (unified architecture)
- `calculateWaitTime()` now uses `Get()` for read-only access (no side effects)
- `refill()` optimized to avoid unnecessary allocations when state unchanged
- **`OnLimit` callback** now receives `context.Context` as first argument for observability
- **Magic numbers extracted** to named constants for maintainability
- **Config validation** now caps Rate, Burst, Interval, and MaxTokensPerRequest to maximum values
- **Metrics interface** now receives context in all methods for distributed tracing
- **KeyFunc** documentation clarified: when set, it takes precedence and the key parameter is ignored
- **Wait()** now has iteration and time limits (10,000 iterations / 5 minutes) to prevent infinite loops

### Fixed
- **Timer leak in Wait()** - Now uses `time.NewTimer` with proper `Stop()` and channel draining
- **Timer reuse optimization** - Wait() now reuses timer with Reset() to reduce allocations
- **Delete() race condition** - Entry lock acquired before deletion to prevent races with AtomicUpdate
- **Delete() key length check** - Now validates key length like other operations
- **Input validation in Take()** - Rejects zero, negative, and excessive token requests
- **calculateWaitTime() side effect** - No longer modifies state via AtomicUpdate
- **TOCTOU race in key creation** - Fixed using pre-increment approach with proper rollback
- **Clear() race condition** - Fixed with two-phase delete (collect then delete)
- **Panic recovery** - Now includes stack trace for debugging
- **Closed rate limiter operations** - Allow/Wait/Take/HealthCheck now check closed flag before proceeding

### Removed

#### Breaking Changes
- **Removed `backends/redis/` module** - Users should implement their own Redis adapter
- **Removed `examples/backends/redis/`** - Docker Compose example removed
- **Removed `ratelimit/tokenbucket.go`** - Logic consolidated into limiter

### Migration
- **In-memory users**: No changes required - works exactly as before
- **Redis users**: Implement custom `Store` adapter (see `docs/MIGRATION_REDIS.md` for examples)

### Documentation
- Updated `docs/MIGRATION_REDIS.md` to be a comprehensive custom storage backend guide
  - Redis implementation example with WATCH/MULTI/EXEC
  - DynamoDB implementation example with conditional writes
  - Testing strategies for custom stores
  - Production deployment best practices

## [1.1.0] - 2025-10-19

### Added

#### Distributed Rate Limiting
- **Redis backend for distributed rate limiting** (`backends/redis`)
  - Separate Go module maintains zero-dependency promise for core
  - Atomic operations via Lua scripts (production-grade, no race conditions)
  - Same `RateLimiter` interface as in-memory (drop-in replacement)
  - Support for Redis Cluster, Redis Sentinel, and standalone Redis
  - Configurable fail-open or fail-closed behavior
  - Automatic bucket expiration with configurable TTL
  - Full observability integration (slog, OpenTelemetry)
  - Comprehensive test suite with miniredis (>90% coverage)
  - Benchmark tests comparing Redis vs in-memory performance

#### Documentation
- **Redis backend README** with installation, configuration, and usage examples
- **Migration guide** (`docs/MIGRATION_REDIS.md`) for moving from in-memory to Redis
- **Complete working example** with Docker Compose demonstrating distributed rate limiting
- Added distributed rate limiting section to main README

#### Examples
- Multi-instance distributed rate limiting example (`examples/backends/redis`)
  - Docker Compose setup with 3 application instances + Redis
  - Makefile for easy testing and demonstration
  - HTTP API with rate limiting across instances
  - Health checks and observability

## [1.0.0] - 2025-10-03

### Added

#### Production Readiness
- **Root package documentation** (doc.go) for improved discoverability
- Comprehensive package-level documentation across all patterns
- Production deployment guide with best practices
- Error handling patterns guide
- Migration guide from other libraries

#### API Improvements
- **Consistent constructor signatures** - All patterns now use Config by value
- **Clean error package naming** - Renamed to `ferrors` to avoid stdlib conflicts
- Zero import aliasing required throughout codebase

#### Critical Bug Fixes
- **Fixed timeout callback goroutine leak** - Callbacks now execute synchronously
- **Fixed bulkhead worker lifecycle** - Clear shutdown semantics with Close()
- **Fixed token bucket edge cases** - Overflow protection and division-by-zero handling
- **Documented concurrency semantics** - Clear thread-safety guarantees

#### Quality Assurance
- Zero goroutine leaks verified with race detector
- Zero race conditions across all packages
- 80%+ test coverage on all packages
- All critical production issues resolved

### Changed
- Constructor signature: `retry.New()` now takes `Config` by value (was pointer)
- Package rename: `errors` → `ferrors` for cleaner imports
- Timeout callbacks execute synchronously to prevent leaks
- Bulkhead `Close()` signals shutdown but doesn't wait (user coordinates)

### Fixed
- Timeout goroutine leak from unbounded callback spawning
- Bulkhead WaitGroup race condition between Execute() and Close()
- Token bucket waitTime() race condition (documented as best-effort)
- Token bucket refill overflow from clock skew or system sleep
- Division-by-zero edge cases in token bucket calculations

### Documentation
- Added root doc.go for pkg.go.dev integration
- V1_RELEASE_SUMMARY.md - Complete v1.0 overview
- V1_FINAL_STATUS.md - Production readiness certification
- Enhanced inline documentation for all edge cases
- Clear resource cleanup semantics

## [0.1.1] - 2025-10-01

### Fixed
- Minor bug fixes and documentation improvements

## [0.1.0] - 2025-10-01

### Added

#### Core Patterns
- **Circuit Breaker** pattern with three states (Closed, Open, Half-Open)
  - Configurable failure thresholds and recovery timeouts
  - State change callbacks for monitoring
  - Request counting with sliding window
  - Type-safe with Go generics

- **Retry** pattern with intelligent backoff strategies
  - Exponential, linear, and constant backoff policies
  - Configurable jitter to prevent thundering herd
  - Custom retry predicates for error handling
  - Maximum attempt and delay limits

- **Rate Limiter** using token bucket algorithm
  - Per-key rate limiting
  - Configurable rate, burst, and interval
  - Both blocking (Wait) and non-blocking (Allow) operations
  - Context-aware cancellation

- **Timeout** pattern with context propagation
  - Configurable default and per-operation timeouts
  - Automatic context deadline enforcement
  - Timeout callbacks for monitoring

- **Bulkhead** pattern for concurrency limiting
  - Semaphore-based concurrency control
  - Queue management with configurable capacity
  - Dual semaphore design preventing race conditions
  - Request statistics tracking

#### Integration
- **Middleware Chain** for composable pattern orchestration
  - Fluent API for building resilience chains
  - Right-to-left composition order
  - Support for all core patterns
  - Type-safe execution

- **HTTP Middleware** for standard http.Handler
  - CircuitBreaker, RateLimit, and Timeout middleware
  - Thread-safe response recording
  - Appropriate HTTP status codes (503, 429, 504)
  - Flexible key extraction (IP, headers)

- **gRPC Interceptors** for unary and streaming RPCs
  - Unary and stream interceptors for all patterns
  - Metadata-based key extraction
  - Standard gRPC status codes
  - Context propagation support

#### Observability
- **Structured Logging** with log/slog integration
  - Pattern-specific log helpers
  - Event and metric logging functions
  - Configurable log levels

- **OpenTelemetry Tracing** support
  - Distributed tracing integration
  - Pattern-specific span creation
  - Attribute and event recording
  - Error tracking

#### Documentation
- Comprehensive README with usage examples
- API documentation with inline comments
- Example programs for all patterns
- Performance benchmarks and metrics

### Performance
- <1µs overhead for all patterns
- Zero allocations in hot paths
- Concurrent execution with race-free guarantees
- Optimized for production workloads

### Testing
- >95% test coverage across all packages
- Race detection on all tests
- Comprehensive benchmarks
- Fuzz testing for backoff algorithms

[Unreleased]: https://github.com/klarlabs-studio/fortify/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/klarlabs-studio/fortify/releases/tag/v1.0.0
[0.1.1]: https://github.com/klarlabs-studio/fortify/releases/tag/v0.1.1
[0.1.0]: https://github.com/klarlabs-studio/fortify/releases/tag/v0.1.0