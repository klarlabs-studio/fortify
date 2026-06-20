package fortify

import (
	"context"
	"fmt"
	"math"
	"time"

	"go.klarlabs.de/fortify/adaptive"
	"go.klarlabs.de/fortify/budget"
	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/hedge"
	"go.klarlabs.de/fortify/middleware"
	"go.klarlabs.de/fortify/ratelimit"
	"go.klarlabs.de/fortify/retry"
	"go.klarlabs.de/fortify/timeout"
)

// ErrBudgetExceeded is returned (wrapped) by a chain carrying a cost
// budget once the configured ceiling is reached. Match it with
// errors.Is. It re-exports budget.ErrBudgetExceeded so the convenience
// API and the lower-level budget package share a single sentinel.
var ErrBudgetExceeded = budget.ErrBudgetExceeded

// usdMicrosPerUSD converts whole US dollars to integer micro-USD, the
// unit the underlying budget package accumulates in to avoid float drift.
const usdMicrosPerUSD = 1_000_000

// CostBudgetConfig configures a single-dimension monetary cost budget for
// the fluent fortify composer. It is the spec's convenience surface over
// the richer multidimensional budget package: costs are expressed as
// floating-point US dollars and mapped onto the budget's integer
// micro-USD dimension.
//
// Use the lower-level budget package directly when you need to cap
// tokens or call counts, fire OnExceeded callbacks, or combine multiple
// cost dimensions.
type CostBudgetConfig struct {
	// MaxCost is the spending ceiling in US dollars (e.g. 5.0 = $5.00).
	// Must be a positive, finite value that does not overflow the budget's
	// micro-USD dimension. An invalid MaxCost (non-positive, NaN, Inf, or
	// overflowing) panics WithCostBudget: a spend cap silently disabled by a
	// typo is the wrong default for a safety control.
	MaxCost float64

	// CostFunc reports the cost, in US dollars, that an operation
	// consumed. It is invoked once per call regardless of err, so a
	// failed attempt that still cost money can be charged. The result is
	// passed as any; type-assert it to your operation's result type.
	// Nil charges nothing (the budget then never advances on cost).
	//
	// A non-finite or overflowing return (NaN, Inf, or a value whose
	// micro-USD conversion would overflow int64) is treated as zero so a
	// bad cost reading cannot corrupt accounting or instantly breach.
	CostFunc func(result any, err error) float64

	// ResetAfter, when positive, turns the budget into a rolling window:
	// the accumulated spend is automatically cleared once this much
	// wall-clock time has elapsed. Zero (the default) makes the ceiling
	// apply for the lifetime of the chain. The window is wall-clock and
	// independent of any ctx deadline or cancellation.
	ResetAfter time.Duration

	// Clock supplies the current time for ResetAfter windowing. Nil defaults
	// to time.Now. Override it to drive the rolling window from a
	// deterministic or virtualized clock (tests, simulations). Mirrors
	// budget.Config.Clock.
	Clock func() time.Time
}

// Composer is the curated, fluent entry point for assembling a resilience
// pipeline from the top-level fortify package. It wraps middleware.Chain and
// re-exposes its pattern builders so the curated surface is not a dead end,
// while adding the spec-level convenience method WithCostBudget.
//
// Composer covers the common patterns. For the full toolkit (WithAdaptive,
// WithHedge, custom ordering, or sharing one pattern instance across chains)
// drop down to middleware.Chain directly; Composer is a thin delegating
// wrapper over it.
//
// Build a Composer with New, attach policies, then call Execute.
type Composer[T any] struct {
	chain *middleware.Chain[T]
}

// New creates an empty Composer for operations returning T.
//
//	out, err := fortify.New[Response]().
//	    WithCostBudget(fortify.CostBudgetConfig{
//	        MaxCost: 5.0,
//	        CostFunc: func(result any, _ error) float64 {
//	            return result.(Response).USDCost
//	        },
//	    }).
//	    Execute(ctx, callLLM)
func New[T any]() *Composer[T] {
	return &Composer[T]{chain: middleware.New[T]()}
}

// WithCostBudget attaches a monetary cost budget to the composer. Once the
// accumulated cost exceeds MaxCost, Execute returns an error matching
// ErrBudgetExceeded and the operation is refused.
//
// It is a thin wrapper over the budget package: a single USD/float ceiling
// mapped to the budget's micro-USD dimension, with optional time-based
// auto-reset via ResetAfter. For tokens/calls dimensions, OnExceeded
// callbacks, or sharing one budget across chains, use the budget package
// with middleware.Chain.WithBudget directly.
//
// It PANICS if cfg.MaxCost is invalid (non-positive, NaN, Inf, or large
// enough to overflow the micro-USD dimension). This is a programmer error in
// a fluent builder: silently degrading a spend cap to a no-op is the wrong
// default for a safety control.
func (c *Composer[T]) WithCostBudget(cfg CostBudgetConfig) *Composer[T] {
	micros, ok := usdToMicros(cfg.MaxCost)
	if !ok {
		panic(fmt.Sprintf(
			"fortify.WithCostBudget: invalid MaxCost %v: must be a positive, finite USD value that does not overflow micro-USD",
			cfg.MaxCost,
		))
	}
	b, err := budget.New[T](budget.Config[T]{
		Max:        budget.Cost{USDMicros: micros},
		Charge:     costFuncToCharge[T](cfg.CostFunc),
		ResetAfter: cfg.ResetAfter,
		Clock:      cfg.Clock,
	})
	if err != nil {
		// usdToMicros already guaranteed a positive USDMicros, so budget.New
		// cannot reject for an empty Max here; surface anything unexpected
		// rather than silently disabling the cap.
		panic(fmt.Sprintf("fortify.WithCostBudget: %v", err))
	}
	c.chain.WithBudget(b)
	return c
}

// WithCircuitBreaker delegates to middleware.Chain.WithCircuitBreaker.
func (c *Composer[T]) WithCircuitBreaker(cb circuitbreaker.CircuitBreaker[T]) *Composer[T] {
	c.chain.WithCircuitBreaker(cb)
	return c
}

// WithRetry delegates to middleware.Chain.WithRetry.
func (c *Composer[T]) WithRetry(r retry.Retry[T]) *Composer[T] {
	c.chain.WithRetry(r)
	return c
}

// WithRateLimit delegates to middleware.Chain.WithRateLimit. The key
// identifies the rate-limit bucket (e.g. user ID, IP address).
func (c *Composer[T]) WithRateLimit(rl ratelimit.RateLimiter, key string) *Composer[T] {
	c.chain.WithRateLimit(rl, key)
	return c
}

// WithTimeout delegates to middleware.Chain.WithTimeout.
func (c *Composer[T]) WithTimeout(tm timeout.Timeout[T], duration time.Duration) *Composer[T] {
	c.chain.WithTimeout(tm, duration)
	return c
}

// WithBulkhead delegates to middleware.Chain.WithBulkhead.
func (c *Composer[T]) WithBulkhead(bh bulkhead.Bulkhead[T]) *Composer[T] {
	c.chain.WithBulkhead(bh)
	return c
}

// WithAdaptive delegates to middleware.Chain.WithAdaptive.
func (c *Composer[T]) WithAdaptive(a adaptive.Limiter[T]) *Composer[T] {
	c.chain.WithAdaptive(a)
	return c
}

// WithHedge delegates to middleware.Chain.WithHedge.
func (c *Composer[T]) WithHedge(h hedge.Hedge[T]) *Composer[T] {
	c.chain.WithHedge(h)
	return c
}

// Execute runs fn through the composed pipeline.
func (c *Composer[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	return c.chain.Execute(ctx, fn)
}

// usdToMicros converts whole US dollars to integer micro-USD, rounding to the
// nearest micro. It is the single guarded conversion shared by the MaxCost
// ceiling and the per-call CostFunc charge so the safety guards cannot drift
// between the two call sites.
//
// It returns ok=false for any value that cannot be safely represented:
// non-positive, NaN, ±Inf, or a magnitude that would overflow int64 after
// scaling. Callers decide how an invalid value surfaces (MaxCost panics; a
// bad CostFunc return charges nothing).
func usdToMicros(usd float64) (micros int64, ok bool) {
	if math.IsNaN(usd) || math.IsInf(usd, 0) || usd <= 0 {
		return 0, false
	}
	scaled := usd * usdMicrosPerUSD
	// Guard the rounded result against int64 overflow. math.MaxInt64 is not
	// exactly representable as a float64; comparing against the next lower
	// representable value (2^63, i.e. float64(math.MaxInt64)+1 rounds up) keeps
	// the conversion strictly inside range.
	if scaled >= float64(math.MaxInt64) {
		return 0, false
	}
	return int64(math.Round(scaled)), true
}

// costFuncToCharge adapts the convenience CostFunc (float USD over any) to the
// budget package's typed Charge (Cost over T). A nil CostFunc charges nothing.
// A non-positive, non-finite, or overflowing return is guarded to charge zero
// via the shared usdToMicros helper, so a bad cost reading cannot corrupt
// accounting or saturate to an instant breach.
func costFuncToCharge[T any](costFunc func(result any, err error) float64) budget.Charge[T] {
	if costFunc == nil {
		return nil
	}
	return func(_ context.Context, result T, err error) budget.Cost {
		micros, ok := usdToMicros(costFunc(result, err))
		if !ok {
			return budget.Cost{}
		}
		return budget.Cost{USDMicros: micros}
	}
}
