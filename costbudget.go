package fortify

import (
	"context"
	"math"
	"time"

	"go.klarlabs.de/fortify/budget"
	"go.klarlabs.de/fortify/middleware"
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
	// Must be positive; a non-positive value makes WithCostBudget a no-op
	// that never gates (and is almost always a misconfiguration).
	MaxCost float64

	// CostFunc reports the cost, in US dollars, that an operation
	// consumed. It is invoked once per call regardless of err, so a
	// failed attempt that still cost money can be charged. The result is
	// passed as any; type-assert it to your operation's result type.
	// Nil charges nothing (the budget then never advances on cost).
	CostFunc func(result any, err error) float64

	// ResetAfter, when positive, turns the budget into a rolling window:
	// the accumulated spend is automatically cleared once this much
	// wall-clock time has elapsed. Zero (the default) makes the ceiling
	// apply for the lifetime of the chain.
	ResetAfter time.Duration

	// clock overrides the time source for ResetAfter windowing. Unexported;
	// set via WithClockForTest. Nil defaults to time.Now.
	clock func() time.Time
}

// WithClockForTest returns a copy of the config with a custom clock used
// to drive the ResetAfter window. It exists so callers (and tests) can
// exercise auto-reset deterministically without sleeping.
func (c CostBudgetConfig) WithClockForTest(now func() time.Time) CostBudgetConfig {
	c.clock = now
	return c
}

// Composer is the fluent entry point for assembling a resilience pipeline
// from the top-level fortify package. It wraps the middleware.Chain
// composer so spec-level convenience methods (WithCostBudget) and the
// lower-level pattern wiring share one execution model.
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
func (c *Composer[T]) WithCostBudget(cfg CostBudgetConfig) *Composer[T] {
	b, err := budget.New[T](budget.Config[T]{
		Max:        budget.Cost{USDMicros: usdToMicros(cfg.MaxCost)},
		Charge:     costFuncToCharge[T](cfg.CostFunc),
		ResetAfter: cfg.ResetAfter,
		Clock:      cfg.clock,
	})
	if err != nil {
		// Max non-positive: register a pass-through so the chain still
		// composes. A non-positive ceiling cannot gate anything, matching
		// "no budget configured" semantics rather than panicking the
		// builder.
		return c
	}
	c.chain.WithBudget(b)
	return c
}

// Execute runs fn through the composed pipeline.
func (c *Composer[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	return c.chain.Execute(ctx, fn)
}

// usdToMicros converts whole US dollars to integer micro-USD, rounding to
// the nearest micro. Non-positive input yields 0 so budget.New rejects it.
func usdToMicros(usd float64) int64 {
	if usd <= 0 {
		return 0
	}
	return int64(math.Round(usd * usdMicrosPerUSD))
}

// costFuncToCharge adapts the convenience CostFunc (float USD over any)
// to the budget package's typed Charge (Cost over T). A nil CostFunc
// charges nothing.
func costFuncToCharge[T any](costFunc func(result any, err error) float64) budget.Charge[T] {
	if costFunc == nil {
		return nil
	}
	return func(_ context.Context, result T, err error) budget.Cost {
		usd := costFunc(any(result), err)
		if usd <= 0 {
			return budget.Cost{}
		}
		return budget.Cost{USDMicros: int64(math.Round(usd * usdMicrosPerUSD))}
	}
}
