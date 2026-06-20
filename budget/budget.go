// Package budget enforces per-chain cost ceilings for operations whose
// cost cannot be capped by attempt count alone (LLM calls, paid APIs).
//
// A Budget tracks accumulated cost across calls to its Execute method
// and refuses further work once any configured maximum is reached. The
// caller supplies a Charge callback that converts each operation's
// result and error into a Cost; Budget aggregates those costs atomically.
//
// Cost is denominated in three independent dimensions:
//
//   - Tokens (any unit caller chooses; typically LLM tokens).
//   - USDMicros (1_000_000 = $1; integer to avoid float drift).
//   - Calls (incremented automatically; Charge cannot affect it).
//
// Budgets may be capped indefinitely (manual Reset) or over a rolling
// time window via Config.ResetAfter, which auto-clears the accumulated
// cost once the window elapses.
//
// Sensitive payloads: budget never inspects the operation result or
// error contents beyond what Charge returns. Charge must not return
// content fields it does not want surfaced via OnExceeded.
//
// Example:
//
//	b := budget.New[Response](budget.Config[Response]{
//	    Max: budget.Cost{Tokens: 10_000, USDMicros: 50_000}, // $0.05
//	    Charge: func(_ context.Context, r Response, _ error) budget.Cost {
//	        return budget.Cost{Tokens: int64(r.Usage.Total)}
//	    },
//	})
//	out, err := b.Execute(ctx, callLLM)
package budget

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// ErrBudgetExceeded is the sentinel returned (wrapped by *BudgetExceededError)
// when any configured maximum is reached. Callers can match it with
// errors.Is and extract the structured fields with errors.As.
var ErrBudgetExceeded = errors.New("budget exceeded")

// Cost is an additive triple measured in tokens, micro-USD, and calls.
// Zero-valued fields are ignored when comparing against a Max.
type Cost struct {
	Tokens    int64
	USDMicros int64
	Calls     int64
}

// Add returns the sum of two costs without mutating either operand.
func (c Cost) Add(o Cost) Cost {
	return Cost{
		Tokens:    c.Tokens + o.Tokens,
		USDMicros: c.USDMicros + o.USDMicros,
		Calls:     c.Calls + o.Calls,
	}
}

// Charge converts the result of an operation into a Cost. It is invoked
// once per Execute call regardless of err, so callers may charge for
// failed attempts that still consumed tokens (e.g., a partial completion
// returned alongside a stream error).
type Charge[T any] func(ctx context.Context, result T, err error) Cost

// Config configures a Budget. Only fields set on Max are enforced; a
// zero Tokens cap means tokens are not capped. The Calls cap is
// enforced regardless of whether Charge contributes to it (Calls is
// incremented automatically per Execute).
type Config[T any] struct {
	// Max is the ceiling. Any field set to a positive value is enforced;
	// non-positive fields are unbounded.
	Max Cost

	// Charge converts an operation result/err into the Cost it consumed.
	// May be nil if the only enforced dimension is Calls.
	Charge Charge[T]

	// OnExceeded fires once when a budget dimension is first breached.
	// Receives the aggregate cost after the breaching call. Synchronous;
	// keep it short. Panics are recovered and logged via Logger.
	OnExceeded func(consumed Cost)

	// ResetAfter, when positive, enables a rolling time window: the
	// accumulated cost (and any breach) is automatically cleared once this
	// much wall-clock time has elapsed since the window opened. The window
	// opens on the first Execute and reopens on each auto-reset. A
	// non-positive value (the default) disables auto-reset; callers must
	// then drive Reset manually.
	ResetAfter time.Duration

	// Clock supplies the current time for ResetAfter windowing. Nil
	// defaults to time.Now. Override it to drive the rolling window from
	// a deterministic or virtualized clock (tests, simulations).
	Clock func() time.Time

	// Logger receives diagnostic warnings (callback panics, breach
	// notices). Nil disables internal logging.
	Logger *slog.Logger
}

// Budget enforces a Config across one or more Execute calls. Budgets
// are safe for concurrent use.
//
// Two storage modes share the same atomic counters:
//
//   - ResetAfter <= 0 (no window): the lock-free fast path. Counters are
//     mutated with bare atomic operations; windowMu is never taken.
//   - ResetAfter > 0 (rolling window): the windowed path. Every mutation of
//     the counters — the auto-reset and the per-Execute charge/breach
//     read-modify-write — is serialized under windowMu so a window reset can
//     never interleave with (and clobber) a concurrent charge. The atomics are
//     still the backing store, which keeps Consumed() and the breach fast-path
//     check lock-free for readers.
type Budget[T any] struct {
	cfg       Config[T]
	tokens    atomic.Int64
	usdMicros atomic.Int64
	calls     atomic.Int64
	breached  atomic.Bool

	// now is the clock source; overridable in tests for deterministic
	// ResetAfter behaviour. Defaults to time.Now.
	now func() time.Time

	// windowMu serializes the rolling-window bookkeeping AND the windowed
	// charge/breach sequence for ResetAfter. It is only taken when ResetAfter
	// is configured; the lock is otherwise uncontended (never acquired).
	windowMu    sync.Mutex
	windowStart time.Time
	windowOpen  bool
}

// New constructs a Budget. Returns an error if Max is entirely zero
// (which would make the Budget a no-op and is almost always a config
// bug).
func New[T any](cfg Config[T]) (*Budget[T], error) {
	if cfg.Max.Tokens <= 0 && cfg.Max.USDMicros <= 0 && cfg.Max.Calls <= 0 {
		return nil, errors.New("budget.New: at least one Max field must be positive")
	}
	clock := cfg.Clock
	if clock == nil {
		clock = time.Now
	}
	return &Budget[T]{cfg: cfg, now: clock}, nil
}

// Execute runs fn, charges the resulting cost against the budget, and
// returns *BudgetExceededError (wrapping ErrBudgetExceeded) if any
// dimension was exceeded by this call. The fn result and error are
// returned unchanged when the budget is not exceeded.
//
// If the budget was already exceeded before this call, fn is not run
// and *BudgetExceededError is returned immediately.
func (b *Budget[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	if b.cfg.ResetAfter > 0 {
		return b.executeWindowed(ctx, fn)
	}
	return b.executeLockFree(ctx, fn)
}

// executeLockFree is the no-window fast path (ResetAfter <= 0). It mutates the
// atomic counters without ever taking windowMu.
func (b *Budget[T]) executeLockFree(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	if b.breached.Load() {
		return zero, b.exceededError(b.snapshot())
	}

	// Pre-charge a call so concurrent racers don't all slip past the cap.
	calls := b.calls.Add(1)
	if b.cfg.Max.Calls > 0 && calls > b.cfg.Max.Calls {
		b.markBreach()
		return zero, b.exceededError(b.snapshot())
	}

	result, err := fn(ctx)

	if b.cfg.Charge != nil {
		c := b.cfg.Charge(ctx, result, err)
		if c.Tokens > 0 {
			b.tokens.Add(c.Tokens)
		}
		if c.USDMicros > 0 {
			b.usdMicros.Add(c.USDMicros)
		}
	}

	consumed := b.snapshot()
	if b.over(consumed) {
		b.markBreach()
		return result, b.exceededError(consumed)
	}

	return result, err
}

// executeWindowed is the rolling-window path (ResetAfter > 0). The auto-reset
// and the charge/breach read-modify-write are guarded by the same windowMu so a
// window boundary can never interleave with a charge: a reset cannot clobber a
// concurrent Add, and no reader inside the critical section observes a
// half-reset state. fn itself runs OUTSIDE the lock so a slow operation does not
// serialize the whole budget; only the cheap bookkeeping before and after fn is
// guarded.
func (b *Budget[T]) executeWindowed(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	// Reset (if the window elapsed) and the pre-charge happen atomically.
	b.windowMu.Lock()
	b.autoResetLocked()

	if b.breached.Load() {
		err := b.exceededError(b.snapshot())
		b.windowMu.Unlock()
		return zero, err
	}

	calls := b.calls.Add(1)
	if b.cfg.Max.Calls > 0 && calls > b.cfg.Max.Calls {
		fireCost, fire := b.markBreachLocked()
		err := b.exceededError(b.snapshot())
		b.windowMu.Unlock()
		if fire {
			b.fireOnExceeded(fireCost)
		}
		return zero, err
	}
	b.windowMu.Unlock()

	// Run the operation without holding the lock.
	result, err := fn(ctx)

	// Charge + breach evaluation atomically w.r.t. a concurrent reset.
	b.windowMu.Lock()
	if b.cfg.Charge != nil {
		c := b.cfg.Charge(ctx, result, err)
		if c.Tokens > 0 {
			b.tokens.Add(c.Tokens)
		}
		if c.USDMicros > 0 {
			b.usdMicros.Add(c.USDMicros)
		}
	}
	consumed := b.snapshot()
	over := b.over(consumed)
	var fireCost Cost
	var fire bool
	if over {
		fireCost, fire = b.markBreachLocked()
	}
	b.windowMu.Unlock()

	if fire {
		b.fireOnExceeded(fireCost)
	}
	if over {
		return result, b.exceededError(consumed)
	}
	return result, err
}

// Consumed returns the current aggregate cost. Snapshot semantics: the
// fields are read independently and so may not be perfectly consistent
// under heavy concurrent contention, but each field is monotonic.
func (b *Budget[T]) Consumed() Cost {
	return b.snapshot()
}

// Reset clears the accumulated cost and re-arms OnExceeded. Useful for
// per-request budgets reused across handlers. Reset also closes any open
// rolling window; the next Execute reopens it (for ResetAfter budgets).
func (b *Budget[T]) Reset() {
	// For windowed budgets, clear the counters under windowMu so a manual Reset
	// is atomic w.r.t. a concurrent windowed Execute (same invariant as the
	// auto-reset). The no-window path stays lock-free.
	if b.cfg.ResetAfter > 0 {
		b.windowMu.Lock()
		b.tokens.Store(0)
		b.usdMicros.Store(0)
		b.calls.Store(0)
		b.breached.Store(false)
		b.windowOpen = false
		b.windowMu.Unlock()
		return
	}

	b.tokens.Store(0)
	b.usdMicros.Store(0)
	b.calls.Store(0)
	b.breached.Store(false)
}

// autoResetLocked clears the accumulated cost when the configured ResetAfter
// window has elapsed since it opened, then reopens the window. The window opens
// lazily on the first call. The window is wall-clock and ctx-independent: it is
// driven solely by Config.Clock (or time.Now) and is unaffected by context
// deadlines or cancellation.
//
// The caller MUST hold windowMu. The four atomic Stores happen inside this
// critical section so a reset is atomic w.r.t. the windowed charge/breach
// sequence in executeWindowed — no concurrent Add can be lost and no reader
// inside the section observes a half-reset state.
func (b *Budget[T]) autoResetLocked() {
	now := b.now()

	if !b.windowOpen {
		b.windowStart = now
		b.windowOpen = true
		return
	}
	if now.Sub(b.windowStart) < b.cfg.ResetAfter {
		return
	}
	// Window elapsed: open a fresh one and clear the accumulated cost atomically.
	b.windowStart = now
	b.tokens.Store(0)
	b.usdMicros.Store(0)
	b.calls.Store(0)
	b.breached.Store(false)
}

func (b *Budget[T]) snapshot() Cost {
	return Cost{
		Tokens:    b.tokens.Load(),
		USDMicros: b.usdMicros.Load(),
		Calls:     b.calls.Load(),
	}
}

func (b *Budget[T]) over(c Cost) bool {
	if b.cfg.Max.Tokens > 0 && c.Tokens > b.cfg.Max.Tokens {
		return true
	}
	if b.cfg.Max.USDMicros > 0 && c.USDMicros > b.cfg.Max.USDMicros {
		return true
	}
	if b.cfg.Max.Calls > 0 && c.Calls > b.cfg.Max.Calls {
		return true
	}
	return false
}

// markBreach flips the breached flag and fires OnExceeded once. Used on the
// lock-free (no-window) path.
func (b *Budget[T]) markBreach() {
	if b.breached.CompareAndSwap(false, true) {
		if b.cfg.OnExceeded != nil {
			b.safeCallback(b.cfg.OnExceeded, b.snapshot())
		}
	}
}

// markBreachLocked flips the breached flag under windowMu but does NOT run the
// OnExceeded callback while holding the lock (user code must not be able to
// deadlock by re-entering Execute, nor stall every other goroutine on the
// budget). If this call won the CAS and a callback is configured, it returns the
// snapshot to fire; the caller must invoke fireOnExceeded after releasing the
// lock. A zero/false return means nothing to fire.
func (b *Budget[T]) markBreachLocked() (Cost, bool) {
	if b.breached.CompareAndSwap(false, true) && b.cfg.OnExceeded != nil {
		return b.snapshot(), true
	}
	return Cost{}, false
}

// fireOnExceeded runs the OnExceeded callback. Call it OUTSIDE windowMu.
func (b *Budget[T]) fireOnExceeded(c Cost) {
	if b.cfg.OnExceeded != nil {
		b.safeCallback(b.cfg.OnExceeded, c)
	}
}

func (b *Budget[T]) safeCallback(fn func(Cost), c Cost) {
	defer func() {
		if r := recover(); r != nil {
			if b.cfg.Logger != nil {
				b.cfg.Logger.Error("budget callback panic",
					slog.String("pattern", "budget"),
					slog.Any("panic", r),
				)
			}
		}
	}()
	fn(c)
}

func (b *Budget[T]) exceededError(c Cost) error {
	return &BudgetExceededError{Consumed: c, Max: b.cfg.Max}
}

// BudgetExceededError carries the consumed and configured limits at
// the moment of refusal. errors.Is(err, ErrBudgetExceeded) keeps
// matching; errors.As unpacks the structured fields.
type BudgetExceededError struct {
	// Consumed is the aggregate cost charged at the time of the breach.
	Consumed Cost
	// Max mirrors the Budget's configured Max for caller convenience.
	Max Cost
}

// Error implements error.
func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf("budget exceeded (tokens=%d/%d usd_micros=%d/%d calls=%d/%d)",
		e.Consumed.Tokens, e.Max.Tokens,
		e.Consumed.USDMicros, e.Max.USDMicros,
		e.Consumed.Calls, e.Max.Calls,
	)
}

// Unwrap allows errors.Is(err, ErrBudgetExceeded) to keep matching.
func (e *BudgetExceededError) Unwrap() error { return ErrBudgetExceeded }

// LogValue implements slog.LogValuer.
func (e *BudgetExceededError) LogValue() slog.Value {
	if e == nil {
		return slog.GroupValue()
	}
	return slog.GroupValue(
		slog.String("error", "budget_exceeded"),
		slog.Int64("consumed_tokens", e.Consumed.Tokens),
		slog.Int64("max_tokens", e.Max.Tokens),
		slog.Int64("consumed_usd_micros", e.Consumed.USDMicros),
		slog.Int64("max_usd_micros", e.Max.USDMicros),
		slog.Int64("consumed_calls", e.Consumed.Calls),
		slog.Int64("max_calls", e.Max.Calls),
	)
}
