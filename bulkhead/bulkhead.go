// Package bulkhead provides concurrency limiting for operations,
// preventing resource exhaustion by isolating concurrent executions.
//
// The bulkhead package implements a semaphore-based pattern to limit
// the number of concurrent operations, with optional queueing for overflow.
// It supports configurable queue timeouts and rejection callbacks.
//
// Example usage:
//
//	bh := bulkhead.New[[]byte](bulkhead.Config{
//	    MaxConcurrent: 5,
//	    MaxQueue:      10,
//	    QueueTimeout:  5 * time.Second,
//	    OnRejected: func() {
//	        log.Println("Request rejected - bulkhead full")
//	    },
//	})
//
//	data, err := bh.Execute(ctx, func(ctx context.Context) ([]byte, error) {
//	    return fetchData(ctx)
//	})
package bulkhead

import (
	"context"
	"log/slog"
	"sync"

	"go.klarlabs.de/fortify/ferrors"
)

// ErrBulkheadFull is returned when a request cannot be admitted: the worker
// slots are full and either there is no queue or the queue is full too.
//
// It is the same value as ferrors.ErrBulkheadFull, re-exported here because
// this package's own documentation names it. Without that, the obvious line
// to write does not compile:
//
//	if errors.Is(err, bulkhead.ErrBulkheadFull) { ... }
//
// and a caller has to read the source to learn the sentinel lives in ferrors
// — a package using bulkhead has no other reason to import. errors.Is against
// either spelling matches, so existing ferrors-based code is unaffected.
var ErrBulkheadFull = ferrors.ErrBulkheadFull

// Bulkhead is a generic interface for enforcing concurrency limits.
// It isolates operations to prevent resource exhaustion and provides queue overflow handling.
type Bulkhead[T any] interface {
	// Execute runs the given function within the concurrency limit.
	// If the bulkhead is at capacity, the request is queued or rejected based on configuration.
	// Returns ErrBulkheadFull if the request cannot be accommodated.
	Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error)

	// Close signals the bulkhead to stop accepting new requests and shuts down the worker goroutine.
	//
	// IMPORTANT: Close() does NOT wait for in-flight executions to complete.
	// It immediately stops the worker and signals rejection for new requests.
	// The caller is responsible for ensuring all Execute() calls have completed before
	// relying on resource cleanup.
	//
	// Typical usage with coordination:
	//   // Stop sending new requests
	//   stopAcceptingRequests()
	//   // Wait for all in-flight to complete
	//   wg.Wait()
	//   // Now safe to close
	//   bh.Close()
	//
	// It is safe to call Close() multiple times.
	Close() error
}

// bulkhead is the concrete implementation of Bulkhead.
//
//nolint:govet // fieldalignment: field order optimized for logical grouping and sync.Once size
type bulkhead[T any] struct {
	once     sync.Once     // Ensures Close is called only once (16 bytes with padding)
	sem      chan struct{} // Semaphore for concurrency control
	queue    chan *request[T]
	queueSem chan struct{} // Semaphore for queue capacity
	done     chan struct{} // Signals shutdown to worker
	config   Config
}

// request represents a queued execution request.
type request[T any] struct {
	ctx      context.Context
	fn       func(context.Context) (T, error)
	resultCh chan result[T]
}

// result holds the execution result.
type result[T any] struct {
	value T
	err   error
}

// New creates a new Bulkhead instance with the given configuration.
func New[T any](config Config) Bulkhead[T] {
	config.setDefaults()

	bh := &bulkhead[T]{
		config: config,
		sem:    make(chan struct{}, config.MaxConcurrent),
		done:   make(chan struct{}),
	}

	// Only create queue if MaxQueue > 0
	if config.MaxQueue > 0 {
		bh.queue = make(chan *request[T])
		bh.queueSem = make(chan struct{}, config.MaxQueue)
		// Start worker goroutine to process queue
		go bh.worker()
	}

	return bh
}

// Execute implements the Bulkhead interface.
func (b *bulkhead[T]) Execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	// Check if bulkhead is closed
	select {
	case <-b.done:
		return zero, ferrors.ErrBulkheadFull
	default:
	}

	// Try to acquire semaphore immediately
	select {
	case b.sem <- struct{}{}:
		// Got semaphore, execute directly
		return b.execute(ctx, fn)

	case <-ctx.Done():
		// Context already cancelled
		return zero, ctx.Err()

	case <-b.done:
		// Closed during execution attempt
		return zero, ferrors.ErrBulkheadFull

	default:
		// Bulkhead full, try to queue
		return b.enqueue(ctx, fn)
	}
}

// execute runs the function with the semaphore held.
func (b *bulkhead[T]) execute(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	defer func() {
		<-b.sem // Release semaphore
	}()

	result, err := fn(ctx)
	return result, err
}

// enqueue attempts to queue the request when bulkhead is full.
func (b *bulkhead[T]) enqueue(ctx context.Context, fn func(context.Context) (T, error)) (T, error) {
	var zero T

	// If no queue configured, reject immediately
	if b.config.MaxQueue == 0 {
		b.logRejection()
		if b.config.OnRejected != nil {
			b.safeCallback(b.config.OnRejected)
		}
		return zero, ferrors.ErrBulkheadFull
	}

	req := &request[T]{
		ctx:      ctx,
		fn:       fn,
		resultCh: make(chan result[T], 1),
	}

	// Determine queue timeout context
	queueCtx := ctx
	var cancel context.CancelFunc
	if b.config.QueueTimeout > 0 {
		queueCtx, cancel = context.WithTimeout(ctx, b.config.QueueTimeout)
		defer cancel()
	}

	// Try to acquire queue semaphore
	select {
	case b.queueSem <- struct{}{}:
		// Got queue slot, now send to queue channel
		defer func() {
			<-b.queueSem // Release queue slot when done
		}()

		select {
		case b.queue <- req:
			// Successfully queued, wait for result
			select {
			case res := <-req.resultCh:
				return res.value, res.err
			case <-queueCtx.Done():
				return zero, queueCtx.Err()
			case <-b.done:
				return zero, ferrors.ErrBulkheadFull
			}
		case <-queueCtx.Done():
			return zero, queueCtx.Err()
		case <-b.done:
			return zero, ferrors.ErrBulkheadFull
		}

	case <-queueCtx.Done():
		// Timeout while trying to get queue slot
		b.logRejection()
		if b.config.OnRejected != nil {
			b.safeCallback(b.config.OnRejected)
		}
		return zero, queueCtx.Err()

	case <-b.done:
		return zero, ferrors.ErrBulkheadFull

	default:
		// Queue full, reject immediately
		b.logRejection()
		if b.config.OnRejected != nil {
			b.safeCallback(b.config.OnRejected)
		}
		return zero, ferrors.ErrBulkheadFull
	}
}

// worker processes queued requests.
func (b *bulkhead[T]) worker() {
	for {
		select {
		case req := <-b.queue:
			// Wait for semaphore in worker goroutine, so queue stays full
			// until we're ready to execute
			select {
			case b.sem <- struct{}{}:
				// Got semaphore, execute in goroutine to continue processing queue.
				// resultCh is buffered=1, so the send is non-blocking even if the
				// caller has already abandoned the request via queueCtx.Done().
				go func(r *request[T]) {
					defer func() {
						<-b.sem // Release semaphore
					}()
					value, err := r.fn(r.ctx)
					r.resultCh <- result[T]{value: value, err: err}
				}(req)

			case <-req.ctx.Done():
				// Context cancelled while waiting for semaphore.
				// Direct send is safe: resultCh is buffered=1.
				req.resultCh <- result[T]{err: req.ctx.Err()}

			case <-b.done:
				// Shutdown signal received: notify this request and drain remaining.
				req.resultCh <- result[T]{err: ferrors.ErrBulkheadFull}
				b.drainQueue()
				return
			}

		case <-b.done:
			// Shutdown signal received: drain remaining requests.
			b.drainQueue()
			return
		}
	}
}

// drainQueue notifies any queued requests that the bulkhead is closed.
// Called once during shutdown; uses non-blocking receive to avoid races
// with concurrent senders that observe b.done in their own select.
func (b *bulkhead[T]) drainQueue() {
	for {
		select {
		case req := <-b.queue:
			req.resultCh <- result[T]{err: ferrors.ErrBulkheadFull}
		default:
			return
		}
	}
}

// Close implements the Bulkhead interface.
// It signals shutdown and stops the worker goroutine.
//
// Close does NOT close the queue channel: senders in enqueue() observe b.done
// in their own select and bail out cleanly, avoiding a send-on-closed-channel panic.
func (b *bulkhead[T]) Close() error {
	b.once.Do(func() {
		close(b.done)
	})
	return nil
}

// logRejection logs rejection events using structured logging.
func (b *bulkhead[T]) logRejection() {
	if b.config.Logger != nil {
		b.config.Logger.Warn("bulkhead rejection",
			slog.Int("max_concurrent", b.config.MaxConcurrent),
			slog.Int("max_queue", b.config.MaxQueue),
		)
	}
}

// safeCallback executes a callback with panic recovery.
func (b *bulkhead[T]) safeCallback(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			if b.config.Logger != nil {
				b.config.Logger.Error("bulkhead callback panic",
					slog.String("pattern", "bulkhead"),
					slog.Any("panic", r),
				)
			}
		}
	}()
	fn()
}
