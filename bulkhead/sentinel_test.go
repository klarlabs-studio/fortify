package bulkhead_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.klarlabs.de/fortify/bulkhead"
	"go.klarlabs.de/fortify/ferrors"
)

// Execute's doc says "Returns ErrBulkheadFull if the request cannot be
// accommodated". The obvious way to act on that must compile and must work
// (#71) — previously `bulkhead.ErrBulkheadFull` was undefined, and a caller
// had to read the source to discover the sentinel lived in ferrors, a package
// they had no reason to import.
func TestErrBulkheadFullIsMatchableFromThisPackage(t *testing.T) {
	b := bulkhead.New[string](bulkhead.Config{MaxConcurrent: 1, MaxQueue: 0})

	release := make(chan struct{})
	occupied := make(chan struct{})
	go func() {
		_, _ = b.Execute(context.Background(), func(context.Context) (string, error) {
			close(occupied)
			<-release
			return "held", nil
		})
	}()
	<-occupied
	defer close(release)

	_, err := b.Execute(context.Background(), func(context.Context) (string, error) {
		return "", nil
	})
	if err == nil {
		t.Fatal("expected rejection while the only slot is held")
	}

	// The name the doc comment uses, in the package the doc comment is on.
	if !errors.Is(err, bulkhead.ErrBulkheadFull) {
		t.Errorf("errors.Is(err, bulkhead.ErrBulkheadFull) = false; err = %v", err)
	}
	// And it must remain the same sentinel, so existing ferrors-based
	// matching keeps working.
	if !errors.Is(err, ferrors.ErrBulkheadFull) {
		t.Errorf("errors.Is(err, ferrors.ErrBulkheadFull) = false; the alias is not the same value")
	}
	if bulkhead.ErrBulkheadFull != ferrors.ErrBulkheadFull {
		t.Error("bulkhead.ErrBulkheadFull must BE ferrors.ErrBulkheadFull, not a copy")
	}
	_ = time.Now
}
