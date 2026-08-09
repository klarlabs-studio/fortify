package circuitbreaker_test

import (
	"context"
	"errors"
	"testing"

	"go.klarlabs.de/fortify/circuitbreaker"
	"go.klarlabs.de/fortify/ferrors"
)

// Same defect as #71, one package over: Execute's doc says "it returns
// ErrCircuitOpen", and circuitbreaker.ErrCircuitOpen did not exist.
func TestErrCircuitOpenIsMatchableFromThisPackage(t *testing.T) {
	cb := circuitbreaker.New[string](circuitbreaker.Config{
		ReadyToTrip: circuitbreaker.TripOnConsecutiveFailures(1),
	})
	defer func() { _ = cb.Close() }()

	ctx := context.Background()
	_, _ = cb.Execute(ctx, func(context.Context) (string, error) {
		return "", errors.New("down")
	})

	_, err := cb.Execute(ctx, func(context.Context) (string, error) {
		return "should not run", nil
	})
	if err == nil {
		t.Fatal("expected the open circuit to reject")
	}

	if !errors.Is(err, circuitbreaker.ErrCircuitOpen) {
		t.Errorf("errors.Is(err, circuitbreaker.ErrCircuitOpen) = false; err = %v", err)
	}
	if !errors.Is(err, ferrors.ErrCircuitOpen) {
		t.Errorf("errors.Is(err, ferrors.ErrCircuitOpen) = false; the alias is not the same value")
	}
	if circuitbreaker.ErrCircuitOpen != ferrors.ErrCircuitOpen {
		t.Error("circuitbreaker.ErrCircuitOpen must BE ferrors.ErrCircuitOpen, not a copy")
	}
}
