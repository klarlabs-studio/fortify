package ratelimit_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"go.klarlabs.de/fortify/ratelimit"
)

// An omitted Interval silently means per-second. Someone writing
//
//	ratelimit.New(ratelimit.Config{Rate: 50, Burst: 50})
//
// for a 50-requests-per-minute provider quota gets 50/sec — sixty times over
// — and the limiter itself causes the 429s it was added to prevent (#66).
//
// New cannot return an error without breaking every caller, so the default
// stays. What it must not do is stay silent.
func TestOmittedIntervalIsAnnounced(t *testing.T) {
	tests := []struct {
		name     string
		config   ratelimit.Config
		wantWarn bool
	}{
		{
			name:     "omitted Interval warns",
			config:   ratelimit.Config{Rate: 50, Burst: 50},
			wantWarn: true,
		},
		{
			// Explicit per-second is a deliberate choice, not a guess.
			name:     "explicit Interval is silent",
			config:   ratelimit.Config{Rate: 50, Burst: 50, Interval: time.Second},
			wantWarn: false,
		},
		{
			name:     "explicit per-minute is silent",
			config:   ratelimit.Config{Rate: 50, Burst: 50, Interval: time.Minute},
			wantWarn: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			cfg := tc.config
			cfg.Logger = slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

			rl := ratelimit.New(cfg)
			defer func() { _ = rl.Close() }()

			got := strings.Contains(buf.String(), "Interval")
			if got != tc.wantWarn {
				t.Errorf("warned=%v, want %v\nlog: %s", got, tc.wantWarn, buf.String())
			}
			if tc.wantWarn {
				// The message has to say what it defaulted to and how to be
				// explicit, or the reader learns nothing they didn't know.
				for _, want := range []string{"1s", "Rate"} {
					if !strings.Contains(buf.String(), want) {
						t.Errorf("warning does not mention %q: %s", want, buf.String())
					}
				}
			}
		})
	}
}

// The warning must not change behaviour.
func TestOmittedIntervalStillDefaultsToPerSecond(t *testing.T) {
	rl := ratelimit.New(ratelimit.Config{Rate: 3, Burst: 3})
	defer func() { _ = rl.Close() }()

	ctx := context.Background()
	allowed := 0
	for i := 0; i < 10; i++ {
		if rl.Allow(ctx, "k") {
			allowed++
		}
	}
	if allowed != 3 {
		t.Errorf("allowed %d, want 3; the warning changed behaviour", allowed)
	}
}
