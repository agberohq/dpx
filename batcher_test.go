package dpx

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func defaultBatchConfig() BatchConfig {
	return BatchConfig{
		MaxEntries: 256,
		MaxBytes:   4 << 20,
		MinAge:     50 * time.Microsecond,
		MaxAge:     2 * time.Millisecond,
		EMAAlpha:   0.1,
		K:          8.0,
	}
}

// Basic Wait behaviour

func TestBatcher_WaitReturnsNilOnTimer(t *testing.T) {
	b := newBatcher(defaultBatchConfig(), nil)
	ctx := context.Background()

	start := time.Now()
	err := b.Wait(ctx, 100)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Wait returned error: %v", err)
	}
	// MinAge is 50µs; should return within MaxAge (2ms) plus slack.
	if elapsed > 50*time.Millisecond {
		t.Errorf("Wait took too long: %v", elapsed)
	}
}

func TestBatcher_WaitReturnsCtxErrOnCancel(t *testing.T) {
	cfg := defaultBatchConfig()
	cfg.MinAge = 10 * time.Millisecond // long enough to cancel first
	cfg.MaxAge = 10 * time.Millisecond
	b := newBatcher(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := b.Wait(ctx, 100)
	if err != context.Canceled {
		t.Errorf("got %v, want context.Canceled", err)
	}
}

func TestBatcher_WaitReturnsImmediatelyOnEntryThreshold(t *testing.T) {
	cfg := defaultBatchConfig()
	cfg.MaxEntries = 2
	cfg.MinAge = 10 * time.Second // would block forever if threshold not hit
	cfg.MaxAge = 10 * time.Second
	b := newBatcher(cfg, nil)

	ctx := context.Background()

	// Pre-fill inFlight to threshold.
	b.inFlight.Store(2)

	start := time.Now()
	err := b.Wait(ctx, 100)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Wait: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("should have returned immediately, took %v", elapsed)
	}
}

func TestBatcher_WaitReturnsImmediatelyOnByteThreshold(t *testing.T) {
	cfg := defaultBatchConfig()
	cfg.MaxBytes = 100
	cfg.MinAge = 10 * time.Second
	cfg.MaxAge = 10 * time.Second
	b := newBatcher(cfg, nil)

	ctx := context.Background()
	b.inBytes.Store(100)

	start := time.Now()
	err := b.Wait(ctx, 50)
	elapsed := time.Since(start)

	if err != nil {
		t.Errorf("Wait: %v", err)
	}
	if elapsed > 50*time.Millisecond {
		t.Errorf("should have returned immediately on byte threshold, took %v", elapsed)
	}
}

// SetConfig (hot-reload)

func TestBatcher_SetConfig_TakesEffect(t *testing.T) {
	b := newBatcher(defaultBatchConfig(), nil)
	ctx := context.Background()

	// Change MaxEntries to 1 so next Wait exits immediately via threshold.
	b.inFlight.Store(1)
	b.SetConfig(BatchConfig{
		MaxEntries: 1,
		MaxBytes:   4 << 20,
		MinAge:     10 * time.Second,
		MaxAge:     10 * time.Second,
		EMAAlpha:   0.1,
		K:          8.0,
	})

	start := time.Now()
	_ = b.Wait(ctx, 0)
	if time.Since(start) > 50*time.Millisecond {
		t.Error("SetConfig did not take effect before next Wait")
	}
}

// inFlight accounting

func TestBatcher_InFlightDecrementedAfterWait(t *testing.T) {
	b := newBatcher(defaultBatchConfig(), nil)
	ctx := context.Background()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = b.Wait(ctx, 500)
	}()
	wg.Wait()

	if b.inFlight.Load() != 0 {
		t.Errorf("inFlight = %d after Wait, want 0", b.inFlight.Load())
	}
	if b.inBytes.Load() != 0 {
		t.Errorf("inBytes = %d after Wait, want 0", b.inBytes.Load())
	}
}

func TestBatcher_InFlightDecrementedOnCancel(t *testing.T) {
	cfg := defaultBatchConfig()
	cfg.MinAge = 5 * time.Second
	cfg.MaxAge = 5 * time.Second
	b := newBatcher(cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		_ = b.Wait(ctx, 200)
		close(done)
	}()

	// Let the goroutine enter Wait and register inFlight.
	time.Sleep(5 * time.Millisecond)
	cancel()
	<-done

	if b.inFlight.Load() != 0 {
		t.Errorf("inFlight not decremented after cancel: %d", b.inFlight.Load())
	}
}

// EMA adaptation

func TestBatcher_EMAUpdatedOnSuccessiveWaits(t *testing.T) {
	b := newBatcher(defaultBatchConfig(), nil)
	ctx := context.Background()

	// Before any Wait: lastAt is zero, EMA stays 0.
	if b.ema != 0 {
		t.Errorf("initial EMA = %f, want 0", b.ema)
	}

	_ = b.Wait(ctx, 10)
	_ = b.Wait(ctx, 10)

	b.mu.Lock()
	ema := b.ema
	b.mu.Unlock()

	// After two calls, EMA should be non-zero (computed from inter-arrival time).
	if ema == 0 {
		t.Error("EMA should be non-zero after two Wait calls")
	}
}

// Concurrency

func TestBatcher_ConcurrentWait_NoRace(t *testing.T) {
	b := newBatcher(defaultBatchConfig(), nil)
	ctx := context.Background()

	var completed atomic.Int64
	var wg sync.WaitGroup
	const goroutines = 20

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = b.Wait(ctx, 100)
			completed.Add(1)
		}()
	}
	wg.Wait()

	if completed.Load() != goroutines {
		t.Errorf("completed = %d, want %d", completed.Load(), goroutines)
	}
	if b.inFlight.Load() != 0 {
		t.Errorf("inFlight not zero after all goroutines complete: %d", b.inFlight.Load())
	}
}

// clampDuration (package helper)

func TestClampDuration(t *testing.T) {
	cases := []struct {
		d, min, max time.Duration
		want        time.Duration
	}{
		{50 * time.Microsecond, 100 * time.Microsecond, 2 * time.Millisecond, 100 * time.Microsecond},  // below min
		{5 * time.Millisecond, 100 * time.Microsecond, 2 * time.Millisecond, 2 * time.Millisecond},     // above max
		{1 * time.Millisecond, 100 * time.Microsecond, 2 * time.Millisecond, 1 * time.Millisecond},     // in range
		{100 * time.Microsecond, 100 * time.Microsecond, 2 * time.Millisecond, 100 * time.Microsecond}, // at min
		{2 * time.Millisecond, 100 * time.Microsecond, 2 * time.Millisecond, 2 * time.Millisecond},     // at max
	}
	for _, tc := range cases {
		got := clampDuration(tc.d, tc.min, tc.max)
		if got != tc.want {
			t.Errorf("clampDuration(%v, %v, %v) = %v, want %v", tc.d, tc.min, tc.max, got, tc.want)
		}
	}
}
