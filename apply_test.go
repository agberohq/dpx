package dpx

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/agberohq/dpx/engine/memory"
	"github.com/agberohq/dpx/shared"
	"github.com/olekukonko/hlc"
	"github.com/olekukonko/jack"
)

// fakeProposer implements shared.Proposer for testing.
type fakeProposer struct {
	result shared.ApplyResult
	calls  int
}

func (f *fakeProposer) Propose(_ []byte) (shared.ApplyResult, error) {
	f.calls++
	return f.result, nil
}
func (f *fakeProposer) Shutdown() error { return nil }

func nodeWithFake(t *testing.T, fp *fakeProposer) *Node {
	t.Helper()
	eng := memory.New()
	if err := eng.Open(); err != nil {
		t.Fatalf("engine Open: %v", err)
	}

	cfg := Config{Engine: eng}
	applyDefaults(&cfg)
	watchers := newWatcherMap(nil)

	n := &Node{
		proposer: fp,
		engine:   eng,
		batcher:  newBatcher(cfg.Batch, cfg.Telemetry),
		watchers: watchers,
		clock:    hlc.NewClock(),
	}
	n.retry = jack.NewRetry(
		jack.RetryWithMaxAttempts(cfg.Retry.MaxAttempts),
		jack.RetryWithBaseDelay(cfg.Retry.BaseDelay),
		jack.RetryWithMaxDelay(cfg.Retry.MaxDelay),
		jack.RetryWithMultiplier(cfg.Retry.Multiplier),
		jack.RetryWithJitter(true),
		jack.RetryWithRetryIf(func(err error) bool { return errors.Is(err, ErrConflict) }),
	)
	sd := jack.NewShutdown(jack.ShutdownWithTimeout(cfg.ShutdownTimeout))
	sd.RegisterWithPriority("watchers", 0, func(_ context.Context) error { watchers.closeAll(); return nil })
	sd.RegisterWithPriority("engine", 1, func(_ context.Context) error { return eng.Close() })
	n.shutdown = sd

	t.Cleanup(func() { n.Close() })
	return n
}

func TestApply_ConflictExhausted(t *testing.T) {
	fp := &fakeProposer{result: shared.ApplyResult{Conflict: true}}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	err := node.RunInTx(ctx, func(tx KVTx) error {
		return tx.Set(ctx, []byte("k"), []byte("v"))
	})
	if !errors.Is(err, ErrConflictExhausted) {
		t.Errorf("got %v, want ErrConflictExhausted", err)
	}
	if fp.calls < 2 {
		t.Errorf("expected multiple Propose calls, got %d", fp.calls)
	}
}

func TestApply_FatalError_NotRetried(t *testing.T) {
	fatalErr := errors.New("engine exploded")
	fp := &fakeProposer{result: shared.ApplyResult{Err: fatalErr}}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	err := node.RunInTx(ctx, func(tx KVTx) error {
		return tx.Set(ctx, []byte("k"), []byte("v"))
	})
	if !errors.Is(err, fatalErr) {
		t.Errorf("got %v, want fatalErr", err)
	}
	if fp.calls != 1 {
		t.Errorf("fatal error must not be retried: got %d calls, want 1", fp.calls)
	}
}

func TestApply_ReadOnlyTx_NeverProposed(t *testing.T) {
	fp := &fakeProposer{}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	err := node.RunInTx(ctx, func(tx KVTx) error {
		_, _ = tx.Get(ctx, []byte("missing"))
		return nil
	})
	if err != nil {
		t.Errorf("read-only tx should not error: %v", err)
	}
	if fp.calls != 0 {
		t.Errorf("read-only tx must not call Propose, got %d", fp.calls)
	}
}

func TestApply_BusinessError_NotRetried(t *testing.T) {
	fp := &fakeProposer{}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	bizErr := errors.New("business logic failed")
	err := node.RunInTx(ctx, func(tx KVTx) error {
		return bizErr
	})
	if !errors.Is(err, bizErr) {
		t.Errorf("got %v, want bizErr", err)
	}
	if fp.calls != 0 {
		t.Errorf("business error must not reach Propose, got %d calls", fp.calls)
	}
}

func TestApply_SuccessfulPropose_NoError(t *testing.T) {
	fp := &fakeProposer{result: shared.ApplyResult{}}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	err := node.RunInTx(ctx, func(tx KVTx) error {
		return tx.Set(ctx, []byte("k"), []byte("v"))
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if fp.calls != 1 {
		t.Errorf("expected exactly 1 Propose call, got %d", fp.calls)
	}
}

func TestApply_ReservedKeyRejected_BeforePropose(t *testing.T) {
	fp := &fakeProposer{}
	node := nodeWithFake(t, fp)
	ctx := context.Background()

	err := node.RunInTx(ctx, func(tx KVTx) error {
		return tx.Set(ctx, []byte("__dpx:anything"), []byte("v"))
	})
	if !errors.Is(err, ErrReservedKey) {
		t.Errorf("got %v, want ErrReservedKey", err)
	}
	if fp.calls != 0 {
		t.Errorf("reserved key must not reach Propose, got %d calls", fp.calls)
	}
}

func TestApply_ClosedNode_RejectsRunInTx(t *testing.T) {
	fp := &fakeProposer{}
	node := nodeWithFake(t, fp)
	node.Close()

	err := node.RunInTx(context.Background(), func(tx KVTx) error {
		return tx.Set(context.Background(), []byte("k"), []byte("v"))
	})
	if !errors.Is(err, ErrStoreClosed) {
		t.Errorf("got %v, want ErrStoreClosed", err)
	}
}

func TestApply_WatcherNotified_ViaFakeProposer(t *testing.T) {
	fp := &fakeProposer{}
	node := nodeWithFake(t, fp)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch, err := node.WatchKey(ctx, []byte("w:"))
	if err != nil {
		t.Fatalf("WatchKey: %v", err)
	}

	node.watchers.NotifyBatch([]shared.WriteEntry{
		{Op: shared.OpSet, Key: []byte("w:alice"), Value: []byte("1")},
	}, nil)

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error("watcher not notified")
	}
}
