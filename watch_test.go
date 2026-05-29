package dpx

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestWatcher_RegisterAndReceiveNotification(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "wallet:")

	// Notify on a key with the registered prefix.
	w.notify("wallet:alice", nil)

	select {
	case <-ch:
		// correct
	case <-time.After(100 * time.Millisecond):
		t.Error("notification not received within 100ms")
	}
}

func TestWatcher_PrefixMatchRequired(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "wallet:")
	w.notify("ledger:alice", nil) // different prefix — should not fire

	select {
	case <-ch:
		t.Error("notification fired for non-matching prefix")
	case <-time.After(20 * time.Millisecond):
		// correct: no notification
	}
}

func TestWatcher_CancelRemovesChannel(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())

	ch := w.register(ctx, "k:")
	cancel() // triggers cleanup goroutine

	// Wait for cleanup goroutine to run.
	time.Sleep(20 * time.Millisecond)

	w.mu.RLock()
	remaining := len(w.watches["k:"])
	w.mu.RUnlock()

	if remaining != 0 {
		t.Errorf("after cancel: %d channels remain, want 0", remaining)
	}

	// Channel must be closed.
	select {
	case _, ok := <-ch:
		if ok {
			t.Error("channel should be closed after cancel")
		}
	case <-time.After(50 * time.Millisecond):
		t.Error("channel not closed after cancel")
	}
}

func TestWatcher_MultipleWatchersSamePrefix(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch1 := w.register(ctx, "p:")
	ch2 := w.register(ctx, "p:")

	w.notify("p:key", nil)

	for i, ch := range []<-chan struct{}{ch1, ch2} {
		select {
		case <-ch:
			// correct
		case <-time.After(100 * time.Millisecond):
			t.Errorf("channel %d: notification not received", i+1)
		}
	}
}

func TestWatcher_DroppedWhenBufferFull(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m := &Metrics{}
	ch := w.register(ctx, "k:")

	// Drain the channel buffer by filling it, then send one more.
	for i := 0; i < watchChanBuf; i++ {
		// Create a goroutine to receive and drain
		go func() { <-ch }()
	}

	// Give goroutines time to drain
	time.Sleep(10 * time.Millisecond)

	// Now fill the buffer for real
	for i := 0; i < watchChanBuf; i++ {
		select {
		case chInner := <-w.register(context.Background(), "k:"):
			_ = chInner
		default:
		}
	}

	// Buffer is full — next notify should drop and increment WatchDropped.
	w.notify("k:x", m)

	if m.WatchDropped.Load() != 1 {
		t.Errorf("WatchDropped = %d, want 1", m.WatchDropped.Load())
	}
}

func TestWatcher_NotifyBatch_FiresForEachKey(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "w:")

	writes := []WriteEntry{
		{Op: OpSet, Key: []byte("w:alice"), Value: []byte("1")},
		{Op: OpCredit, Key: []byte("w:bob"), Value: []byte("2")},
	}
	w.NotifyBatch(writes, nil)

	count := 0
	deadline := time.After(100 * time.Millisecond)
	for {
		select {
		case <-ch:
			count++
			if count == 2 {
				return
			}
		case <-deadline:
			t.Errorf("NotifyBatch: got %d notifications, want 2", count)
			return
		}
	}
}

func TestWatcher_NotifyBatch_EmptyWatchesIsNoop(t *testing.T) {
	w := newWatcherMap()
	// No registered watchers — must not panic.
	writes := []WriteEntry{{Op: OpSet, Key: []byte("k"), Value: []byte("v")}}
	w.NotifyBatch(writes, nil)
}

// Concurrency

func TestWatcher_ConcurrentRegisterAndNotify(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	const goroutines = 10

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := w.register(ctx, "x:")
			// Drain any notifications that arrive.
			select {
			case <-ch:
			case <-time.After(50 * time.Millisecond):
			}
		}()
	}

	// Notify concurrently with registrations.
	for i := 0; i < 5; i++ {
		go func() { w.notify("x:key", nil) }()
	}

	wg.Wait()
	// Race detector will catch any data races.
}
