package dpx

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/agberohq/dpx/shared"
)

func TestWatcher_RegisterAndReceiveNotification(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "wallet:")
	w.notify("wallet:alice", nil)

	select {
	case <-ch:
	case <-time.After(100 * time.Millisecond):
		t.Error("notification not received within 100ms")
	}
}

func TestWatcher_PrefixMatchRequired(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "wallet:")
	w.notify("ledger:alice", nil)

	select {
	case <-ch:
		t.Error("notification fired for non-matching prefix")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestWatcher_CancelRemovesChannel(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())

	ch := w.register(ctx, "k:")
	cancel()

	time.Sleep(20 * time.Millisecond)

	w.mu.RLock()
	remaining := len(w.watches["k:"])
	w.mu.RUnlock()

	if remaining != 0 {
		t.Errorf("after cancel: %d channels remain, want 0", remaining)
	}

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

	for i := 0; i < watchChanBuf; i++ {
		w.notify("k:x", m)
	}

	w.notify("k:x", m)

	if m.WatchDropped.Load() != 1 {
		t.Errorf("WatchDropped = %d, want 1", m.WatchDropped.Load())
	}

	drained := 0
	for {
		select {
		case <-ch:
			drained++
		default:
			if drained != watchChanBuf {
				t.Errorf("drained %d, want %d", drained, watchChanBuf)
			}
			return
		}
	}
}

func TestWatcher_NotifyBatch_FiresForEachKey(t *testing.T) {
	w := newWatcherMap()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := w.register(ctx, "w:")

	writes := []shared.WriteEntry{
		{Op: shared.OpSet, Key: []byte("w:alice"), Value: []byte("1")},
		{Op: shared.OpCredit, Key: []byte("w:bob"), Value: []byte("2")},
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
	writes := []shared.WriteEntry{{Op: shared.OpSet, Key: []byte("k"), Value: []byte("v")}}
	w.NotifyBatch(writes, nil)
}

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
			select {
			case <-ch:
			case <-time.After(50 * time.Millisecond):
			}
		}()
	}

	for i := 0; i < 5; i++ {
		go func() { w.notify("x:key", nil) }()
	}

	wg.Wait()
}
