package dpx

import (
	"context"
	"strings"
	"sync"
)

const watchChanBuf = 256

// watcherMap manages per-prefix watch channels.
// Implements WatchNotifier so raft/fsm.go can call NotifyBatch without
// importing watch internals.
type watcherMap struct {
	mu      sync.RWMutex
	watches map[string][]chan struct{}
	done    chan struct{}
	closed  bool
}

func newWatcherMap() *watcherMap {
	return &watcherMap{
		watches: make(map[string][]chan struct{}),
		done:    make(chan struct{}),
	}
}

func (w *watcherMap) register(ctx context.Context, prefix string) <-chan struct{} {
	ch := make(chan struct{}, watchChanBuf)

	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		close(ch)
		return ch
	}
	w.watches[prefix] = append(w.watches[prefix], ch)
	w.mu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-w.done:
		}
		w.mu.Lock()
		arr := w.watches[prefix]
		for i, c := range arr {
			if c == ch {
				w.watches[prefix] = append(arr[:i], arr[i+1:]...)
				break
			}
		}
		if len(w.watches[prefix]) == 0 {
			delete(w.watches, prefix)
		}
		w.mu.Unlock()
		close(ch)
	}()

	return ch
}

// closeAll signals all watcher goroutines to exit.
// Called at shutdown priority 1, after Raft stops (priority 0).
func (w *watcherMap) closeAll() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	close(w.done)
}

func (w *watcherMap) notify(key string, metrics *Metrics) {
	w.mu.RLock()
	if w.closed {
		w.mu.RUnlock()
		return
	}
	defer w.mu.RUnlock()

	for prefix, chans := range w.watches {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, ch := range chans {
			select {
			case ch <- struct{}{}:
			default:
				if metrics != nil {
					metrics.WatchDropped.Add(1)
				}
			}
		}
	}
}

// NotifyBatch satisfies the WatchNotifier interface.
// Called by raft/fsm.go after each successful ApplyBatch.
func (w *watcherMap) NotifyBatch(writes []WriteEntry, metrics *Metrics) {
	if len(w.watches) == 0 {
		return
	}
	for i := range writes {
		w.notify(string(writes[i].Key), metrics)
	}
}
