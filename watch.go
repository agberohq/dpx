package dpx

import (
	"context"
	"strings"
	"sync"

	"github.com/agberohq/dpx/shared"
)

const watchChanBuf = 256

// watcherMap manages per-prefix watch channels.
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

// NotifyBatch satisfies the shared.WatchNotifier interface.
func (w *watcherMap) NotifyBatch(writes []shared.WriteEntry, metrics *shared.Metrics) {
	if len(w.watches) == 0 {
		return
	}
	// Convert shared.Metrics to dpx.Metrics for notify().
	// notify() only uses WatchDropped, so we pass nil if no local metrics.
	var m *Metrics
	if metrics != nil {
		m = &Metrics{} // dummy; notify only checks nil and calls Add
	}
	for i := range writes {
		w.notify(string(writes[i].Key), m)
	}
}
