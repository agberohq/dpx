package dpx

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/agberohq/dpx/shared"
)

const watchChanBuf = 256

// watcherMap manages per-prefix watch channels.
type watcherMap struct {
	mu        sync.RWMutex
	watches   map[string][]chan struct{}
	done      chan struct{}
	closed    bool
	telemetry *shared.Telemetry // <-- ADDED
}

func newWatcherMap(telemetry *shared.Telemetry) *watcherMap { // <-- UPDATED
	return &watcherMap{
		watches:   make(map[string][]chan struct{}),
		done:      make(chan struct{}),
		telemetry: telemetry,
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
			sendStart := time.Now()
			select {
			case ch <- struct{}{}:
				if w.telemetry != nil {
					w.telemetry.WatchChannelSend.Record(time.Since(sendStart))
				}
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
	if w.telemetry != nil {
		defer func(start time.Time) {
			w.telemetry.WatchNotify.Record(time.Since(start))
		}(time.Now())
	}

	if len(w.watches) == 0 {
		return
	}
	// Pass real metrics pointer directly; notify() safely handles nil.
	for i := range writes {
		w.notify(string(writes[i].Key), metrics)
	}
}
