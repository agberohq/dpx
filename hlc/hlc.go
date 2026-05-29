// Package hlc implements a Hybrid Logical Clock as described in:
//
//	Kulkarni et al., "Logical Physical Clocks and Consistent Snapshots
//	in Globally Distributed Databases", University at Buffalo, 2014.
//	https://cse.buffalo.edu/tech-reports/2014-04.pdf
//
// # Why HLC and not time.Now()
//
// Wall clocks (NTP) can go backwards, jump, or repeat the same millisecond
// for concurrent events. A logical clock (Lamport) is monotone but has no
// relationship to physical time. HLC combines both:
//
//   - Monotone: hlc(e) < hlc(f) whenever e happened-before f.
//   - Physical: |hlc - NTP| is bounded (within one NTP sync interval).
//   - Compact: fits in 64 bits (48-bit wall ms + 16-bit counter).
//
// For DPX, HLC is used to timestamp every committed Raft entry.
// Dragonboat provides the authoritative ordering (Raft log index), but
// HLC provides a human-readable, causally-correct wall-time timestamp for:
//   - GetDatabaseTime() — returned to Teller for ledger entry timestamps
//   - Feed event timestamps — so downstream consumers can order events
//     across nodes without understanding Raft log indices
//   - Snapshot reads — a snapshot at HLC time T is causally consistent
//
// # Packing into 64 bits
//
//	Bits 63–16 (48 bits): wall clock in milliseconds since Unix epoch.
//	                       Overflows year 10889 — sufficient.
//	Bits 15–0  (16 bits): logical counter c. Max 65535 events per ms.
//	                       At 50k TPS a single ms holds at most 50 events.
//
// # Thread safety
//
// Clock is safe for concurrent use. All state is protected by a mutex.
// Tick and Observe are the only mutating methods and are called from
// the single-goroutine Raft Update loop, so contention is minimal.
package hlc

import (
	"sync"
	"time"
)

const (
	counterBits = 16
	counterMask = (1 << counterBits) - 1 // 0xFFFF
	maxCounter  = counterMask            // 65535
)

// HLCTime is a hybrid logical timestamp.
// The zero value is a valid timestamp representing the epoch.
type HLCTime struct {
	WallMs  uint64 // milliseconds since Unix epoch
	Counter uint16 // logical counter for sub-millisecond ordering
}

// Pack encodes an HLCTime into a single uint64 for storage and wire transport.
// Layout: [48-bit WallMs][16-bit Counter]
func (t HLCTime) Pack() uint64 {
	return (t.WallMs << counterBits) | uint64(t.Counter)
}

// Unpack decodes a packed uint64 into an HLCTime.
func Unpack(v uint64) HLCTime {
	return HLCTime{
		WallMs:  v >> counterBits,
		Counter: uint16(v & counterMask),
	}
}

// Physical converts the wall-clock component to a time.Time.
// The counter component is dropped — use After() for causal ordering.
func (t HLCTime) Physical() time.Time {
	return time.UnixMilli(int64(t.WallMs)).UTC()
}

// After reports whether t causally follows u.
// Implements the strict total order: first compare WallMs, then Counter.
func (t HLCTime) After(u HLCTime) bool {
	if t.WallMs != u.WallMs {
		return t.WallMs > u.WallMs
	}
	return t.Counter > u.Counter
}

// Equal reports whether t and u are the same causal moment.
func (t HLCTime) Equal(u HLCTime) bool {
	return t.WallMs == u.WallMs && t.Counter == u.Counter
}

// IsZero reports whether t is the zero value.
func (t HLCTime) IsZero() bool {
	return t.WallMs == 0 && t.Counter == 0
}

// Clock is a Hybrid Logical Clock for a single DPX node.
// It maintains the maximum physical time observed and a counter
// for events that occur within the same physical millisecond.
type Clock struct {
	mu      sync.Mutex
	wallMs  uint64 // max physical ms seen so far
	counter uint16 // monotone counter for same-ms events
}

// New creates a Clock initialised to the current physical time.
func New() *Clock {
	return &Clock{
		wallMs:  nowMs(),
		counter: 0,
	}
}

// Now returns the current HLC time without advancing it.
// Safe to call at any time for read-only inspection.
func (c *Clock) Now() HLCTime {
	c.mu.Lock()
	t := HLCTime{WallMs: c.wallMs, Counter: c.counter}
	c.mu.Unlock()
	return t
}

// Tick advances the clock for a local or send event and returns the new time.
// This is the HLC "send or local event" rule from the paper (Figure 5):
//
//	pt = physical time now (ms)
//	if pt > l: l = pt, c = 0
//	else:       c = c + 1
//	return (l, c)
//
// Called by the DPX state machine on every committed Raft entry.
func (c *Clock) Tick() HLCTime {
	pt := nowMs()
	c.mu.Lock()
	defer c.mu.Unlock()

	if pt > c.wallMs {
		c.wallMs = pt
		c.counter = 0
	} else {
		if c.counter == maxCounter {
			// Counter overflow: advance wall clock by 1ms.
			// This keeps the clock monotone at the cost of a 1ms drift
			// beyond the physical clock. The paper bounds this drift.
			c.wallMs++
			c.counter = 0
		} else {
			c.counter++
		}
	}
	return HLCTime{WallMs: c.wallMs, Counter: c.counter}
}

// Observe advances the clock for a receive event, incorporating the
// timestamp t from a remote sender. Returns the new local time.
// This is the HLC "receive event" rule from the paper (Figure 5):
//
//	pt = physical time now (ms)
//	l_old = l
//	l = max(l_old, l.m, pt)
//	if l == l_old == l.m: c = max(c, c.m) + 1
//	if l == l_old > l.m:  c = c + 1
//	if l == l.m > l_old:  c = c.m + 1
//	if l == pt:           c = 0      (physical time dominates)
//	return (l, c)
//
// In DPX, Observe is called when a follower applies a log entry that
// carries an HLC timestamp from the leader. This propagates the leader's
// causal history to the follower.
func (c *Clock) Observe(t HLCTime) HLCTime {
	pt := nowMs()
	c.mu.Lock()
	defer c.mu.Unlock()

	lOld := c.wallMs
	cOld := c.counter

	// Advance wall component to the maximum of all three sources.
	lNew := max3(lOld, t.WallMs, pt)
	c.wallMs = lNew

	var cNew uint16
	switch {
	case lNew == lOld && lNew == t.WallMs:
		// All three agree: take max(c, c.m) + 1.
		m := maxU16(cOld, t.Counter)
		if m == maxCounter {
			c.wallMs++
			cNew = 0
		} else {
			cNew = m + 1
		}
	case lNew == lOld && lNew > t.WallMs:
		// Local time dominates: increment local counter.
		if cOld == maxCounter {
			c.wallMs++
			cNew = 0
		} else {
			cNew = cOld + 1
		}
	case lNew == t.WallMs && lNew > lOld:
		// Sender's time dominates: adopt sender counter + 1.
		// Check before incrementing to avoid uint16 overflow.
		if t.Counter == maxCounter {
			c.wallMs++
			cNew = 0
		} else {
			cNew = t.Counter + 1
		}
	default:
		// Physical time dominates: reset counter.
		cNew = 0
	}

	c.counter = cNew
	return HLCTime{WallMs: c.wallMs, Counter: c.counter}
}

// --- helpers -----------------------------------------------------------------

// nowMs returns the current physical time in milliseconds since Unix epoch.
func nowMs() uint64 {
	return uint64(time.Now().UnixMilli())
}

func max3(a, b, c uint64) uint64 {
	if a >= b && a >= c {
		return a
	}
	if b >= c {
		return b
	}
	return c
}

func maxU16(a, b uint16) uint16 {
	if a > b {
		return a
	}
	return b
}
