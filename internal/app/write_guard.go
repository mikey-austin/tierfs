package app

import (
	"sync"
	"time"
)

// WriteGuard tracks open write handles and recently-closed write handles per
// relative path. The Replicator consults it before starting a copy to avoid
// replicating files that are actively being written or have not yet been
// stable for the configured quiescence window.
//
// A file is considered "write-active" if any of the following are true:
//  1. At least one write handle is currently open (openCount > 0).
//  2. The last write handle was closed less than quiescence ago.
//
// Both conditions must be false before the Replicator will start a copy.
// This handles two common patterns in media workloads:
//
//   - Frigate keeps a segment file handle open for the duration of the segment
//     (condition 1): copy is blocked until Frigate calls close(2).
//   - A muxer re-opens a file to add a subtitle track 2s after the initial
//     write (condition 2): copy is blocked until 2s + quiescence has elapsed.
//
// WriteGuard is safe for concurrent use.
type WriteGuard struct {
	mu         sync.RWMutex
	entries    map[string]*writeEntry
	quiescence time.Duration // minimum idle time after last close before replication
}

type writeEntry struct {
	openCount int       // number of currently-open write handles
	lastClose time.Time // time of most recent Release/close
}

// NewWriteGuard creates a WriteGuard with the given quiescence duration.
// quiescence = 0 disables the idle window (only open handles block replication).
func NewWriteGuard(quiescence time.Duration) *WriteGuard {
	return &WriteGuard{
		entries:    make(map[string]*writeEntry),
		quiescence: quiescence,
	}
}

// Open records that a write handle has been opened for relPath.
// Called by writeHandle at creation time (FUSE Create and O_WRONLY/O_RDWR Open).
func (g *WriteGuard) Open(relPath string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entry(relPath)
	e.openCount++
}

// Close records that a write handle for relPath has been released.
// Called by writeHandle.Release().
func (g *WriteGuard) Close(relPath string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	e := g.entry(relPath)
	if e.openCount > 0 {
		e.openCount--
	}
	if e.openCount == 0 {
		e.lastClose = time.Now()
	}
}

// IsWriteActive reports whether relPath should be considered write-active and
// therefore must not be replicated yet.
//
// Returns (true, reason) if a copy should be blocked:
//   - "open write handle" — at least one handle is currently open
//   - "quiescence window" — last close was too recent
//
// Returns (false, "") when the file is safe to replicate.
func (g *WriteGuard) IsWriteActive(relPath string) (active bool, reason string) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	e, ok := g.entries[relPath]
	if !ok {
		return false, ""
	}
	if e.openCount > 0 {
		return true, "open write handle"
	}
	if g.quiescence > 0 && !e.lastClose.IsZero() {
		elapsed := time.Since(e.lastClose)
		if elapsed < g.quiescence {
			return true, "quiescence window"
		}
	}
	return false, ""
}

// Forget removes the entry for relPath. Called when a file is deleted or
// definitively evicted. Prevents unbounded map growth.
func (g *WriteGuard) Forget(relPath string) {
	g.mu.Lock()
	delete(g.entries, relPath)
	g.mu.Unlock()
}

// entry returns the writeEntry for relPath, creating it if absent.
// Must be called with mu held for writing.
func (g *WriteGuard) entry(relPath string) *writeEntry {
	e, ok := g.entries[relPath]
	if !ok {
		e = &writeEntry{}
		g.entries[relPath] = e
	}
	return e
}

// Snapshot returns a point-in-time view of all active entries for diagnostics.
// Only entries with open handles or recent closes are included.
func (g *WriteGuard) Snapshot() map[string]WriteGuardEntry {
	g.mu.RLock()
	defer g.mu.RUnlock()

	out := make(map[string]WriteGuardEntry)
	now := time.Now()
	for path, e := range g.entries {
		if e.openCount == 0 && (e.lastClose.IsZero() || now.Sub(e.lastClose) > g.quiescence*2) {
			continue
		}
		out[path] = WriteGuardEntry{
			OpenCount:     e.openCount,
			LastClose:     e.lastClose,
			QuiescentSoon: e.openCount == 0 && g.quiescence > 0 && now.Sub(e.lastClose) < g.quiescence,
		}
	}
	return out
}

// WriteGuardEntry is a snapshot of one path's write-guard state.
type WriteGuardEntry struct {
	OpenCount     int
	LastClose     time.Time
	QuiescentSoon bool // true: no open handles but quiescence window not yet elapsed
}
