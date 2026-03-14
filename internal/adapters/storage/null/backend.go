// Package null implements a write-only discard backend for use as a terminal
// tier in a tiering policy. It satisfies domain.Backend by accepting Put()
// calls (draining the reader to io.Discard) and returning ErrNotExist for
// all read operations.
//
// The backend also implements domain.Finalizer: IsFinal() returns true,
// which causes the evictor to delete the file from metadata entirely after
// evicting it here — rather than updating CurrentTier to a tier from which
// reads would always fail.
//
// Typical use: last tier in a time-series retention policy where files must
// be permanently deleted after a configured age rather than kept on any
// storage medium.
//
//	[[backend]]
//	name = "void"
//	uri  = "null://"
//
//	[[tier]]
//	name     = "tier3"
//	backend  = "void"
//	priority = 3
//	capacity = "unlimited"
package null

import (
	"context"
	"io"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// Backend is a stateless discard backend. All Put() bytes are drained to
// io.Discard; all other operations report the file as non-existent.
// It is safe for concurrent use with no internal state.
type Backend struct{}

// New returns a Backend. No configuration is required beyond the URI.
func New() *Backend {
	return &Backend{}
}

// Scheme returns "null".
func (b *Backend) Scheme() string { return "null" }

// URI returns "null://<relPath>" for logging purposes.
func (b *Backend) URI(relPath string) string { return "null://" + relPath }

// LocalPath always returns ("", false) — the null backend has no local filesystem presence.
func (b *Backend) LocalPath(_ string) (string, bool) { return "", false }

// IsFinal implements domain.Finalizer. Returning true causes the evictor to
// purge the file from metadata entirely after evicting it to this tier,
// rather than updating CurrentTier to "null" and leaving a ghost record.
func (b *Backend) IsFinal() bool { return true }

// Put drains r to io.Discard and reports success. The reader is always fully
// consumed so that callers using io.TeeReader for digest verification see a
// complete stream before computing the hash.
func (b *Backend) Put(_ context.Context, _ string, r io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}

// Get always returns ErrNotExist — discarded files cannot be retrieved.
func (b *Backend) Get(_ context.Context, _ string) (io.ReadCloser, int64, error) {
	return nil, 0, domain.ErrNotExist
}

// Stat always returns ErrNotExist.
func (b *Backend) Stat(_ context.Context, _ string) (*domain.FileInfo, error) {
	return nil, domain.ErrNotExist
}

// Delete is a no-op and always succeeds. The file was never stored.
func (b *Backend) Delete(_ context.Context, _ string) error { return nil }

// List always returns an empty slice. No files are retained on this backend.
func (b *Backend) List(_ context.Context, _ string) ([]domain.FileInfo, error) {
	return nil, nil
}
