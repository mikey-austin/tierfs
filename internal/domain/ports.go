package domain

import (
	"context"
	"io"
	"time"
)

// Backend is the storage port. Adapters implement this for file://, s3://, etc.
// All paths passed to backend methods are relative to the backend's root.
type Backend interface {
	// Scheme returns the URI scheme this backend serves, e.g. "file" or "s3".
	Scheme() string

	// URI returns a display URI for a relative path, used in logs and metadata.
	URI(relPath string) string

	// Put writes data from r into the backend at relPath.
	// size must be provided accurately; it is required for S3 Content-Length.
	Put(ctx context.Context, relPath string, r io.Reader, size int64) error

	// Get opens a reader for relPath. Returns (reader, size, error).
	// Caller must close the reader.
	Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error)

	// Stat returns metadata for relPath, or ErrNotExist if absent.
	Stat(ctx context.Context, relPath string) (*FileInfo, error)

	// Delete removes the file at relPath.
	Delete(ctx context.Context, relPath string) error

	// List enumerates all files recursively under prefix.
	// prefix="" returns all files. prefix must not have a leading slash.
	List(ctx context.Context, prefix string) ([]FileInfo, error)

	// LocalPath returns the absolute host path for relPath if this backend
	// is a local filesystem (file://). Returns ("", false) for remote backends.
	// When true, the FUSE layer can open the file directly with open(2),
	// bypassing any userspace copy.
	LocalPath(relPath string) (string, bool)
}

// Finalizer is an optional interface a Backend may implement to signal that
// it is a terminal tier — one from which files are never retrieved.
//
// If a Backend implements Finalizer and IsFinal() returns true, the evictor
// will delete the file from metadata entirely after evicting it to this
// backend, rather than updating CurrentTier to this tier's name. This
// prevents ghost records for files that have been intentionally discarded.
//
// The canonical implementation is the null:// backend.
type Finalizer interface {
	IsFinal() bool
}


// All operations must be safe for concurrent use.
type MetadataStore interface {
	// File CRUD

	UpsertFile(ctx context.Context, f File) error
	GetFile(ctx context.Context, relPath string) (*File, error)
	DeleteFile(ctx context.Context, relPath string) error

	// ListFiles returns all files whose RelPath has the given prefix.
	// prefix="" returns all files.
	ListFiles(ctx context.Context, prefix string) ([]File, error)

	// Tier presence tracking

	AddFileTier(ctx context.Context, ft FileTier) error
	GetFileTiers(ctx context.Context, relPath string) ([]FileTier, error)
	MarkTierVerified(ctx context.Context, relPath, tierName string) error
	RemoveFileTier(ctx context.Context, relPath, tierName string) error
	TierArrivedAt(ctx context.Context, relPath, tierName string) (time.Time, error)
	IsTierVerified(ctx context.Context, relPath, tierName string) (bool, error)

	// Evictor queries

	// FilesOnTier returns all files whose CurrentTier is tierName and state is synced.
	FilesOnTier(ctx context.Context, tierName string) ([]File, error)

	// FilesAwaitingReplication returns files in StateLocal with no in-flight sync.
	FilesAwaitingReplication(ctx context.Context) ([]File, error)

	// Directory listing

	// ListDir returns the immediate children of dirPath.
	// dirPath="" returns entries at the mount root.
	// Children that are intermediate directories are marked IsDir=true.
	ListDir(ctx context.Context, dirPath string) ([]FileInfo, error)

	Close() error
}
