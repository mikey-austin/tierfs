package domain

import "time"

// TierState represents where a file is in its lifecycle across tiers.
type TierState string

const (
	// StateWriting: initial write to tier0 is still in progress.
	StateWriting TierState = "writing"
	// StateLocal: write complete, digest computed, replication not yet started.
	StateLocal TierState = "local"
	// StateSyncing: at least one replication job is in flight.
	StateSyncing TierState = "syncing"
	// StateSynced: verified on at least one additional tier beyond current.
	StateSynced TierState = "synced"
)

// File is the authoritative record for a managed file.
type File struct {
	RelPath     string    // path relative to mount root, e.g. "recordings/cam1/2026-03/13/10/30.00.mp4"
	CurrentTier string    // name of the tier serving reads right now
	State       TierState
	Size        int64
	ModTime     time.Time
	Digest      string // xxhash3-128 hex, empty until StateLocal or later
}

// FileTier records a file's presence on a specific tier.
type FileTier struct {
	RelPath   string
	TierName  string
	ArrivedAt time.Time
	Verified  bool // digest confirmed after copy
}

// FileInfo is a lightweight stat result returned by backends and the meta store.
type FileInfo struct {
	RelPath string
	Name    string // basename
	Size    int64
	ModTime time.Time
	IsDir   bool
}
