package domain

import "errors"

// Sentinel errors used throughout the domain and adapters.
var (
	ErrNotExist       = errors.New("file does not exist")
	ErrAlreadyExists  = errors.New("file already exists")
	ErrTierNotFound   = errors.New("tier not found")
	ErrBackendFailure = errors.New("backend operation failed")
	ErrDigestMismatch = errors.New("digest mismatch after copy")
	ErrNoRule         = errors.New("no matching rule for path")
	ErrPinned         = errors.New("file is pinned, eviction skipped")
)
