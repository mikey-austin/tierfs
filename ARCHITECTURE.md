# TierFS Architecture

TierFS is a FUSE storage tiering daemon built on hexagonal (ports-and-adapters) architecture. The domain model is pure Go with no external dependencies; all I/O concerns are injected as interfaces. This document describes the complete system structure, data flows, and key design decisions.

## Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  cmd/tierfs/main.go  — composition root; wires all adapters          │
└─────────────────────────────┬────────────────────────────────────────┘
                              │ injects
       ┌──────────────────────┼──────────────────────────┐
       │                      │                          │
┌──────▼──────┐   ┌───────────▼──────────┐   ┌──────────▼──────────┐
│ fuse        │   │ app/TierService       │   │ observability/Stack  │
│ adapter     │──▶│ app/Replicator        │   │ decorators wrapping  │
│ (pathfs)    │   │ app/Evictor           │   │ all domain ports     │
└─────────────┘   └──────────┬───────────┘   └─────────────────────┘
                             │ calls domain ports
              ┌──────────────┼──────────────────┐
              │                                 │
  ┌───────────▼────────────┐      ┌─────────────▼──────────────┐
  │ domain.Backend         │      │ domain.MetadataStore        │
  │ (interface / port)     │      │ (interface / port)          │
  └─────┬──────────────────┘      └──────────┬─────────────────┘
        │                                    │
  ┌─────▼───────────────────────┐    ┌───────▼──────────────────┐
  │ adapters/storage/           │    │ adapters/meta/sqlite/     │
  │   file.Backend              │    │   sqlite.Store            │
  │   s3.Backend                │    │   (WAL, write mutex)      │
  └─────────────────────────────┘    └──────────────────────────┘
```

**The key rule:** `internal/domain/` imports only Go stdlib (`context`, `errors`, `fmt`, `io`, `time`). Nothing in domain knows about SQLite, FUSE, or S3. All dependencies point inward.

## Directory Structure

```
github.com/mikey-austin/tierfs/
│
├── cmd/tierfs/
│   └── main.go               # Composition root: parse config, build adapters, wire, mount, signal loop
│
├── internal/
│   ├── domain/
│   │   ├── errors.go         # Sentinel errors: ErrNotExist, ErrTierNotFound, ErrDigestMismatch, ErrBackendFailure
│   │   ├── file.go           # File, FileTier, FileInfo value types; TierState enum
│   │   ├── ports.go          # Backend and MetadataStore interface definitions
│   │   └── policy.go         # PolicyEngine, Rule, EvictStep; Match() evaluates rules in order
│   │
│   ├── config/
│   │   ├── config.go         # TOML parsing via BurntSushi/toml; produces Resolved struct with TiersByName map
│   │   └── observability.go  # LoggingConfig, MetricsConfig, TracingConfig; defaults() method
│   │
│   ├── adapters/
│   │   ├── storage/
│   │   │   ├── file/
│   │   │   │   └── backend.go    # file:// — atomic writes, LocalPath, empty-dir pruning
│   │   │   ├── s3/
│   │   │   │   └── backend.go    # s3:// — AWS SDK v2, multipart upload, paginated List
│   │   │   ├── sftp/
│   │   │   │   ├── backend.go    # sftp:// — pkg/sftp over x/crypto/ssh; agent/key/password auth; auto-reconnect
│   │   │   │   └── backend_test.go  # URI parsing tests; integration tests (build tag: integration)
│   │   │   ├── smb/
│   │   │   │   ├── backend.go    # smb:// — pure-Go SMB2/3, auto-reconnect, atomic rename
│   │   │   │   └── backend_test.go  # URI parsing tests; integration tests (build tag: integration)
│   │   │   ├── null/
│   │   │   │   └── backend.go    # null:// — discard terminal tier; implements domain.Finalizer
│   │   │   └── transform/
│   │   │       ├── transform.go  # Transform interface; TransformBackend; NewPipeline (ordering + elision)
│   │   │       ├── compression.go # GzipTransform
│   │   │       ├── zstd.go       # ZstdTransform — pure-Go via klauspost/compress
│   │   │       ├── encryption.go # AES256GCMTransform — chunked streaming AEAD
│   │   │       └── checksum.go   # ChecksumTransform — xxhash3-128 bit-rot detection
│   │   ├── meta/
│   │   │   └── sqlite/
│   │   │       ├── migrations.go # Schema: files + file_tiers tables; WAL pragma; indices
│   │   │       └── store.go      # Full MetadataStore implementation; write mutex
│   │   └── fuse/
│   │       ├── fs.go             # TierFS: pathfs.FileSystem; GetAttr, OpenDir, Create, Open, Unlink, Rename
│   │       ├── handle.go         # writeHandle: wraps *os.File; calls OnWriteComplete on Release
│   │       └── mount.go          # Mount(): NewPathNodeFs + NewServer; returns *fuse.Server
│   │
│   ├── app/
│   │   ├── tier_service.go   # TierService: owns backends map; WriteTarget, ReadTarget, OnWriteComplete, OnDelete, OnRename
│   │   ├── replicator.go     # Replicator: N workers, CopyJob channel, retry, streaming verify
│   │   ├── evictor.go        # Evictor: tick loop, age-based + capacity-pressure eviction, PromoteForRead
│   │   └── stager.go         # Stager: StagePath() for remote-tier read staging
│   │
│   ├── digest/
│   │   └── digest.go         # xxhash3-128 via zeebo/xxh3; ComputeFile, Compute, Verify
│   │
│   └── observability/
│       ├── wire.go           # Stack{Log, WrapBackend, WrapMetaStore, Shutdown}; Wire() entry point
│       ├── logging/
│       │   ├── logger.go     # zap + lumberjack rotation; JSON/console format; output_paths
│       │   └── shims.go      # zapcore.WriteSyncer adapters
│       ├── metrics/
│       │   ├── registry.go   # 16 Prometheus metrics across 5 subsystems
│       │   ├── server.go     # HTTP /metrics + /healthz
│       │   └── gather.go     # Gather() helper for test assertions
│       ├── tracing/
│       │   └── provider.go   # OTLP gRPC TracerProvider; noop fallback when disabled
│       └── decorators/
│           ├── backend.go    # ObservableBackend: spans + metrics on all 8 Backend methods
│           └── meta.go       # ObservableMetaStore: observe() helper for all 13 MetadataStore methods
│
├── integration/
│   └── integration_test.go   # Full-stack: real SQLite + file backends + TierService; no FUSE
│
├── Makefile
├── Dockerfile
├── go.mod
├── go.sum
└── tierfs.example.toml
```

## Domain Model

### File State Machine

```
                ┌─────────┐
   FUSE Create  │ writing │  file handle open; size=0 in metadata
  ─────────────▶│         │
                └────┬────┘
                     │ FUSE Release (writeHandle.Release)
                     │ digest computed, metadata updated
                ┌────▼────┐
                │  local  │  file on tier0; not yet replicated
                └────┬────┘
                     │ Replicator.Enqueue (if rule.Replicate)
                     │ AddFileTier(toTier, verified=false)
                ┌────▼──────┐
                │  syncing  │  copy in progress on worker
                └────┬──────┘
                     │ dst.Put succeeded + verified
                     │ MarkTierVerified; UpsertFile State=synced
                ┌────▼────┐
                │  synced  │  copy verified on ≥1 cold tier
                └────┬────┘
                     │ Evictor: arrivedAt+after < now AND verified
                     │ backend.Delete(fromTier); UpdateCurrentTier
                ┌────▼─────────┐
                │ synced       │  CurrentTier = tier1; tier0 copy gone
                │ (cold tier)  │
                └──────────────┘
```

### Port Interfaces

```go
// domain/ports.go

type Backend interface {
    Scheme() string                                                  // "file", "s3", or "null"
    URI(relPath string) string                                       // full URI for logging
    Put(ctx context.Context, relPath string, r io.Reader, size int64) error
    Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error)
    Stat(ctx context.Context, relPath string) (*FileInfo, error)
    Delete(ctx context.Context, relPath string) error
    List(ctx context.Context, prefix string) ([]FileInfo, error)
    LocalPath(relPath string) (string, bool)  // false for S3 and null; true enables zero-copy FUSE reads
}

// Finalizer is an optional interface a Backend may implement.
// If IsFinal() returns true, the evictor deletes the file from metadata
// entirely after evicting it to this backend — no ghost CurrentTier record
// is left behind. Implemented by null.Backend.
type Finalizer interface {
    IsFinal() bool
}

type MetadataStore interface {
    // File CRUD
    UpsertFile(ctx context.Context, f File) error
    GetFile(ctx context.Context, relPath string) (*File, error)
    DeleteFile(ctx context.Context, relPath string) error
    ListFiles(ctx context.Context, prefix string) ([]File, error)

    // Per-tier tracking
    AddFileTier(ctx context.Context, ft FileTier) error
    GetFileTiers(ctx context.Context, relPath string) ([]FileTier, error)
    MarkTierVerified(ctx context.Context, relPath, tierName string) error
    RemoveFileTier(ctx context.Context, relPath, tierName string) error
    TierArrivedAt(ctx context.Context, relPath, tierName string) (time.Time, error)
    IsTierVerified(ctx context.Context, relPath, tierName string) (bool, error)

    // Evictor queries
    FilesOnTier(ctx context.Context, tierName string) ([]File, error)        // State=synced only
    FilesAwaitingReplication(ctx context.Context) ([]File, error)            // State=local

    // FUSE directory listing
    ListDir(ctx context.Context, dirPath string) ([]FileInfo, error)         // synthesised from rel_path
    Close() error
}
```

## Adapters

### `adapters/storage/transform`

The transform package provides a `domain.Backend` decorator that applies a pipeline of reversible byte-level transformations. It is the only place in the codebase where compression and encryption logic lives — adapters and the application layer are unaware of it.

**`Transform` interface:**

```go
type Transform interface {
    Name() string
    Writer(dst io.Writer) (io.WriteCloser, error)  // forward: plaintext → stored
    Reader(src io.Reader) (io.ReadCloser, error)   // inverse: stored → plaintext
}
```

**Pipeline execution:**

Given `pipeline = [compress, encrypt]`:
- **Write** (`Put`): `r → compress.Writer → encrypt.Writer → tempfile → inner.Put(size)`
- **Read** (`Get`): `inner.Get() → encrypt.Reader → compress.Reader → caller`

The pipeline is reversed automatically for reads — the `TransformBackend` builds the read chain in reverse index order.

**Temp file buffering for `Put`:** After transformation, the output size is unknown (compression ratio is data-dependent; encryption adds per-chunk overhead). `Put` writes the transformed output to `os.CreateTemp("", "tierfs-transform-*")`, stats the temp file for the actual byte count, then passes the open temp file to `inner.Put(size)`. The temp file is deleted after `inner.Put` returns. This approach avoids unbounded memory usage for large files and works correctly for S3 backends that require `Content-Length`.

**`LocalPath` always returns `("", false)`:** Even when the inner backend is a `file.Backend` with a local path, the TransformBackend disables the zero-copy FUSE loopback path. The stored file is compressed/encrypted; serving it directly to the FUSE caller would expose raw ciphertext.

**`IsFinal` delegation:** If the inner backend implements `domain.Finalizer`, `TransformBackend.IsFinal()` delegates to it. A `null://` backend wrapped with encryption is still a final tier.

**Ordering enforcement in `NewPipeline`:** Regardless of how the user orders `Compression` and `Encryption` in config, `NewPipeline` always builds `[compress, encrypt]`. Encrypting before compressing would eliminate any compression benefit (encrypted data is statistically random and incompressible); this ordering is enforced structurally rather than documented.

**AES-256-GCM chunked streaming format:**

```
[8 bytes]  magic = "TFSAEAD1"
[per-chunk, repeating]
  [12 bytes] nonce (crypto/rand, per-chunk)
  [4 bytes]  uint32 LE: ciphertext length (plaintext + 16-byte GCM tag)
  [N bytes]  ciphertext
[terminal]
  [12 bytes] all-zero nonce
  [4 bytes]  uint32 LE = 0
```

Each chunk's AAD is its 8-byte little-endian chunk index, preventing reordering attacks. Chunk size is 64 KiB plaintext → ≤64 KiB + 28 bytes ciphertext. Authentication failure on any chunk returns `errAuthFailed` immediately without consuming further data.

### `adapters/storage/sftp`

The SFTP backend uses `github.com/pkg/sftp` over `golang.org/x/crypto/ssh`. No CGO, no native binaries, no kernel FUSE. The same `withReconnect` pattern as the SMB backend handles dropped SSH connections transparently.

**Authentication chain** (tried in order):
1. SSH agent via `SSH_AUTH_SOCK` unix socket — zero-config for interactive sessions and systemd user services.
2. Private key file — `TIERFS_SFTP_KEY_PATH` env → `sftp_key_path` config → `~/.ssh/id_ed25519` → `~/.ssh/id_ecdsa` → `~/.ssh/id_rsa`.
3. Password — `TIERFS_SFTP_PASS` env → `sftp_password` config.

`authMethods()` constructs the slice of `ssh.AuthMethod` at `connect()` time. If the slice is empty, `New()` returns an error immediately rather than connecting and failing on the first operation.

**Host key verification**: `hostKeyCallback()` returns `ssh.FixedHostKey(pk)` when `sftp_host_key` is set. Without it, `ssh.InsecureIgnoreHostKey()` is used with a `Warn`-level log entry. Production deployments must set `sftp_host_key` (obtained via `ssh-keyscan`).

**Atomic writes**: identical pattern to SMB — write to `<path>.tierfs-tmp-<8 hex>`, then `c.Rename()`. SFTP rename is atomic on POSIX servers (Linux, BSD, macOS). For Windows SFTP servers where rename-over-existing is not atomic, the backend falls back to `c.Remove(dst)` followed by `c.Rename(tmp, dst)`.

**`remotePath`** joins `basePath + "/" + prefix + "/" + relPath` using `path.Join`. The URI parser splits the server path as: first component → `basePath`, remainder → `prefix`, so `sftp://host/mnt/storage/cctv` → `basePath=/mnt`, `prefix=storage/cctv`.

**`mkdirAll`** walks from root, creating each path component with `c.Mkdir()` and ignoring `SSH_FX_FILE_ALREADY_EXISTS` (code 11). Safe for concurrent `Put` calls creating sibling directories.

**`pruneEmptyDirs`** removes empty directories up to the `basePath` root using `c.RemoveDirectory()`. Stops on any error (non-empty, permission denied) without propagating.

**`isNotExist`** — checks `errors.As(err, &sftp.StatusError{})` with `Code == 2` (`SSH_FX_NO_SUCH_FILE`) in addition to the standard `fs.ErrNotExist` and string heuristics. This is the correct way to distinguish SFTP not-found from other errors without relying on error string matching.

### `adapters/storage/smb`

The SMB backend uses `github.com/hirochachacha/go-smb2` — pure Go, no CGO, no kernel mount. Each `Backend` owns a single authenticated SMB2 session and share handle.

**Connection model:**

```
Backend
  ├── mu sync.RWMutex
  ├── conn net.Conn          (TCP to host:445)
  ├── session *smb2.Session  (authenticated SMB2 session)
  └── share *smb2.Share      (mounted share handle)
```

`withReconnect(ctx, fn)` is the core resilience primitive:
1. Acquire read lock, call `fn(share)`.
2. If `fn` returns a transport error (`io.EOF`, `net.Error`, `STATUS_CONNECTION_DISCONNECTED` etc.) — upgrade to write lock, call `disconnect()`, wait 500ms, call `connect()`, retry `fn` once.
3. Any other error (file-not-found, permission-denied) passes through immediately without reconnect.

This gives transparent recovery from NAS reboots and switch failovers without application-level retry logic.

**Atomic writes:** `Put` writes to `<path>.tierfs-tmp-<8 random hex chars>` then calls `share.Rename()`. SMB2 `SetInformationFile(FileRenameInformation)` is atomic within the same share. The temp file is removed on any error before rename.

**`mkdirAll`** walks the path from root creating each component, treating `STATUS_OBJECT_NAME_COLLISION` (already exists) as success. This is safe for concurrent Put operations to different paths in the same directory.

**`pruneEmptyDirs`** walks upward after `Delete` or `Rename`, removing empty directories until it hits the share prefix or a non-empty directory. Keeps the share clean without extra tooling.

**URI parsing:** `smb://[user[:pass]@]host[:port]/share[/prefix]` — the `url.Parse` standard library handles IPv6 brackets, percent-encoding, and optional fields. Port defaults to 445. Credentials in the URI are the lowest priority; environment variables and config fields override them.

**Credential resolution order:** env (`TIERFS_SMB_USER`/`TIERFS_SMB_PASS`) → config fields (`smb_username`/`smb_password`) → URI userinfo. This order prevents credentials from leaking into config files or URIs in most deployments.

**`Rename` optional interface:** The SMB backend implements `Rename(ctx, old, new string) error`, the optional extension checked by `TierService.OnRename()`. SMB's `SetInformationFile` rename is a single round-trip and cheaper than copy+delete.

### `adapters/storage/null`

The null backend is a stateless discard terminal. It has no fields and is safe for concurrent use with no locking.

- **`Put()`**: drains the entire reader to `io.Discard` before returning. The reader is always fully consumed so that callers using `io.TeeReader` for digest verification see a complete byte stream before computing the hash. If `Put` returned immediately, the `TeeReader` would not flush into the hash buffer.
- **`Get()` / `Stat()`**: always return `domain.ErrNotExist`. Files evicted to this tier cannot be retrieved.
- **`Delete()`**: no-op; always returns nil.
- **`IsFinal() bool`**: returns `true`, implementing `domain.Finalizer`. The evictor checks for this interface after any eviction and, if found, calls `MetadataStore.DeleteFile()` to purge the record entirely rather than updating `CurrentTier`.

The correct alternative to `file:///dev/null` — that path is a character device, not a directory. The file backend calls `os.MkdirAll(root)` and would fail with `ENOTDIR`.

### `adapters/storage/file`

The file backend stores files in a root directory tree mirroring the virtual relative paths. Key design decisions:

- **Atomic writes**: `Put()` writes to a temp file in the same directory (ensuring same filesystem) then calls `os.Rename()`. This guarantees readers never see a partial write.
- **LocalPath**: Returns the real absolute path on the host. The FUSE layer uses this to open a direct `*os.File` fd, enabling the kernel to serve reads via the page cache without copying through tierfs.
- **Empty-dir pruning**: `Delete()` walks up the directory tree and removes empty parent directories, preventing unbounded accumulation of empty dirs as files age off.
- **Rename support**: Implements optional `Rename(ctx, old, new string) error` for `OnRename()` to call natively rather than copy+delete.

### `adapters/storage/s3`

Uses AWS SDK v2. Compatible with any S3-compatible endpoint (MinIO, Ceph RGW, Backblaze B2) via `endpoint` + `path_style=true` config.

- **Multipart upload**: Files > 16 MiB use `CreateMultipartUpload` with 4 concurrent part uploads. Aborts the multipart upload on any part failure.
- **Paginated listing**: `List()` uses `ListObjectsV2Paginator` to handle prefixes with >1000 objects.
- **Stat**: Implemented as `HeadObject` — avoids downloading data just for metadata.
- **LocalPath**: Always returns `("", false)`. Remote-tier reads are staged to `stageDir` by the FUSE layer.

### `adapters/meta/sqlite`

SQLite in WAL mode is the metadata store. Schema:

```sql
CREATE TABLE files (
    rel_path     TEXT PRIMARY KEY,
    current_tier TEXT NOT NULL,
    state        TEXT NOT NULL,  -- writing|local|syncing|synced
    size         INTEGER,
    mod_time     INTEGER,         -- Unix nanoseconds
    digest       TEXT
);

CREATE TABLE file_tiers (
    rel_path   TEXT NOT NULL,
    tier_name  TEXT NOT NULL,
    arrived_at INTEGER NOT NULL,  -- Unix nanoseconds
    verified   INTEGER DEFAULT 0,
    PRIMARY KEY (rel_path, tier_name)
);

CREATE INDEX idx_file_tiers_tier ON file_tiers(tier_name);
CREATE INDEX idx_files_state ON files(state);
CREATE INDEX idx_files_tier ON files(current_tier);
```

Pragmas applied at open: `PRAGMA journal_mode=WAL`, `PRAGMA synchronous=NORMAL`, `PRAGMA cache_size=-32768` (32 MiB), `PRAGMA foreign_keys=ON`.

A `sync.Mutex` serialises all writes. Reads run concurrently via separate connections (SQLite WAL allows one writer + N readers simultaneously).

**ListDir** synthesises virtual directory entries by selecting `DISTINCT` path segments from `rel_path` at the requested depth. A query for `"recordings"` returns `cam1`, `cam2`, etc. by splitting `rel_path` on `/` and grouping by position.

### `adapters/fuse`

Implements `pathfs.FileSystem` from `hanwen/go-fuse/v2`. The pathfs API provides a higher-level interface than rawfs — methods receive cleaned relative paths rather than raw inode numbers, which maps naturally to TierFS's relPath-keyed metadata.

**Inode assignment**: A `sync.RWMutex`-protected map from `relPath → uint64` with an `atomic.Uint64` counter. Inode 1 is reserved for the mount root; all other inodes are assigned on first access and never reassigned. This is safe for our use case because FUSE inode remapping only matters for `rename()`, which TierFS handles via `OnRename`.

**`writeHandle`**: Wraps an `*os.File` with a `sync.Mutex`. `Write()` and `Read()` call `WriteAt`/`ReadAt` so concurrent pwrite/pread from a single file handle are safe. `Release()` stats the file, closes it, then calls `TierService.OnWriteComplete()` with the final size and mtime. For remote-tier backends (S3), `Release()` pushes the staged file to the backend in a goroutine so the FUSE release syscall returns immediately.

**Staging**: Remote-tier reads call `backend.Get()` and write to a temp file in `stageDir` via atomic rename before opening for the caller. Staged files are not evicted automatically — a separate janitor (planned) sweeps files older than a configurable TTL.

## Application Services

### TierService

TierService is the central application service. It owns the `backends map[string]domain.Backend` and implements both `TierLookup` and `TierCapacity` interfaces used by Replicator and Evictor respectively.

```
TierService
  ├── WriteTarget(relPath) → (Backend, tierName)
  │     PolicyEngine.Match(relPath) → rule
  │     if rule.PinTier: return that tier
  │     else: return HottestTier (priority=0)
  │
  ├── ReadTarget(ctx, relPath) → (Backend, localPath, File)
  │     MetadataStore.GetFile → currentTier
  │     BackendFor(currentTier) → backend
  │     backend.LocalPath(relPath) → localPath (empty if S3)
  │     if rule.PromoteOnRead: Evictor.PromoteForRead(f, targetTier)
  │
  ├── OnWriteComplete(ctx, relPath, tier, size, modTime)
  │     digest.ComputeFile(localPath)
  │     MetadataStore.UpsertFile(StateLocal)
  │     MetadataStore.AddFileTier(verified=true)
  │     if rule.Replicate: Replicator.Enqueue(CopyJob)
  │
  ├── OnDelete(ctx, relPath)
  │     for each FileTier: backend.Delete
  │     MetadataStore.DeleteFile
  │
  └── OnRename(ctx, old, new)
        for each FileTier: backend.Rename (if supported)
        MetadataStore: DeleteFile(old) + UpsertFile(new) + AddFileTiers(new)
```

### Replicator

```
CopyJob{RelPath, FromTier, ToTier}
      │
      ▼ channel (buffer: 4096)
┌─────────────────────────────────┐
│  Worker goroutine × N           │
│                                 │
│  1. GetFileMeta → check digest  │
│  2. src.Get(relPath) → rc       │
│  3. if verify=digest:           │
│       hashBuf := &bytes.Buffer  │
│       streamBody = TeeReader(rc, hashBuf) │
│  4. dst.Put(relPath, streamBody, size) │
│  5. rc.Close()                  │
│  6. Verify:                     │
│     digest: dst.LocalPath?      │
│       → digest.Verify(path)     │
│       else → digest.Compute(hashBuf) │
│     size: dst.Stat → compare    │
│  7. MarkTierVerified            │
│  8. UpsertFile(StateSynced)     │
│                                 │
│  On error: retry after interval │
│  After MaxRetries: log+discard  │
└─────────────────────────────────┘
```

Metrics exported: `copied int64`, `failed int64`, `depth int64` (via `Metrics()` method used by Prometheus gauges/counters).

### Evictor

Two eviction modes run on each tick:

**Age-based (schedule eviction)**:
```
for each file WHERE state=synced AND current_tier != destinationTier:
    rule := PolicyEngine.Match(file.RelPath)
    if rule.PinTier == file.CurrentTier: skip
    for each EvictStep in rule.EvictSchedule:
        arrivedAt := TierArrivedAt(relPath, currentTier)
        if arrivedAt + step.After < now:
            if IsTierVerified(relPath, step.ToTier):
                backend.Delete(currentTier, relPath)
                UpdateCurrentTier(step.ToTier)
                eviction_events_total.Inc()
                break
```

**Capacity pressure**:
```
for each tier where capacity != unlimited:
    used := UsedBytes(tier)
    cap  := CapacityBytes(tier)
    if used/cap > CapacityThreshold:
        files := FilesOnTier(tier) sorted by ArrivedAt ASC
        for _, f := range files:
            if used/cap <= CapacityHeadroom: break
            if IsTierVerified(f.RelPath, nextTier):
                evict f from currentTier
                used -= f.Size
```

`PromoteForRead(f, targetTier)` enqueues a `CopyJob{FromTier: f.CurrentTier, ToTier: targetTier}` and, on completion, the Replicator calls `OnWriteComplete` logic equivalent to update `CurrentTier`.

## Observability Layer

All domain ports are wrapped at the composition root in `wire.go`. The application layer and FUSE layer never know whether they are talking to a raw adapter or a decorated one — they call the same interface.

```
       domain.Backend (interface)
             │
    ┌────────▼──────────────────┐
    │   ObservableBackend       │
    │  ┌──────────────────────┐ │
    │  │  OpenTelemetry span  │ │
    │  │  start span          │ │
    │  │  defer span.End()    │ │
    │  │                      │ │
    │  │  Prometheus counter  │ │
    │  │  backend_ops_total   │ │
    │  │  .WithLabelValues()  │ │
    │  │  .Inc()              │ │
    │  │                      │ │
    │  │  zap.Logger          │ │
    │  │  log.Debug(op, ...)  │ │
    │  └──────────────────────┘ │
    │       │ delegate          │
    │  ┌────▼──────────────────┐│
    │  │ concrete adapter      ││
    │  │ file.Backend / s3     ││
    │  └───────────────────────┘│
    └──────────────────────────┘
```

### Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `tierfs_backend_operations_total` | Counter | `backend`, `op`, `outcome` | All backend calls |
| `tierfs_backend_operation_duration_seconds` | Histogram | `backend`, `op` | Per-call latency |
| `tierfs_backend_bytes_read_total` | Counter | `backend` | Bytes returned by Get() |
| `tierfs_backend_bytes_written_total` | Counter | `backend` | Bytes sent to Put() |
| `tierfs_meta_operations_total` | Counter | `op`, `outcome` | All metadata calls |
| `tierfs_meta_operation_duration_seconds` | Histogram | `op` | Metadata latency |
| `tierfs_replication_queue_depth` | Gauge | — | Pending CopyJobs |
| `tierfs_replication_jobs_total` | Counter | `from_tier`, `to_tier`, `outcome` | Copy completions |
| `tierfs_replication_job_duration_seconds` | Histogram | `from_tier`, `to_tier` | End-to-end copy time |
| `tierfs_replication_bytes_transferred_total` | Counter | `from_tier`, `to_tier` | Bytes copied |
| `tierfs_eviction_events_total` | Counter | `from_tier` | Files evicted |
| `tierfs_fuse_operations_total` | Counter | `op`, `outcome` | FUSE syscall counts |
| `tierfs_fuse_operation_duration_seconds` | Histogram | `op` | FUSE syscall latency |
| `tierfs_fuse_staged_bytes_total` | Counter | — | Bytes fetched for cold reads |
| `tierfs_tier_file_count` | Gauge | `tier` | Files currently on tier |
| `tierfs_tier_bytes_used` | Gauge | `tier` | Bytes summed from metadata |

## Data Flow: Write Path

1. Application calls `open(path, O_CREAT|O_WRONLY)` on the FUSE mount.
2. Kernel delivers `CREATE` to TierFS; `TierFS.Create()` is called.
3. `PolicyEngine.Match(relPath)` identifies the rule; `WriteTarget()` returns tier0 backend (or PinTier if set).
4. For `file.Backend`: `CreateFile(relPath, mode)` opens a real `*os.File` in the tier0 root directory; a `writeHandle` wrapping it is returned to FUSE.
5. Application `write()` calls arrive as `writeHandle.Write()` → `f.WriteAt()` — kernel page cache handles buffering.
6. Application calls `close(fd)`; kernel delivers `RELEASE` to TierFS.
7. `writeHandle.Release()` stats the file for size+mtime, closes the fd.
8. `TierService.OnWriteComplete()`: `digest.ComputeFile(localPath)` reads the file to compute xxhash3-128.
9. `MetadataStore.UpsertFile()` records `State=StateLocal`, `Digest`, `Size`, `ModTime`.
10. `MetadataStore.AddFileTier()` records `(relPath, tier0, now, verified=true)`.
11. If `rule.Replicate`: `Replicator.Enqueue(CopyJob{relPath, tier0, tier1})`.
12. A replication worker picks up the job; `src.Get()` → `io.TeeReader` → `dst.Put()` → verify → `MarkTierVerified`.
13. `UpsertFile(State=StateSynced)`.

## Data Flow: Read Path

**Hot path (file backend, local tier)**:
1. Application calls `read(fd, ...)` via FUSE.
2. `TierFS.Open()` → `ReadTarget()` → `backend.LocalPath(relPath)` returns real path.
3. `os.Open(localPath)` → `nodefs.NewLoopbackFile(f)` returned to FUSE.
4. Kernel serves subsequent reads from the page cache without going through tierfs at all.

**Cold path (file on remote tier)**:
1. `TierFS.Open()` → `ReadTarget()` → `backend.LocalPath()` returns `("", false)`.
2. `TierFS.stage()` calls `backend.Get()` → streams to `stageDir/flat-name`.
3. Atomic rename ensures no partial staged file is ever served.
4. `os.Open(stagePath)` returned as loopback file.
5. `tierfs_fuse_staged_bytes_total` counter incremented.
6. If `rule.PromoteOnRead`: `Evictor.PromoteForRead()` enqueues a copy back to the target hot tier.

## Data Flow: Eviction

```
Evictor.tick()
    │
    ├── runScheduledEvictions()
    │     MetadataStore.FilesOnTier(tier0)          // all synced files on hot tier
    │     for each file:
    │       rule = PolicyEngine.Match(relPath)
    │       if rule.PinTier == "tier0": continue
    │       for step in rule.EvictSchedule:
    │         arrivedAt = TierArrivedAt(relPath, "tier0")
    │         if now > arrivedAt + step.After:
    │           if IsTierVerified(relPath, step.ToTier):
    │             tier0.Delete(relPath)              // removes physical file
    │             RemoveFileTier(relPath, "tier0")
    │             // Finalizer check:
    │             if dstBackend implements Finalizer && IsFinal():
    │               meta.DeleteFile(relPath)         // purge entirely — no ghost record
    │             else:
    │               UpsertFile(CurrentTier=step.ToTier)
    │             eviction_events_total{from_tier="tier0"}.Inc()
    │             break  // only one step per tick
    │
    └── runCapacityEvictions()
          if UsedBytes("tier0") / CapacityBytes("tier0") > Threshold:
            files = FilesOnTier("tier0") ORDER BY arrivedAt ASC
            for _, f := range files:
              // same evict logic as above, using first available verified cold tier
              if ratio <= Headroom: break
```

## Concurrency Model

| Resource | Mechanism | Rationale |
|---|---|---|
| SQLite writes | `sync.Mutex` | SQLite WAL allows one writer; mutex is simpler than connection pool |
| SQLite reads | Concurrent (separate connections) | WAL mode; reads don't block each other |
| Inode map | `sync.RWMutex` | Many FUSE ops read the map; writes (new files) are rare |
| Replication queue | Buffered channel (4096) | Decouples write path from copy latency; backpressure is visible as queue depth |
| writeHandle state | `sync.Mutex` per handle | Multiple goroutines can call Write/Read/Fsync concurrently (pwrite semantics) |
| Backend.Put (S3) | Per-request, no shared state | AWS SDK v2 is goroutine-safe |
| Evictor loop | Single goroutine | Sequential evaluation simplifies state; tick period throttles throughput |
| Stage directory | Temp file + atomic rename | Concurrent cold reads for the same file race to rename; last writer wins but content is identical |

**Why serialise SQLite writes?** go-sqlite3 supports shared-cache mode, but shared-cache + WAL has known edge cases. A single write mutex with ~2 µs overhead per operation is negligible given that writes are always to the `files` and `file_tiers` tables (not bulk data), and the write mutex is only held for the duration of a single `INSERT OR REPLACE`.

## Design Decisions

### 1. xxhash3-128 for digest verification (not SHA-256)

SHA-256 is ~500 MB/s in software; xxhash3-128 exceeds 30 GB/s with SIMD. For a tiering daemon verifying files that may be tens of gigabytes (Frigate segments), SHA-256 would add 20+ seconds per file. xxhash3-128 adds <1 second. The collision probability of 128-bit non-cryptographic hash is negligible for integrity verification (distinct from security hashing). We use `zeebo/xxh3` which generates SIMD code for amd64/arm64 via `//go:generate`.

### 2. SQLite, not etcd or Postgres

The metadata store is local to the node and needs to survive restart without a network service. SQLite WAL mode gives us concurrent reads, atomic writes, and crash recovery with no operational overhead. The database is typically <100 MB for millions of files. etcd adds distributed consensus complexity that is unnecessary for a single-node daemon; Postgres requires a running server.

### 3. pathfs, not the lower-level rawfs/nodefs

`go-fuse/v2/fuse/pathfs` provides a stable path-based API that maps directly to TierFS's relPath model. The rawfs API requires managing inode numbers and directory children in a tree structure, which would duplicate work that pathfs handles. The trade-off is that pathfs does one extra string allocation per operation for path cleaning — acceptable at our scale.

### 4. Decorator pattern for observability

Embedding telemetry in adapters couples them to specific observability libraries and makes unit testing harder. The decorator pattern keeps adapters pure (only I/O logic), keeps domain clean (only business logic), and lets the observability stack be replaced or disabled entirely by changing `wire.go`. The `ObservableBackend` wrapping `instrumentedReadCloser` for lazy byte counting is the most non-trivial piece — it defers the bytes-read counter increment until the caller actually consumes the stream, not just when `Get()` returns.

### 5. `io.TeeReader`, not `io.Pipe`, for streaming digest

The earlier design used `io.Pipe()` with a goroutine writing from source to pipe and the put body reading from the pipe reader while a teeHasher accumulated bytes. This introduced a goroutine whose lifetime was hard to reason about (write goroutine could outlive the Put call; errors were difficult to propagate). `io.TeeReader(rc, hashBuf)` is simpler: `dst.Put()` reads from `streamBody`, which reads from `rc` and tees every byte into `hashBuf`. No goroutine. The hash is available synchronously after `Put` returns.

### 6. Atomic rename for file backend writes

`O_CREATE | O_WRONLY` directly on the destination path is unsafe: a reader or a crash mid-write sees a partial file. Writing to `destDir/.tierfs-tmp-*` (same filesystem, so rename is atomic on Linux) and renaming on close ensures readers always see a complete file. The temp file prefix starts with `.` so it is not returned by `List()`.
