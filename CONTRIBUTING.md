# Contributing to TierFS

TierFS is a FUSE storage tiering daemon written in Go. Contributions of all kinds are welcome — bug reports, documentation improvements, new storage backend adapters, performance work, and additional test coverage. Please read this guide before opening a pull request.

---

## Project Structure

```
github.com/mikey-austin/tierfs/
│
├── cmd/tierfs/
│   └── main.go               # Composition root — parses config, builds adapters, wires, mounts FUSE, handles signals
│
├── internal/
│   ├── domain/               # Pure domain — no external dependencies
│   │   ├── errors.go         # Sentinel errors (ErrNotExist, ErrTierNotFound, ErrDigestMismatch, ErrBackendFailure)
│   │   ├── file.go           # File, FileTier, FileInfo value types; TierState enum
│   │   ├── ports.go          # Backend and MetadataStore interface definitions
│   │   └── policy.go         # PolicyEngine — evaluates []Rule in order; first match wins
│   │
│   ├── config/               # TOML config parsing
│   │   ├── config.go         # BurntSushi/toml; Resolved struct with TiersByName, BackendsByName maps; validation
│   │   └── observability.go  # LoggingConfig, MetricsConfig, TracingConfig sub-structs
│   │
│   ├── adapters/
│   │   ├── storage/
│   │   │   ├── file/         # file:// backend — atomic writes, LocalPath, empty-dir pruning
│   │   │   └── s3/           # s3:// backend — AWS SDK v2, multipart upload, paginated List
│   │   ├── meta/
│   │   │   └── sqlite/       # WAL SQLite MetadataStore; sync.Mutex serialises writes
│   │   └── fuse/             # FUSE adapter — pathfs.FileSystem, writeHandle, Mount()
│   │
│   ├── app/                  # Application services — coordinate domain ports
│   │   ├── tier_service.go   # TierService — central coordinator; WriteTarget, ReadTarget, OnWrite*, OnDelete, OnRename
│   │   ├── replicator.go     # Async copy workers — CopyJob channel, retry, streaming verify
│   │   ├── evictor.go        # Tick loop — age-based + capacity-pressure eviction, PromoteForRead
│   │   └── stager.go         # Stager — StagePath() helper for remote-tier read staging
│   │
│   ├── digest/               # xxhash3-128 via zeebo/xxh3 — ComputeFile, Compute, Verify
│   │
│   └── observability/        # Metrics, logging, tracing — decorator pattern
│       ├── wire.go           # Stack composition root; Wire() entry point; WrapBackend/WrapMetaStore
│       ├── logging/          # zap + lumberjack rotation
│       ├── metrics/          # 16 Prometheus metrics; HTTP /metrics + /healthz
│       ├── tracing/          # OTLP gRPC TracerProvider; noop fallback
│       └── decorators/       # ObservableBackend, ObservableMetaStore — wrap all domain ports
│
├── integration/
│   └── integration_test.go   # Full-stack tests: real SQLite + file backends + TierService; no FUSE
│
├── docs/
│   ├── CONFIGURATION.md      # Complete config field reference with examples
│   ├── OPERATIONS.md         # Deployment, metrics, alerting, troubleshooting
│   └── tierfs.1              # Man page
│
├── Makefile
├── Dockerfile
├── go.mod / go.sum
└── tierfs.example.toml
```

---

## Prerequisites

**Required:**

| Tool | Version | Purpose |
|---|---|---|
| Go | 1.22+ | Build and test |
| gcc / CGO | any | Required by `mattn/go-sqlite3` |
| libfuse3-dev | any | FUSE header files for `hanwen/go-fuse/v2` |
| make | any | Build targets |
| golangci-lint | 1.57+ | Static analysis |

**Optional:**

| Tool | Purpose |
|---|---|
| Docker + MinIO | S3 integration tests |
| fusermount3 | FUSE integration tests (Linux only) |

> **macOS note:** The FUSE adapter (`adapters/fuse/`) requires Linux. All other packages — including the full integration test suite — work on macOS. FUSE-specific tests are gated with `//go:build !nofuse` and skipped automatically on CI.

```bash
# Ubuntu/Debian
sudo apt-get install -y gcc libfuse3-dev

# Install golangci-lint
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
```

---

## Getting Started

```bash
# 1. Fork on GitHub, then clone
git clone https://github.com/YOUR_USERNAME/tierfs.git
cd tierfs

# 2. Download dependencies
go mod download

# 3. Build
make build
# → bin/tierfs

# 4. Verify
./bin/tierfs -help

# 5. Run tests
make test-unit
```

---

## Running Tests

### Unit Tests

```bash
make test-unit
# Equivalent: go test ./internal/... -count=1 -race -timeout 60s -coverprofile=coverage.out ./...
```

The `-race` flag is mandatory. All new code must pass with `-race`. Do not add `//nolint:race` comments without a very strong justification.

### Integration Tests

```bash
make test-integration
# Equivalent: go test ./integration/... -count=1 -race -timeout 120s -v
```

Integration tests exercise the complete application stack: real SQLite, real `file.Backend` instances in temp directories, `TierService`, `Replicator`, and `Evictor`. No FUSE. Scenarios tested include write+read, replication, eviction, delete propagation, rename, and directory listing.

### S3 Integration Tests

```bash
# Start MinIO
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create test bucket
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/test-bucket

# Run S3 tests
TIERFS_S3_TEST_ENDPOINT=http://localhost:9000 \
AWS_ACCESS_KEY_ID=minioadmin \
AWS_SECRET_ACCESS_KEY=minioadmin \
go test ./internal/adapters/storage/s3/... -v -run TestIntegration
```

### FUSE Tests

FUSE tests require Linux with `fusermount3` and either root or `user_allow_other` in `/etc/fuse.conf`.

```bash
# Skip FUSE tests (default on macOS and CI)
go test ./... -tags nofuse

# Run FUSE tests (Linux, fusermount3 available)
go test ./internal/adapters/fuse/... -v
```

### Coverage

```bash
make test-unit
go tool cover -html=coverage.out   # opens browser
go tool cover -func=coverage.out   # function-level summary
```

---

## Adding a New Storage Backend

New backends must implement `domain.Backend` (8 methods). Follow these steps:

**1. Create the package:**

```
internal/adapters/storage/yourscheme/
├── backend.go        # Backend struct + all 8 interface methods
└── backend_test.go   # Unit + integration tests
```

**2. Implement `domain.Backend`:**

```go
package yourscheme

import (
    "context"
    "io"
    "github.com/mikey-austin/tierfs/internal/domain"
)

type Backend struct { /* your fields */ }

func New(cfg Config) (*Backend, error) { ... }

func (b *Backend) Scheme() string { return "yourscheme" }
func (b *Backend) URI(relPath string) string { ... }
func (b *Backend) Put(ctx context.Context, relPath string, r io.Reader, size int64) error { ... }
func (b *Backend) Get(ctx context.Context, relPath string) (io.ReadCloser, int64, error) { ... }
func (b *Backend) Stat(ctx context.Context, relPath string) (*domain.FileInfo, error) { ... }
func (b *Backend) Delete(ctx context.Context, relPath string) error { ... }
func (b *Backend) List(ctx context.Context, prefix string) ([]domain.FileInfo, error) { ... }
func (b *Backend) LocalPath(relPath string) (string, bool) { return "", false } // false if remote
```

**3. Implement optional `Rename` for native rename support:**

```go
func (b *Backend) Rename(ctx context.Context, oldRel, newRel string) error { ... }
```

TierFS checks for this via a type assertion in `TierService.OnRename()`.

**4. Add a `ParseURI` helper:**

```go
func ParseURI(uri string) (SomeConfig, error) { ... }
```

**5. Register in `cmd/tierfs/main.go`:**

```go
case "yourscheme":
    parsed, err := yourscheme.ParseURI(cfg.URI)
    if err != nil { return nil, err }
    return yourscheme.New(parsed)
```

**6. Write tests** — see `internal/adapters/storage/file/backend_test.go` for patterns. Cover: Put+Get roundtrip, Stat, Delete, List, LocalPath, error cases (not found, permission denied), and concurrent access.

**7. Document** the new URI scheme and config fields in `docs/CONFIGURATION.md` under the `[[backend]]` section, including the S3 compatibility matrix if applicable.

---

## Adding a New Prometheus Metric

**1. Declare in `internal/observability/metrics/registry.go`:**

```go
var MyNewCounter = prometheus.NewCounterVec(prometheus.CounterOpts{
    Namespace: "tierfs",
    Subsystem: "mysubsystem",
    Name:      "things_total",
    Help:      "Number of things processed.",
}, []string{"label1", "label2"})

// Register in init() or NewRegistry():
prometheus.MustRegister(MyNewCounter)
```

**Naming convention:** `tierfs_{subsystem}_{what}_{unit}` — e.g. `tierfs_backend_bytes_read_total`, `tierfs_replication_job_duration_seconds`.

**2. Increment in the appropriate decorator:**

- For backend operations: `internal/observability/decorators/backend.go`
- For metadata operations: `internal/observability/decorators/meta.go`
- For application-level events (eviction, replication): directly in `app/evictor.go` or `app/replicator.go` via the observability stack

**3. Add to `docs/OPERATIONS.md`** — add a row to the Prometheus Metrics Reference table.

---

## Code Style

**Formatting and linting:**
```bash
gofmt -w -s .
go vet ./...
golangci-lint run ./...
```

**Error wrapping** — always use `fmt.Errorf("context: %w", err)`. Never swallow errors silently. The `%w` verb enables `errors.Is` / `errors.As` unwrapping.

**No `panic` in library code** — `panic` is only acceptable in `main()` for truly unrecoverable startup errors (e.g. broken binary). All error paths must return errors.

**Context propagation** — every exported function or method that performs I/O must accept `context.Context` as its first argument.

**Table-driven tests:**
```go
func TestBackend_Put(t *testing.T) {
    cases := []struct {
        name    string
        relPath string
        data    []byte
        wantErr bool
    }{
        {"normal", "dir/file.mp4", []byte("data"), false},
        {"empty path", "", []byte("data"), true},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            // ...
        })
    }
}
```

**Test file conventions:**
- `foo_test.go` with `package foo` — white-box tests (access unexported symbols)
- `foo_test.go` with `package foo_test` — black-box tests (only exported API)
- Integration tests in `integration/` with `package integration_test`

---

## The Hexagonal Rule

The core architectural invariant is that **dependencies always point inward**:

```
cmd/ → adapters/ → app/ → domain/
                 ↗
       observability/
```

**Concretely:**
- `internal/domain/` must import **only Go stdlib** (`context`, `errors`, `fmt`, `io`, `time`, `strings`). Run `go list -f '{{.Imports}}' ./internal/domain/` and verify there are no third-party imports.
- `internal/app/` must import **only domain** and stdlib. It must not import any adapter package.
- `internal/adapters/` packages import **only domain** and their own specific external libraries. They must not import each other or `internal/app/`.
- `internal/observability/` imports domain (for the port interfaces to wrap), adapters are not imported.
- `cmd/` is the only package allowed to import everything.

**Checking compliance:**
```bash
# Domain must have no external imports
go list -f '{{.Imports}}' ./internal/domain/... | grep -v "^std"

# App must not import adapters
grep -r "adapters/" internal/app/ && echo "VIOLATION"
```

PRs that violate these rules will not be merged, regardless of test coverage.

---

## Commit Convention

TierFS uses [Conventional Commits](https://www.conventionalcommits.org/):

| Type | When to use | Example |
|---|---|---|
| `feat:` | New feature or capability | `feat: add Ceph RGW backend adapter` |
| `fix:` | Bug fix | `fix: digest mismatch on empty file replication` |
| `perf:` | Performance improvement | `perf: use io.TeeReader instead of io.Pipe for streaming digest` |
| `refactor:` | Code change with no feature/fix | `refactor: extract stager into its own package` |
| `test:` | Test-only changes | `test: add concurrent write integration test` |
| `docs:` | Documentation only | `docs: add Backblaze B2 config example` |
| `chore:` | Maintenance (deps, CI, tooling) | `chore: upgrade AWS SDK v2 to v1.35` |

Breaking changes must include `BREAKING CHANGE:` in the commit footer:
```
feat!: change evict_schedule after field from int to duration string

BREAKING CHANGE: evict_schedule.after is now a Go duration string ("24h")
instead of an integer number of seconds. Update all config files.
```

---

## Pull Request Checklist

Before opening a PR, verify:

- [ ] All tests pass: `make test`
- [ ] No new linter warnings: `make lint`
- [ ] `-race` detector clean: `make test-unit` (race detector is enabled by default)
- [ ] New backends have unit tests covering Put/Get/Stat/Delete/List and error cases
- [ ] Non-trivial changes have integration tests in `integration/`
- [ ] Config changes are documented in `docs/CONFIGURATION.md`
- [ ] New metrics are documented in `docs/OPERATIONS.md`
- [ ] No debug print statements (`fmt.Println`, `log.Print`) left in code
- [ ] No `internal/domain/` → `adapters/` import violations
- [ ] PR description explains the **why**, not just the what; includes issue number if applicable

---

## Reporting Issues

**Bug reports** — please include:
- TierFS version (`tierfs -version`)
- OS and kernel version
- FUSE version (`fusermount3 --version`)
- Relevant section of `tierfs.toml` (redact credentials)
- Steps to reproduce
- Expected behaviour
- Actual behaviour
- Relevant log lines (JSON format preferred; set `level = "debug"` for the affected component)

**Security vulnerabilities** — please do **not** open a public GitHub issue. Email `security@PLACEHOLDER` with a description of the issue. We will respond within 48 hours and coordinate a fix and responsible disclosure timeline before any public announcement.

---

## License

TierFS is MIT licensed. By contributing, you agree that your contributions will be licensed under the [MIT License](LICENSE). You retain copyright to your own contributions.
