# tierfs.toml(5) — TierFS Configuration Reference

## NAME

**tierfs.toml** — configuration file for the TierFS FUSE storage tiering daemon

## SYNOPSIS

```
/etc/tierfs/tierfs.toml
```

## DESCRIPTION

TierFS reads a single TOML configuration file at startup. The file location defaults to `/etc/tierfs/tierfs.toml` and can be overridden with the `-config` command-line flag.

The configuration describes the mount point, metadata store, replication and eviction behaviour, observability stack, backend storage targets, tier definitions, and the policy rules that govern which files are stored where and for how long. All fields are validated at startup; TierFS will refuse to start and print a descriptive error if the configuration is invalid.

The file uses [TOML v1.0](https://toml.io/en/v1.0.0) syntax. String values must be quoted. Duration values use Go duration syntax (`"30s"`, `"5m"`, `"24h"`). Capacity values accept standard binary suffixes (`"500GiB"`, `"8TiB"`, `"unlimited"`).

---

## [mount]

Controls the FUSE mount point and storage paths for metadata and staging.

| Field       | Type   | Default               | Required | Description                                                                                                                                 |
|-------------|--------|-----------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `path`      | string | —                     | **yes**  | Absolute path where the FUSE filesystem is mounted. The directory must exist and be empty.                                                  |
| `meta_db`   | string | —                     | **yes**  | Absolute path for the SQLite metadata database. The file is created on first run; the directory must exist.                                 |
| `stage_dir` | string | `"/tmp/tierfs-stage"` | no       | Temporary directory used when staging files from remote (non-local) backends for reads. Must be on a filesystem with sufficient free space. |

> **NOTE:** The mount `path` must be accessible by the user running tierfs. On systems with default FUSE configuration, only root can mount FUSE filesystems. Grant non-root access by adding `user_allow_other` to `/etc/fuse.conf` and running with `--allow-other` in the mount options, or run tierfs as root with `CAP_SYS_ADMIN`.
>
> If `stage_dir` fills up, cold-tier reads will fail with `ENOSPC`. Monitor `tierfs_fuse_staged_bytes_total` and configure a janitor (or use `tmpfs` with appropriate size) for high-traffic deployments.

```toml
[mount]
path      = "/share/CCTV"
meta_db   = "/var/lib/tierfs/meta.db"
stage_dir = "/tmp/tierfs-stage"
```

---

## [replication]

Controls the async worker pool that copies files between tiers after a write.

| Field                    | Type     | Default    | Required | Description                                                                                                                                                                   |
|--------------------------|----------|------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `workers`                | int      | `4`        | no       | Number of concurrent copy goroutines. Each worker holds one source `Get()` stream and one destination `Put()` stream open simultaneously. Range: 1–64.                        |
| `retry_interval`         | duration | `"30s"`    | no       | How long to wait before retrying a failed copy or a write-active delay. No exponential backoff.                                                                               |
| `max_retries`            | int      | `5`        | no       | Maximum number of attempts per job. Set to `0` for infinite retries. After exhausting retries the job is dropped and an error is logged; the file remains on the source tier. |
| `verify`                 | string   | `"digest"` | no       | Post-copy verification mode. One of `"none"`, `"size"`, or `"digest"`. See below.                                                                                             |
| `write_quiescence`       | duration | `"0s"`     | no       | Minimum idle time after the last write handle closes before a file is eligible for replication. See [Write Safety](#write-safety) below.                                      |
| `bandwidth_limit`        | string   | `""`       | no       | Global rate limit for replication uploads. Shared across all workers. Examples: `"50MiB/s"`, `"100MB/s"`. Empty or `"unlimited"` = no limit. See [Bandwidth Throttling](#bandwidth-throttling) below. |
| `health_check_interval`  | duration | `"30s"`    | no       | How often to probe each backend for connectivity. See [Backend Health Checks](#backend-health-checks) below.                                                                  |
| `health_check_timeout`   | duration | `"10s"`    | no       | Per-backend probe timeout. If a probe does not complete within this duration, the backend is marked unhealthy.                                                                 |

### Write Safety

TierFS is optimised for **immutable-after-close** workloads (Frigate NVR segments, Immich originals, security camera recordings). It detects and handles active writes through two complementary mechanisms:

**1. Open handle guard** — Any file with at least one open write handle (`O_WRONLY`, `O_RDWR`, or a new `Create`) will not be replicated. This is tracked in-process via a `WriteGuard` map. The replicator checks the guard before starting any copy; if the file is write-active it re-enqueues silently without consuming a retry count.

**2. Quiescence window** — Some applications write a file in multiple phases. A common example: a video muxer creates a segment, closes it, then re-opens it 2–3 seconds later to splice in a subtitle track or fix the moov atom. With `write_quiescence = "5s"`, the replicator waits 5 seconds after the last `close(2)` before starting the copy, giving the muxer time to finish its second write.

**3. Staleness re-check** — Immediately before opening the source for streaming, the replicator calls `Stat()` and compares the live `size` and `mtime` against what was recorded at write-complete time. If they differ (a write happened between enqueue and copy start), the job is aborted silently. `OnWriteComplete` will re-enqueue a fresh job once the new write finishes.

These three mechanisms together mean that: a file that is being actively written is never copied mid-write; a file that was modified after being enqueued is never copied stale; and a file that was re-opened for a second write phase is never copied until the second phase completes.

> **TierFS does not support mutable files as a general pattern.** If your workload involves files that are continuously or randomly modified after creation (databases, VM disk images, live log files), you should either `pin_tier` them to keep them on the hot tier indefinitely, or exclude them from tiering via a dedicated rule with `replicate = false`. The write-safety mechanisms above handle the common *two-phase write* pattern (write → close → brief gap → re-open → close), not arbitrary ongoing mutation.

### Choosing `write_quiescence`

| Workload                      | Recommended value | Reason                                                         |
|-------------------------------|-------------------|----------------------------------------------------------------|
| Frigate NVR recordings        | `"0s"` (default)  | Segments are complete and immutable on close                   |
| Frigate NVR clips             | `"0s"`            | Same                                                           |
| Immich original ingest        | `"5s"`            | EXIF writer may touch file briefly after upload completes      |
| General video transcoding     | `"10s"`           | Muxers vary; 10s covers most moov-atom fixup patterns          |
| Samba/NFS writes from Windows | `"30s"`           | Windows delayed-write cache can hold dirty pages for up to 30s |
| Conservative / unknown        | `"60s"`           | Safe default for any workload you haven't profiled             |

The cost of a higher quiescence value is only replication latency — files wait longer before being copied to the cold tier. There is no correctness risk from setting it too high.

### Verification Modes

**`none`** — No verification performed after copy. The copy is marked as verified immediately. Use only when the destination backend guarantees integrity (e.g. S3 with TLS + server-side checksums) and throughput is more important than safety.

> **⚠ WARNING:** `verify = "none"` means silent data corruption will not be detected. A network error, partial write, or S3 rate-limit truncation will result in a corrupted file on the cold tier with no alert. Do not use for irreplaceable data.

**`size`** — After `Put()` completes, calls `Stat()` on the destination and compares the size in bytes to the source metadata. Catches truncations and most network errors. About 10× faster than digest verification since it does not re-read the file. Does not detect bit-flip corruption.

**`digest`** — After `Put()` completes:
- For **file backends**: calls `digest.Verify(localPath, expectedDigest)` — reads the destination file and computes xxhash3-128 (>30 GB/s with SIMD).
- For **S3 backends**: the bytes were teed through a `bytes.Buffer` during `Put()`; digest is computed from the buffer in memory. This avoids a second GET from S3.

The source digest is computed by `OnWriteComplete()` immediately after the initial write to tier0. If the source file's digest is empty (e.g. an in-flight write that was interrupted), digest verification is skipped and the copy is trusted.

### Bandwidth Throttling

Rate-limits replication upload bandwidth to avoid saturating your network uplink. The limit is **global** — shared across all replication workers via a token-bucket rate limiter. Individual workers block briefly after each chunk read to stay within the budget.

The value uses the same unit syntax as capacity (`MiB`, `GiB`, `KB`, `MB`, etc.) with an optional trailing `/s`:

| Value         | Bytes/sec         | Notes                                      |
|---------------|-------------------|--------------------------------------------|
| `"50MiB/s"`  | 52,428,800        | Binary mebibytes per second                |
| `"100MB/s"`  | 100,000,000       | Decimal megabytes per second               |
| `"1GiB/s"`   | 1,073,741,824     | Gigabit link headroom                      |
| `""`          | unlimited         | Default — no throttle                      |
| `"unlimited"` | unlimited         | Explicit unlimited                         |

**Choosing a value:** set this to 60–80% of your uplink capacity. For a 1 Gbps link, `"100MiB/s"` leaves headroom for live camera streams. For a 100 Mbps WAN link to Backblaze, `"10MiB/s"` avoids starving other traffic.

The throttle is context-aware: if the `backend_timeout` expires during a throttled upload, the copy is cancelled cleanly without blocking indefinitely.

### Backend Health Checks

TierFS periodically probes each backend for connectivity by calling `Stat()` on a non-existent sentinel path (`.tierfs-health-probe`). The probe result determines health:

- **Healthy**: `Stat()` returns `ErrNotExist` (the backend responded — it's reachable)
- **Unhealthy**: `Stat()` returns any other error (timeout, connection refused, auth failure)

Health status is used in three places:

1. **Replicator** — skips unhealthy destination backends and re-enqueues the job for later. The retry does not consume a `max_retries` count — the file will be copied once the backend recovers.
2. **Prometheus** — `tierfs_backend_healthy` gauge (1.0 = healthy, 0.0 = unhealthy) per tier, for alerting.
3. **Admin UI** — per-tier health indicator in the dashboard and tier detail views.

Health transitions are logged: `backend unhealthy` (warn) when a probe fails, `backend recovered` (info) when it passes again.

> **Fail-open:** if health checks are not configured or a tier is not tracked, the replicator assumes it is healthy. This prevents health checks from accidentally blocking replication.

```toml
[replication]
workers                = 4
retry_interval         = "30s"
max_retries            = 5
verify                 = "digest"
write_quiescence       = "5s"    # set to "0s" for pure Frigate NVR deployments
bandwidth_limit        = "50MiB/s"
health_check_interval  = "30s"
health_check_timeout   = "10s"
```

---

## [eviction]

Controls the background loop that removes files from a tier once a verified copy exists on a colder tier.

| Field | Type | Default | Required | Description |
|---|---|---|---|---|
| `check_interval` | duration | `"5m"` | no | How frequently the evictor loop runs. Lower values increase responsiveness at the cost of more SQLite reads. |
| `capacity_threshold` | float | `0.85` | no | Fraction of a tier's configured `capacity` above which capacity-pressure eviction triggers. Range: 0.0–1.0. |
| `capacity_headroom` | float | `0.70` | no | Fraction to evict down to during capacity-pressure eviction. Files are evicted oldest-first until `used/capacity ≤ headroom`. |

### Two Eviction Triggers

**Age-based (schedule eviction)** — The primary mechanism. For each file on a tier, the evictor checks if `arrivedAt + EvictStep.After < now` AND the destination tier has a verified copy. If both conditions are met, the file is deleted from the current tier and `CurrentTier` is updated. This runs on every tick for every file.

**Capacity pressure** — Secondary, safety mechanism. If the ratio of `UsedBytes / CapacityBytes` for a tier exceeds `capacity_threshold`, the evictor evicts the oldest synced files on that tier regardless of their age schedule, stopping when `UsedBytes / CapacityBytes ≤ capacity_headroom`. This requires that a verified copy exist on a colder tier before evicting; if no verified cold copy exists the file is skipped.

> **NOTE:** Capacity pressure eviction counts bytes reported by the metadata store (size field set at write time). It does not directly measure filesystem usage. If files exist on a tier that were not written through TierFS (e.g. manually copied), capacity pressure will not account for them.

```toml
[eviction]
check_interval     = "5m"
capacity_threshold = 0.85
capacity_headroom  = 0.70
```

---

## [observability]

### [observability.logging]

| Field          | Type     | Default      | Required | Description                                                                                             |
|----------------|----------|--------------|----------|---------------------------------------------------------------------------------------------------------|
| `level`        | string   | `"info"`     | no       | Minimum log level. One of `"debug"`, `"info"`, `"warn"`, `"error"`.                                     |
| `format`       | string   | `"json"`     | no       | Output format. `"json"` for structured JSON (production); `"console"` for human-readable (development). |
| `output_paths` | []string | `["stdout"]` | no       | List of sinks. Each entry is `"stdout"`, `"stderr"`, or an absolute file path.                          |
| `development`  | bool     | `false`      | no       | Enables caller location and stack traces on error-level logs.                                           |

#### Log Levels in TierFS Context

- **`debug`** — Every FUSE syscall, every metadata read, every replication step. Extremely verbose; use only for troubleshooting a specific file.
- **`info`** — Startup/shutdown, successful copies, evictions, capacity events. Normal production level.
- **`warn`** — Retried replication failures, missing files on expected tiers, slow backend operations.
- **`error`** — Failed copies after exhausting retries, FUSE errors, metadata inconsistencies, backend auth failures.

#### Example JSON Log Line

```json
{
  "ts": "2026-03-13T19:32:43.127Z",
  "level": "info",
  "logger": "tier-service.replicator",
  "msg": "copying file",
  "rel_path": "recordings/cam1/2026-03-13/12-00-00.mp4",
  "from": "file:///data/tier0/recordings/cam1/2026-03-13/12-00-00.mp4",
  "to": "s3://nvr-archive/cctv/recordings/cam1/2026-03-13/12-00-00.mp4",
  "attempt": 1,
  "size": 524288000
}
```

#### [observability.logging.rotation]

Only active when `output_paths` contains at least one file path.

| Field          | Type | Default | Description                          |
|----------------|------|---------|--------------------------------------|
| `max_size_mb`  | int  | `100`   | File size in MB before rotation      |
| `max_backups`  | int  | `5`     | Number of old log files to retain    |
| `max_age_days` | int  | `30`    | Delete rotated files older than this |
| `compress`     | bool | `true`  | Gzip rotated files                   |

```toml
[observability.logging]
level        = "info"
format       = "json"
output_paths = ["stdout", "/var/log/tierfs/tierfs.log"]
development  = false

[observability.logging.rotation]
max_size_mb  = 100
max_backups  = 5
max_age_days = 30
compress     = true
```

### [observability.metrics]

| Field     | Type   | Default      | Description                               |
|-----------|--------|--------------|-------------------------------------------|
| `enabled` | bool   | `true`       | Expose the Prometheus `/metrics` endpoint |
| `address` | string | `":9100"`    | TCP listen address                        |
| `path`    | string | `"/metrics"` | HTTP path for Prometheus scrape           |

The metrics server also serves `GET /healthz` → `200 ok` on the same address.

Exposed metric names (namespace `tierfs_`): `backend_operations_total`, `backend_operation_duration_seconds`, `backend_bytes_read_total`, `backend_bytes_written_total`, `backend_healthy`, `meta_operations_total`, `meta_operation_duration_seconds`, `replication_queue_depth`, `replication_lag_seconds`, `replication_jobs_total`, `replication_job_duration_seconds`, `replication_bytes_transferred_total`, `eviction_events_total`, `fuse_operations_total`, `fuse_operation_duration_seconds`, `fuse_staged_bytes_total`, `tier_file_count`, `tier_bytes_used`.

### [observability.tracing]

| Field          | Type   | Default            | Description                            |
|----------------|--------|--------------------|----------------------------------------|
| `enabled`      | bool   | `false`            | Send OTLP spans to a collector         |
| `endpoint`     | string | `"localhost:4317"` | OTLP gRPC endpoint (no scheme prefix)  |
| `service_name` | string | `"tierfs"`         | `service.name` resource attribute      |
| `sample_rate`  | float  | `1.0`              | Fraction of traces to sample (0.0–1.0) |
| `insecure`     | bool   | `false`            | Disable TLS on the OTLP connection     |

Compatible with any OTLP-capable collector: Grafana Tempo, Jaeger, Honeycomb, Datadog Agent (OTLP mode), OpenTelemetry Collector.

---

## [[backend]]

Defines a named storage target. Each backend is referenced by one or more `[[tier]]` entries.

| Field        | Type   | Default | Required | Description                                                                 |
|--------------|--------|---------|----------|-----------------------------------------------------------------------------|
| `name`       | string | —       | **yes**  | Unique identifier; referenced by `[[tier]].backend`                         |
| `uri`        | string | —       | **yes**  | Storage URI. `file:///absolute/path` or `s3://bucket/optional-prefix`       |
| `endpoint`   | string | —       | S3 only  | Custom endpoint URL including scheme. Required for non-AWS S3.              |
| `region`     | string | —       | S3 only  | AWS region. Use `"us-east-1"` for MinIO/Ceph that don't enforce region.     |
| `path_style` | bool   | `false` | S3 only  | Force path-style URLs (`endpoint/bucket/key`). Required for MinIO and Ceph. |
| `access_key` | string | —       | S3 only  | AWS access key ID. Prefer `AWS_ACCESS_KEY_ID` environment variable.         |
| `secret_key` | string | —       | S3 only  | AWS secret access key. Prefer `AWS_SECRET_ACCESS_KEY` environment variable. |

### URI Scheme Reference

**`file:///path`** — Files are stored in a local directory tree. The path must be an absolute path on the host filesystem. NFS and SMB mounts are supported if the mount is available when TierFS starts. `LocalPath()` returns the real absolute path, enabling zero-copy FUSE loopback reads via the kernel page cache.

**`s3://bucket/prefix`** — Files are stored in the named S3 bucket under the given prefix. The prefix may be empty (`s3://bucket`). All object keys are `prefix/relPath`. `LocalPath()` always returns false; reads trigger staging to `stageDir`.

**`null://`** — The discard backend. All writes are accepted and drained to `/dev/null`; all reads return `ErrNotExist`. Use as the final tier in a retention policy to permanently delete files after a configured age. When a file is evicted to a `null://` tier, it is also purged from metadata entirely — no ghost records remain. See [Terminal Tier / Permanent Deletion](#terminal-tier--permanent-deletion) below.

### Credential Precedence (S3)

Credentials are resolved in this order:
1. `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables
2. `access_key` / `secret_key` fields in the config file
3. AWS shared credentials file (`~/.aws/credentials`)
4. EC2/ECS instance metadata (if running on AWS)

> **Security:** Do not store credentials in the config file if it is committed to version control. Use environment variables or AWS IAM roles.

### S3 Compatibility Matrix

| Service       | `endpoint`                                 | `path_style` | Notes                                       |
|---------------|--------------------------------------------|--------------|---------------------------------------------|
| AWS S3        | _(omit)_                                   | `false`      | Default; region required                    |
| MinIO         | `https://minio.lan:9000`                   | `true`       | Required for non-virtual-host MinIO         |
| Ceph RGW      | `https://ceph.lan:7480`                    | `true`       | RGW default port is 7480                    |
| Backblaze B2  | `https://s3.us-west-004.backblazeb2.com`   | `false`      | Region in endpoint URL; get from B2 console |
| Garage        | `https://garage.lan:3900`                  | `true`       | s3_region = "garage"                        |
| Cloudflare R2 | `https://ACCOUNT.r2.cloudflarestorage.com` | `false`      | No egress fees                              |

---

## [[backend]] Transform Configuration

Any backend can have an optional `[backend.transform]` block that applies compression, encryption, or both to all data written to and read from that backend. The transformation is fully transparent to the application — files are compressed/encrypted on write and decrypted/decompressed on read automatically.

**Ordering is enforced:** when both compression and encryption are configured, compression is always applied first on the write path (plaintext → compress → encrypt → store). This is reversed on read (store → decrypt → decompress → plaintext). Compressing already-encrypted data yields no size reduction; TierFS enforces the correct order regardless of which appears first in the config.

### [backend.transform.compression]

| Field   | Type | Default | Description                                                                                                                                                                            |
|---------|------|---------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `level` | int  | `-1`    | gzip compression level. `1` = fastest, `9` = best compression, `-1` = default (≈6). Use `1` for video/media (already compressed); use `6`–`9` for logs, databases, or text-heavy data. |

> **For Frigate recordings (H.264/H.265):** the compression ratio will be near 1.0 — the data is already compressed. Adding compression only wastes CPU. Omit `[backend.transform.compression]` for video workloads. Use it for NAS tiers storing metadata, SQLite exports, or log archives.

### [backend.transform.encryption]

| Field     | Type   | Default | Required | Description                                                                                                              |
|-----------|--------|---------|----------|--------------------------------------------------------------------------------------------------------------------------|
| `key_env` | string | —       | one of   | Environment variable name containing the 64-char hex key. Takes precedence over `key_hex`. **Preferred for production.** |
| `key_hex` | string | —       | one of   | 64-character lowercase hex string (32 bytes). Avoid committing to VCS.                                                   |

The encryption algorithm is AES-256-GCM in a chunked streaming format (64 KiB chunks, random nonce per chunk, chunk index as AAD to prevent reordering). Each chunk is independently authenticated; corruption is detected at read time per chunk rather than requiring the entire file to be read first.

**Generating a key:**
```bash
# With OpenSSL
openssl rand -hex 32

# With the tierfs CLI (planned)
tierfs keygen
```

**Key management:** store keys in Ansible Vault, HashiCorp Vault, or a system secrets manager. Pass to tierfs via environment variable:
```bash
# systemd unit
Environment=TIERFS_NAS_KEY=<redacted>
# or EnvironmentFile pointing to a protected file
EnvironmentFile=/etc/tierfs/tierfs.env   # mode 0600, owner tierfs
```

> **⚠ WARNING:** if the key is lost, all data encrypted with it is permanently unrecoverable. Back up keys separately from the data they protect. Rotating a key requires re-encrypting all files on the backend (no built-in key rotation; use a new backend with a new key and re-replicate).

### Example: Encrypted NAS tier

```toml
[[backend]]
name = "nas-encrypted"
uri  = "file:///mnt/nas/cctv-enc"

[backend.transform.encryption]
key_env = "TIERFS_NAS_KEY"    # export TIERFS_NAS_KEY=$(openssl rand -hex 32)
```

### Example: Compressed and encrypted S3 tier (logs/metadata)

```toml
[[backend]]
name = "s3-compressed-encrypted"
uri  = "s3://my-bucket/tierfs"
endpoint   = "https://minio.lan:9000"
region     = "us-east-1"
path_style = true

[backend.transform.compression]
level = 6   # good ratio for log/metadata data; omit for video

[backend.transform.encryption]
key_env = "TIERFS_S3_KEY"
# Write path: plaintext → gzip(level=6) → aes-256-gcm → S3
# Read path:  S3 → aes-256-gcm decrypt → gzip decompress → plaintext
```

### Example: Encryption only on Backblaze B2 (video, already compressed)

```toml
[[backend]]
name     = "backblaze"
uri      = "s3://my-bucket/cctv"
endpoint = "https://s3.us-west-004.backblazeb2.com"
region   = "us-west-004"

[backend.transform.encryption]
key_env = "TIERFS_B2_KEY"
# No compression: H.264/H.265 video does not compress further
# Write path: plaintext → aes-256-gcm → Backblaze B2
# Read path:  Backblaze B2 → aes-256-gcm decrypt → plaintext
```

---

## smb:// — SMB2/SMB3 Backend

The SMB backend uses a pure-Go SMB2/3 client (`github.com/hirochachacha/go-smb2`). No kernel mount, no `mount.cifs`, no root privileges required. The SMB session lives entirely within the tierfs process.

### URI Format

```
smb://[user[:password]@]host[:port]/share[/optional/prefix]
```

| Example                                        | Description                                 |
|------------------------------------------------|---------------------------------------------|
| `smb://nas.lan/recordings`                     | Synology NAS, share `recordings`, no prefix |
| `smb://admin:secret@192.168.1.10/cctv/frigate` | Credentials in URI, prefix `frigate`        |
| `smb://nas.lan:445/data`                       | Explicit port (same as default)             |
| `smb://[::1]/share`                            | IPv6 host                                   |

> **Security:** Embedding passwords in the URI stores them in the config file on disk. Use `smb_username`/`smb_password` config fields or `TIERFS_SMB_USER`/`TIERFS_SMB_PASS` environment variables instead.

### [[backend]] Fields for SMB

| Field                    | Type   | Default | Description                                                                                                             |
|--------------------------|--------|---------|-------------------------------------------------------------------------------------------------------------------------|
| `uri`                    | string | —       | SMB URI (see format above). Credentials here are lowest priority.                                                       |
| `smb_username`           | string | —       | NTLM username. Prefer `TIERFS_SMB_USER` env var.                                                                        |
| `smb_password`           | string | —       | NTLM password. Prefer `TIERFS_SMB_PASS` env var.                                                                        |
| `smb_domain`             | string | `""`    | Windows/AD domain. Leave empty for workgroup NAS (Synology, TrueNAS, QNAP). Required for Active Directory environments. |
| `smb_require_encryption` | bool   | `false` | Request SMB3 session encryption. Requires SMB 3.0+ on the server. Most NAS firmware from 2015+ qualifies.               |

### Credential Resolution Order

1. `TIERFS_SMB_USER` / `TIERFS_SMB_PASS` environment variables
2. `smb_username` / `smb_password` config fields
3. Credentials embedded in the URI

### NAS Compatibility

| Device               | Protocol  | Notes                                                       |
|----------------------|-----------|-------------------------------------------------------------|
| Synology DSM 6+      | SMB2/SMB3 | Enable SMB in File Services; create a dedicated share       |
| TrueNAS CORE/SCALE   | SMB2/SMB3 | Create a share via Web UI; ACL must allow the user          |
| QNAP QTS             | SMB2/SMB3 | Enable Windows Network Neighborhood; create a shared folder |
| Windows Server       | SMB2/SMB3 | Use a service account with write access to the share        |
| Linux Samba          | SMB2/SMB3 | Set `min protocol = SMB2` in `smb.conf`                     |
| Raspberry Pi (Samba) | SMB2      | Works well; disable SMB1 in smb.conf                        |

### Connection Resilience

TierFS maintains a single SMB session per backend. If a network error occurs (NAS reboot, switch failover, cable pull), the backend automatically reconnects and retries the failed operation once. Concurrent operations wait during the brief reconnect window (typically 500ms–30s depending on the NAS restart time).

### Performance Notes

- SMB2 supports request compounding — multiple operations can be pipelined over the same TCP connection. For large sequential reads/writes (Frigate segments), expect 100–300 MB/s on gigabit Ethernet.
- `LocalPath()` always returns `("", false)`. SMB files cannot be served directly via the FUSE kernel page cache; cold reads are staged to `stage_dir`. Enable `promote_on_read` on rules matching frequently re-accessed cold clips to minimise staging overhead.
- For maximum throughput to a NAS, use a `file://` backend over NFS instead — NFS supports direct `mmap()` and zero-copy reads that SMB cannot match from userspace.

### Example: Synology NAS as tier1

```toml
[[backend]]
name         = "synology"
uri          = "smb://nas.lan/cctv"
smb_username = "tierfs-svc"    # dedicated service account, not admin
# smb_password: set TIERFS_SMB_PASS=<password> in environment

[[tier]]
name     = "tier1"
backend  = "synology"
priority = 1
capacity = "8TiB"
```

### Example: Synology with SMB3 encryption and zstd compression

```toml
[[backend]]
name                  = "synology-enc"
uri                   = "smb://nas.lan/cctv"
smb_username          = "tierfs-svc"
smb_require_encryption = true     # requires DSM 6.2+ with SMB3

[backend.transform.compression]
algorithm = "zstd"
level     = 1     # fast; video doesn't compress much anyway

# No encryption transform needed: SMB3 encryption covers the wire;
# for at-rest encryption add [backend.transform.encryption] as well.
```

### Example: TrueNAS with Active Directory

```toml
[[backend]]
name       = "truenas-ad"
uri        = "smb://truenas.corp.example.com/recordings"
smb_domain = "CORP"    # AD domain name (NetBIOS short name)
# Credentials from env: TIERFS_SMB_USER=svc_tierfs TIERFS_SMB_PASS=...
```

### Troubleshooting SMB

**`STATUS_ACCESS_DENIED`** — The user does not have write permission on the share. On Synology: Admin Panel → Shared Folder → Edit → Permissions → set Read/Write for the user. On TrueNAS: check the share's ACL.

**`STATUS_LOGON_FAILURE`** — Wrong username or password. Verify with `smbclient -U user //host/share`. Check that `smb_domain` matches the server's workgroup or AD domain.

**`STATUS_BAD_NETWORK_NAME`** — The share name does not exist. Verify the exact share name (case-sensitive on Linux Samba, case-insensitive on Synology/Windows).

**Connection refused on port 445** — SMB is disabled or firewalled on the NAS. Enable SMB in NAS settings. If port 445 is blocked by a firewall rule, add an exception for the tierfs host IP.

**Slow throughput** — Check whether the NAS is the bottleneck (`iotop` on TrueNAS, Resource Monitor on DSM). For bulk sequential I/O, NFS with `file://` backend typically outperforms SMB by 20–40% due to better kernel integration.

---

## sftp:// — SFTP Backend

The SFTP backend uses a pure-Go SSH/SFTP client (`github.com/pkg/sftp` over `golang.org/x/crypto/ssh`). No native `ssh` binary, no FUSE, no root privileges required. Works with any standard SFTP server: OpenSSH, Dropbear, ProFTPD, Rebex, WinSCP server, NAS firmware (Synology, TrueNAS, QNAP all expose SFTP).

### URI Format

```
sftp://[user@]host[:port]/base/path[/optional/prefix]
```

| Example                                                | Description                                        |
|--------------------------------------------------------|----------------------------------------------------|
| `sftp://nas.lan/mnt/storage/cctv`                      | OpenSSH on NAS, base `/mnt/storage`, prefix `cctv` |
| `sftp://admin@192.168.1.10/data/frigate`               | Explicit user, base `/data`, prefix `frigate`      |
| `sftp://backup@offsite.example.com:2222/backups/tier2` | Non-standard port, offsite server                  |
| `sftp://[::1]/srv/media`                               | IPv6 host                                          |

The path is split at the first component: `sftp://host/mnt/storage/cctv` → `basePath=/mnt`, `prefix=storage/cctv`. This matches the SMB convention of keeping the mount root and sub-path distinct in config.

### [[backend]] Fields for SFTP

| Field                   | Type   | Default | Description                                                                                                                                       |
|-------------------------|--------|---------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `uri`                   | string | —       | SFTP URI (see format above)                                                                                                                       |
| `sftp_username`         | string | —       | SSH username. Prefer `TIERFS_SFTP_USER` env var. Falls back to OS user (`$USER`).                                                                 |
| `sftp_password`         | string | —       | SSH password. Prefer `TIERFS_SFTP_PASS` env var. Leave empty when using key auth.                                                                 |
| `sftp_key_path`         | string | —       | Absolute path to PEM private key. Prefer `TIERFS_SFTP_KEY_PATH` env var. If empty, tries `~/.ssh/id_ed25519`, `~/.ssh/id_ecdsa`, `~/.ssh/id_rsa`. |
| `sftp_key_passphrase`   | string | —       | Decrypts an encrypted private key. Prefer `TIERFS_SFTP_KEY_PASSPHRASE` env var.                                                                   |
| `sftp_host_key`         | string | —       | Expected server public key in `authorized_keys` format (`ssh-ed25519 AAAA...`). If empty, host key verification is skipped (insecure).            |
| `sftp_known_hosts_file` | string | —       | Path to a `known_hosts` file. Takes precedence over `sftp_host_key`. *(Not yet implemented — use `sftp_host_key` for now.)*                       |

### Authentication Methods

Tried in this order:

1. **SSH agent** — if `SSH_AUTH_SOCK` is set and the private key is loaded (`ssh-add`). Zero config; ideal for development and systemd user sessions with `SSH_AUTH_SOCK` forwarded.
2. **Private key file** — resolved from `TIERFS_SFTP_KEY_PATH` env var → `sftp_key_path` config field → default locations (`~/.ssh/id_ed25519`, `~/.ssh/id_ecdsa`, `~/.ssh/id_rsa`).
3. **Password** — resolved from `TIERFS_SFTP_PASS` env var → `sftp_password` config field.

If none of the above produces a usable credential, `New()` returns an error at startup.

### Host Key Verification

> **⚠ Production requirement:** always set `sftp_host_key` or `sftp_known_hosts_file`. Without one, TierFS logs a warning and uses `InsecureIgnoreHostKey`, making the connection vulnerable to MITM attacks.

Get the server's host key fingerprint:
```bash
ssh-keyscan -t ed25519 nas.lan
# Output: nas.lan ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA...
```

Use the full key (not just the fingerprint) as `sftp_host_key`:
```toml
sftp_host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAA..."
```

### Credential Resolution Order

1. `TIERFS_SFTP_USER` / `TIERFS_SFTP_PASS` / `TIERFS_SFTP_KEY_PATH` environment variables
2. `sftp_username` / `sftp_password` / `sftp_key_path` config fields
3. URI userinfo (for username only; passwords in URIs are not recommended)

### Example: OpenSSH on NAS with Ed25519 key

```toml
[[backend]]
name          = "nas-sftp"
uri           = "sftp://tierfs@nas.lan/mnt/nvr/cctv"
sftp_key_path = "/etc/tierfs/id_ed25519"     # dedicated service key
sftp_host_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOqP..."
# No password — key auth only
```

```bash
# Generate a dedicated key for tierfs (no passphrase for unattended service):
ssh-keygen -t ed25519 -f /etc/tierfs/id_ed25519 -C "tierfs@$(hostname)" -N ""
# Add public key to NAS authorized_keys for the tierfs user:
ssh-copy-id -i /etc/tierfs/id_ed25519.pub tierfs@nas.lan
```

### Example: Offsite backup server on port 2222 with passphrase-protected key

```toml
[[backend]]
name                 = "offsite"
uri                  = "sftp://backup@offsite.example.com:2222/backups/cctv"
sftp_host_key        = "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAAB..."
# Key and passphrase from environment to avoid plaintext secrets in config:
# TIERFS_SFTP_KEY_PATH=/etc/tierfs/offsite_id_rsa
# TIERFS_SFTP_KEY_PASSPHRASE=<passphrase>

[[tier]]
name     = "tier2"
backend  = "offsite"
priority = 2
capacity = "unlimited"
```

### Example: Encrypted SFTP tier (AES-256-GCM at rest)

```toml
[[backend]]
name          = "nas-sftp-enc"
uri           = "sftp://tierfs@nas.lan/mnt/nvr/cctv"
sftp_key_path = "/etc/tierfs/id_ed25519"
sftp_host_key = "ssh-ed25519 AAAAC3..."

[backend.transform.encryption]
key_env = "TIERFS_SFTP_ENCRYPT_KEY"
# Data is encrypted before transmission — even if the SFTP server is
# compromised, stored files are ciphertext.
```

### Example: SSH agent auth (development / interactive sessions)

```toml
[[backend]]
name = "dev-sftp"
uri  = "sftp://myuser@dev-nas.local/home/myuser/tierfs-test"
# No credentials — uses SSH_AUTH_SOCK agent automatically
# (run: ssh-add ~/.ssh/id_ed25519  before starting tierfs)
```

### Performance Notes

- SFTP over a LAN gigabit connection typically achieves 80–200 MB/s depending on server CPU and SSH cipher. Ed25519 + AES-256-GCM-SHA256 cipher is recommended: hardware-accelerated on all modern CPUs, lower CPU overhead than ChaCha20 on x86.
- `LocalPath()` always returns `("", false)`. SFTP files go through staging on cold reads. Enable `promote_on_read` on frequently re-accessed cold-tier files.
- For raw throughput to a local NAS, `file://` over NFS outperforms SFTP due to zero userspace copying. Use SFTP when NFS is unavailable (remote/WAN servers, restricted networks, NAS devices that don't expose NFS).

### NAS / Server Compatibility

| Server                      | Notes                                                                                                                                            |
|-----------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| OpenSSH (Linux, macOS, BSD) | Full support; use `sftp` subsystem (default). Set `Subsystem sftp /usr/lib/openssh/sftp-server` in `sshd_config`.                                |
| Synology DSM                | Enable SSH in Control Panel → Terminal & SNMP. SFTP uses port 22.                                                                                |
| TrueNAS SCALE               | Enable SSH service; add user with SSH key in Credentials.                                                                                        |
| QNAP QTS                    | Enable SSH in Control Panel → Network & File Services → Telnet/SSH.                                                                              |
| Dropbear                    | Full support; commonly used on embedded NAS and routers.                                                                                         |
| ProFTPD (mod_sftp)          | Full support.                                                                                                                                    |
| Windows OpenSSH             | Full support since Windows Server 2019. Paths use forward slashes. Rename-over-existing may need Remove+Rename fallback (handled automatically). |
| Rebex SFTP server           | Full support.                                                                                                                                    |

### Troubleshooting SFTP

**`no authentication methods available`** — No password set, no key file found, and `SSH_AUTH_SOCK` is not set. Set `sftp_key_path` or `sftp_password`, or load a key into the SSH agent.

**`ssh: handshake failed: ssh: unable to authenticate`** — Wrong credentials. Test with: `ssh -i /etc/tierfs/id_ed25519 tierfs@nas.lan`. Check that the public key is in `~/.ssh/authorized_keys` on the server.

**`ssh: handshake failed: knownhosts: key mismatch`** — Server host key has changed (upgrade, re-install, MITM). Re-run `ssh-keyscan` and update `sftp_host_key`.

**`permission denied`** — The SFTP user doesn't have write access to the base path. Check server-side permissions: `ls -la /mnt/nvr/cctv` on the server.

**`connection refused` on port 22** — SSH is disabled on the NAS. Enable it in NAS settings. Check firewall rules if connecting over WAN.

**Slow throughput** — Try a faster SSH cipher: add `sftp_cipher = "aes128-gcm@openssh.com"` (planned config field). On gigabit LAN, throughput below 50 MB/s usually indicates server CPU bottleneck or TCP window size misconfiguration.

---

## Terminal Tier / Permanent Deletion

A `null://` backend acts as a blackhole terminal tier. Files evicted to it are:

1. Drained from the source tier backend (the Put() call streams to `io.Discard` so digest verification still runs on the source bytes).
2. Deleted from the source tier's backend storage.
3. **Purged from metadata entirely** — no `CurrentTier = "null"` ghost record is left behind.

This is the correct way to implement hard retention limits. Do **not** use `file:///dev/null` — that path is a character device, not a directory, and the file backend will fail with `ENOTDIR` on every write.

```toml
[[backend]]
name = "void"
uri  = "null://"      # no other fields required or accepted

[[tier]]
name     = "tier-delete"
backend  = "void"
priority = 3           # must be highest priority number (coldest)
capacity = "unlimited" # null tier has no storage; this is always unlimited
```

> **NOTE:** Once a file is evicted to the null tier, it is gone permanently. There is no recycle bin. Ensure your retention periods are correct before deploying. Test with a short `after` on non-critical data first.

---

## [[tier]]

Defines a named storage tier. Tiers are ordered by `priority`; writes always go to the lowest priority number.

| Field      | Type   | Default | Required | Description                                                                      |
|------------|--------|---------|----------|----------------------------------------------------------------------------------|
| `name`     | string | —       | **yes**  | Unique tier identifier; referenced by `[[rule]]` evict schedules                 |
| `backend`  | string | —       | **yes**  | References a `[[backend]].name`                                                  |
| `priority` | int    | —       | **yes**  | 0 = hottest tier; higher numbers = colder. Must be unique across tiers.          |
| `capacity` | string | —       | **yes**  | Maximum bytes this tier is allowed to hold. Used for capacity-pressure eviction. |

**Priority ordering**: New file writes always land on the tier with `priority = 0`. If that tier is full (capacity pressure) and no cold tier has a verified copy, the write proceeds anyway and capacity pressure eviction will reclaim space on the next tick.

**Capacity parsing**: Values are parsed case-insensitively. Supported suffixes:

| Suffix       | Multiplier                                                           |
|--------------|----------------------------------------------------------------------|
| `B`          | 1                                                                    |
| `KiB` / `KB` | 1,024 / 1,000                                                        |
| `MiB` / `MB` | 1,048,576 / 1,000,000                                                |
| `GiB` / `GB` | 1,073,741,824 / 1,000,000,000                                        |
| `TiB` / `TB` | 2^40 / 10^12                                                         |
| `PiB` / `PB` | 2^50 / 10^15                                                         |
| `unlimited`  | No capacity limit; capacity-pressure eviction disabled for this tier |

---

## [[rule]]

Rules define the eviction policy for files matching a path pattern. **Rules are evaluated in declaration order; the first match wins.** A catch-all rule (`match = "**"`) at the end is required — TierFS will refuse to start if no such rule is present.

| Field             | Type         | Default | Required | Description                                                               |
|-------------------|--------------|---------|----------|---------------------------------------------------------------------------|
| `name`            | string       | —       | **yes**  | Unique rule identifier used in logs and metrics                           |
| `match`           | string       | —       | **yes**  | Doublestar glob matched against the relative path                         |
| `pin_tier`        | string       | `""`    | no       | Tier name; files matching this rule are never auto-evicted from this tier |
| `evict_schedule`  | []EvictStep  | `[]`    | no       | Ordered list of eviction steps                                            |
| `promote_on_read` | bool\|string | `false` | no       | `false`, or a tier name to promote to on first read                       |
| `replicate`       | bool         | `true`  | no       | Whether to enqueue async replication after write                          |

### Glob Pattern Syntax

Uses [doublestar](https://github.com/bmatcuk/doublestar) matching (same as `.gitignore`):

| Pattern               | Matches                                          |
|-----------------------|--------------------------------------------------|
| `recordings/**`       | All files under `recordings/`, at any depth      |
| `clips/*.mp4`         | `.mp4` files directly in `clips/` (not subdirs)  |
| `thumbnails/**/*.jpg` | All `.jpg` files anywhere under `thumbnails/`    |
| `exports/2026/**`     | All files under the 2026 subdirectory of exports |
| `**`                  | All files (catch-all)                            |

### Evict Schedule Semantics

Each step in `evict_schedule` is an `{after = "duration", to = "tierName"}` pair. Steps are evaluated independently on each evictor tick. A file is moved from its current tier to `to` when **all** of:
1. The file has been on the current tier for at least `after` duration (measured from `ArrivedAt`)
2. A verified copy of the file exists on the `to` tier (i.e. replication has completed)
3. The file is not pinned to the current tier by `pin_tier`

Multiple steps can appear in one schedule to create a tiering ladder:

```toml
evict_schedule = [
  {after = "24h",  to = "tier1"},   # SSD → NAS after 1 day
  {after = "720h", to = "tier2"},   # NAS → MinIO after 30 days
  {after = "8760h", to = "tier3"},  # MinIO → Backblaze after 1 year
]
```

The schedule advances one step at a time. A file moves to tier1 after 24 hours, then (after becoming the CurrentTier on tier1) to tier2 after 720 hours from tier1 arrival, etc.

### `pin_tier`

Files matching a rule with `pin_tier` set will never be auto-evicted from that tier by the age schedule or capacity pressure. They can still be deleted via `Unlink`. Use for exports, reference footage, or any file that must be immediately accessible indefinitely.

```toml
[[rule]]
name     = "exports"
match    = "exports/**"
pin_tier = "tier0"
```

### `promote_on_read`

When a cold-tier file is read and `promote_on_read` is set to a tier name, a copy job is enqueued to bring the file back to the specified tier. This is useful for "access-based caching" — cold files that are unexpectedly re-accessed become hot again automatically.

Set to `false` (default) to disable promotion. Set to `"tier0"` to promote to the hottest tier.

```toml
[[rule]]
name            = "clips"
match           = "clips/**"
evict_schedule  = [{after = "168h", to = "tier1"}]
promote_on_read = "tier0"
```

### `replicate`

Setting `replicate = false` skips async replication entirely for matching files. The file remains on tier0 only. This is useful for scratch files that will be deleted before eviction would occur.

> **⚠ WARNING:** `replicate = false` without `pin_tier` means files are vulnerable to capacity-pressure eviction with nowhere to go. If capacity pressure forces eviction of an unreplicated file, the file is **deleted permanently**. Use `replicate = false` only with `pin_tier` or for explicitly ephemeral data.

---

## Rule Evaluation

Rules are evaluated in the order they appear in the config file. **The first rule whose `match` glob matches the file's relative path is used.** Subsequent rules are not evaluated.

**Validation at startup:**
- Every `[[tier]]` referenced in any `[[rule]]` must have a corresponding `[[tier]]` definition.
- Every `[[backend]]` referenced in any `[[tier]]` must have a corresponding `[[backend]]` definition.
- A catch-all rule (`match = "**"`) must appear as the last rule.
- Rule names must be unique.
- Tier priorities must be unique.

TierFS prints the validation error and exits with code 1 if any of these constraints are violated.

---

## Environment Variables

| Variable                | Used by                | Description                                                        |
|-------------------------|------------------------|--------------------------------------------------------------------|
| `AWS_ACCESS_KEY_ID`     | s3 backend             | AWS / S3-compatible access key ID                                  |
| `AWS_SECRET_ACCESS_KEY` | s3 backend             | AWS / S3-compatible secret access key                              |
| `AWS_REGION`            | s3 backend             | Override region without config file change                         |
| `AWS_ENDPOINT_URL`      | s3 backend             | Override endpoint without config file change (AWS SDK v2 standard) |
| `B2_APPLICATION_KEY_ID` | s3 backend (Backblaze) | Backblaze B2 key ID (use as AWS_ACCESS_KEY_ID)                     |
| `B2_APPLICATION_KEY`    | s3 backend (Backblaze) | Backblaze B2 application key (use as AWS_SECRET_ACCESS_KEY)        |

---

## Example Configurations

### Example 1: Minimal — Frigate NVR, 2-Tier (Local SSD + NAS)

```toml
# Minimal tierfs.toml for Frigate NVR
# tier0 = local SSD, tier1 = NAS via NFS mounted at /mnt/nas

[mount]
path    = "/share/CCTV"
meta_db = "/var/lib/tierfs/meta.db"

[replication]
workers = 4
verify  = "digest"

[eviction]
check_interval     = "5m"
capacity_threshold = 0.85
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file:///data/tier0"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/cctv"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "500GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "8TiB"

# Keep clips accessible for 7 days before moving to NAS
[[rule]]
name            = "clips"
match           = "clips/**"
evict_schedule  = [{after = "168h", to = "tier1"}]
promote_on_read = "tier0"

# Recordings: move to NAS after 24 hours
[[rule]]
name           = "recordings"
match          = "recordings/**"
evict_schedule = [{after = "24h", to = "tier1"}]

# Exports stay on SSD forever
[[rule]]
name     = "exports"
match    = "exports/**"
pin_tier = "tier0"

# Everything else moves after 48h
[[rule]]
name           = "default"
match          = "**"
evict_schedule = [{after = "48h", to = "tier1"}]
```

---

### Example 2: Three-Tier — SSD + NAS + MinIO

```toml
# 3-tier: SSD (hot) → NAS (warm, 30 days) → MinIO (cold, 1 year)

[mount]
path    = "/share/CCTV"
meta_db = "/var/lib/tierfs/meta.db"

[replication]
workers        = 8
retry_interval = "1m"
max_retries    = 10
verify         = "digest"

[eviction]
check_interval     = "10m"
capacity_threshold = 0.80
capacity_headroom  = 0.65

[[backend]]
name = "ssd"
uri  = "file:///data/ssd/cctv"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/cctv"

[[backend]]
name      = "minio"
uri       = "s3://nvr-archive/cctv"
endpoint  = "https://minio.lan:9000"
region    = "us-east-1"
path_style = true
# Credentials via environment: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "500GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "8TiB"

[[tier]]
name     = "tier2"
backend  = "minio"
priority = 2
capacity = "unlimited"

[[rule]]
name     = "exports"
match    = "exports/**"
pin_tier = "tier0"

[[rule]]
name  = "clips"
match = "clips/**"
evict_schedule = [
  {after = "168h",  to = "tier1"},   # NAS after 7 days
  {after = "720h",  to = "tier2"},   # MinIO after 30 days
]
promote_on_read = "tier0"

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [
  {after = "24h",  to = "tier1"},   # NAS after 1 day
  {after = "720h", to = "tier2"},   # MinIO after 30 days
]

[[rule]]
name  = "default"
match = "**"
evict_schedule = [
  {after = "48h",  to = "tier1"},
  {after = "720h", to = "tier2"},
]
```

---

### Example 3: Four-Tier with Backblaze B2 (Long-Term Archive)

```toml
# SSD (1 day) → NAS (30 days) → MinIO (1 year) → Backblaze B2 (forever)
# Optimised for Frigate NVR with 2-year retention requirement

[mount]
path    = "/share/CCTV"
meta_db = "/var/lib/tierfs/meta.db"

[replication]
workers        = 8
retry_interval = "2m"
max_retries    = 20    # Backblaze can be slow; be patient
verify         = "digest"

[eviction]
check_interval     = "15m"
capacity_threshold = 0.82
capacity_headroom  = 0.65

[[backend]]
name = "ssd"
uri  = "file:///data/ssd/cctv"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/cctv"

[[backend]]
name       = "minio"
uri        = "s3://nvr-archive/cctv"
endpoint   = "https://minio.lan:9000"
region     = "us-east-1"
path_style = true

[[backend]]
name     = "backblaze"
uri      = "s3://my-nvr-bucket/cctv"
endpoint = "https://s3.us-west-004.backblazeb2.com"
region   = "us-west-004"
# B2 application key — set via environment variables:
# AWS_ACCESS_KEY_ID=<keyID>  AWS_SECRET_ACCESS_KEY=<applicationKey>

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "500GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "8TiB"

[[tier]]
name     = "tier2"
backend  = "minio"
priority = 2
capacity = "20TiB"

[[tier]]
name     = "tier3"
backend  = "backblaze"
priority = 3
capacity = "unlimited"

[[rule]]
name     = "exports"
match    = "exports/**"
pin_tier = "tier0"

[[rule]]
name  = "clips"
match = "clips/**"
evict_schedule = [
  {after = "168h",  to = "tier1"},
  {after = "720h",  to = "tier2"},
  {after = "8760h", to = "tier3"},
]
promote_on_read = "tier0"

[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [
  {after = "24h",   to = "tier1"},
  {after = "720h",  to = "tier2"},
  {after = "8760h", to = "tier3"},
]

[[rule]]
name  = "default"
match = "**"
evict_schedule = [
  {after = "48h",   to = "tier1"},
  {after = "720h",  to = "tier2"},
  {after = "8760h", to = "tier3"},
]
```

---

### Example 4: Immich Photo Library

```toml
# Immich stores: library/ (originals), thumbs/ (generated), encoded/ (video), upload/ (staging)
# Strategy:
#   - RAW originals: never evict (pin to NAS)
#   - Thumbnails/encoded: tier1 after 7 days
#   - Upload staging: tier0 only, no replication (ephemeral)
#   - Everything else: standard 30-day schedule

[mount]
path    = "/share/immich"
meta_db = "/var/lib/tierfs/immich-meta.db"

[replication]
workers = 6
verify  = "digest"

[eviction]
check_interval     = "10m"
capacity_threshold = 0.85
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file:///data/ssd/immich"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/immich"

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "1TiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "unlimited"

# RAW originals: replicate to NAS immediately, pin there, never lose them
[[rule]]
name     = "originals"
match    = "library/**"
pin_tier = "tier1"
evict_schedule = [{after = "1h", to = "tier1"}]   # Move to NAS quickly

# Upload staging: temporary files during Immich processing; no replication
[[rule]]
name      = "upload-staging"
match     = "upload/**"
replicate = false
pin_tier  = "tier0"

# Generated thumbnails and web-optimised previews: move to NAS after 7 days
[[rule]]
name  = "thumbnails"
match = "thumbs/**"
evict_schedule = [{after = "168h", to = "tier1"}]

# Encoded video (transcoded for web): move to NAS after 7 days
[[rule]]
name  = "encoded-video"
match = "encoded-video/**"
evict_schedule = [{after = "168h", to = "tier1"}]

# Default for anything else Immich might create
[[rule]]
name  = "default"
match = "**"
evict_schedule = [{after = "720h", to = "tier1"}]
```

---

### Example 5: Frigate NVR with Hard 90-Day Deletion

For deployments where recordings must be permanently deleted after a retention period — compliance, storage constraints, or simply not wanting to pay for cold storage forever.

```toml
# SSD (1 day) → NAS (30 days) → permanently deleted after 90 days
# Files evicted to the null tier are purged from metadata entirely.

[mount]
path    = "/share/CCTV"
meta_db = "/var/lib/tierfs/meta.db"

[replication]
workers = 4
verify  = "digest"

[eviction]
check_interval     = "10m"
capacity_threshold = 0.85
capacity_headroom  = 0.70

[[backend]]
name = "ssd"
uri  = "file:///data/ssd/cctv"

[[backend]]
name = "nas"
uri  = "file:///mnt/nas/cctv"

[[backend]]
name = "void"
uri  = "null://"    # terminal tier — files evicted here are gone forever

[[tier]]
name     = "tier0"
backend  = "ssd"
priority = 0
capacity = "500GiB"

[[tier]]
name     = "tier1"
backend  = "nas"
priority = 1
capacity = "8TiB"

[[tier]]
name     = "tier-delete"
backend  = "void"
priority = 2
capacity = "unlimited"

# Exports are pinned to SSD — never deleted automatically
[[rule]]
name     = "exports"
match    = "exports/**"
pin_tier = "tier0"

# Clips: keep on NAS for 30 days, then permanently delete
[[rule]]
name  = "clips"
match = "clips/**"
evict_schedule = [
  {after = "168h", to = "tier1"},       # NAS after 7 days
  {after = "720h", to = "tier-delete"}, # deleted after 30 days on NAS
]
promote_on_read = "tier0"

# Recordings: move to NAS after 1 day, permanently delete after 90 days total
[[rule]]
name  = "recordings"
match = "recordings/**"
evict_schedule = [
  {after = "24h",   to = "tier1"},       # NAS after 1 day
  {after = "2160h", to = "tier-delete"}, # deleted 90 days after NAS arrival
]

# Everything else: delete after 48h on NAS
[[rule]]
name  = "default"
match = "**"
evict_schedule = [
  {after = "48h",  to = "tier1"},
  {after = "720h", to = "tier-delete"},
]
```

TierFS uses Go's `time.ParseDuration` format. Values must be quoted strings.

| Example | Meaning |
|---|---|
| `"300ms"` | 300 milliseconds |
| `"30s"` | 30 seconds |
| `"5m"` | 5 minutes |
| `"1.5h"` | 1 hour 30 minutes |
| `"24h"` | 24 hours (1 day) |
| `"168h"` | 168 hours (7 days) |
| `"720h"` | 720 hours (30 days) |
| `"8760h"` | 8760 hours (365 days, 1 year) |

Fractional values are allowed: `"1.5h"`, `"0.5m"`. Units: `ns`, `us` (or `µs`), `ms`, `s`, `m`, `h`. Days, weeks, and months are not supported directly — use `"168h"` for 7 days, `"720h"` for 30 days.

---

## SEE ALSO

`tierfs(1)`, `docs/OPERATIONS.md`, `docs/ARCHITECTURE.md`, [TOML specification](https://toml.io/en/v1.0.0)
