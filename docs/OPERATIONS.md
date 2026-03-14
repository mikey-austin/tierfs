# TierFS Operations Guide

This document covers deployment, monitoring, alerting, and troubleshooting for TierFS in production. A healthy TierFS instance has a low replication queue depth, a backend error rate near zero, and evictions that track the configured schedule.

---

## Deployment

### systemd Unit

```ini
# /etc/systemd/system/tierfs.service
[Unit]
Description=TierFS FUSE Storage Tiering Daemon
Documentation=https://github.com/mikey-austin/tierfs
After=network.target local-fs.target
Wants=network.target

[Service]
Type=simple
User=tierfs
Group=tierfs

ExecStartPre=/bin/mkdir -p /var/lib/tierfs /var/log/tierfs /tmp/tierfs-stage /share/CCTV
ExecStart=/usr/local/bin/tierfs -config /etc/tierfs/tierfs.toml
ExecStop=/bin/fusermount3 -u /share/CCTV

Restart=on-failure
RestartSec=5s
TimeoutStartSec=30s
TimeoutStopSec=60s

# FUSE requires SYS_ADMIN
CapabilityBoundingSet=CAP_SYS_ADMIN
AmbientCapabilities=CAP_SYS_ADMIN

# Allow /dev/fuse access
DevicePolicy=auto
DeviceAllow=/dev/fuse rw

# File descriptor limit for large media libraries
LimitNOFILE=65536
LimitMEMLOCK=infinity

# Do NOT use PrivateTmp — stage dir must persist across restarts
PrivateTmp=no
PrivateDevices=no
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=/var/lib/tierfs /var/log/tierfs /tmp/tierfs-stage /share/CCTV /data /mnt

Environment=AWS_ACCESS_KEY_ID=
Environment=AWS_SECRET_ACCESS_KEY=
EnvironmentFile=-/etc/tierfs/tierfs.env

StandardOutput=journal
StandardError=journal
SyslogIdentifier=tierfs

[Install]
WantedBy=multi-user.target
```

```bash
# Install and start
sudo useradd -r -s /bin/false tierfs
sudo systemctl daemon-reload
sudo systemctl enable --now tierfs
sudo systemctl status tierfs
```

### Docker

```bash
docker run -d \
  --name tierfs \
  --restart unless-stopped \
  --cap-add SYS_ADMIN \
  --device /dev/fuse \
  --security-opt apparmor:unconfined \
  -v /etc/tierfs/tierfs.toml:/etc/tierfs/tierfs.toml:ro \
  -v /data/ssd/cctv:/data/tier0 \
  -v /mnt/nas/cctv:/data/tier1 \
  -v /var/lib/tierfs:/var/lib/tierfs \
  -v /var/log/tierfs:/var/log/tierfs \
  -v /share/CCTV:/share/CCTV:shared \
  -p 9100:9100 \
  -e AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID} \
  -e AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY} \
  ghcr.io/mikey-austin/tierfs:latest
```

### docker-compose (with Frigate NVR)

```yaml
# docker-compose.yml
version: "3.9"

services:
  tierfs:
    image: ghcr.io/mikey-austin/tierfs:latest
    restart: unless-stopped
    cap_add: [SYS_ADMIN]
    devices: [/dev/fuse:/dev/fuse]
    security_opt: [apparmor:unconfined]
    volumes:
      - ./tierfs.toml:/etc/tierfs/tierfs.toml:ro
      - /data/ssd/cctv:/data/tier0
      - /mnt/nas/cctv:/data/tier1
      - tierfs-meta:/var/lib/tierfs
      - tierfs-stage:/tmp/tierfs-stage
      - cctv-mount:/share/CCTV:shared
    ports: ["9100:9100"]
    environment:
      - AWS_ACCESS_KEY_ID
      - AWS_SECRET_ACCESS_KEY
    healthcheck:
      test: ["CMD", "curl", "-sf", "http://localhost:9100/healthz"]
      interval: 30s
      timeout: 5s
      retries: 3

  frigate:
    image: ghcr.io/blakeblackshear/frigate:stable
    restart: unless-stopped
    volumes:
      - /dev/bus/usb:/dev/bus/usb
      - ./frigate.yml:/config/config.yml:ro
      - cctv-mount:/media/frigate   # same shared mount
    depends_on:
      tierfs:
        condition: service_healthy

volumes:
  tierfs-meta:
  tierfs-stage:
  cctv-mount:
    driver_opts:
      type: none
      o: bind
      device: /share/CCTV
```

---

## Health Checks

```bash
# Liveness check
curl -sf http://localhost:9100/healthz && echo "healthy"

# Returns: 200 OK, body: "ok"
```

**Healthy** — `/healthz` returns 200, `tierfs_replication_queue_depth` < 100, no `error`-level log entries in the last 5 minutes, `tierfs_backend_operations_total{outcome="error"}` rate near zero.

**Degraded** — `/healthz` returns 200 (process alive) but queue depth growing, or backend error rate elevated. Check `tierfs_replication_jobs_total{outcome="error"}` and logs.

**Down** — Process crashed or FUSE mount point is stale. The `up` metric will be 0 in Prometheus. Run `mountpoint /share/CCTV` to verify the FUSE mount is active. If the mount is stale, `fusermount3 -uz /share/CCTV` and restart.

**Liveness vs Readiness**: The `/healthz` endpoint is a liveness probe only — it indicates the HTTP server is up, not that all backends are reachable. For a readiness probe, verify that at least one backend is reachable by checking `tierfs_backend_operations_total{outcome="ok"}` is non-zero.

---

## Prometheus Metrics Reference

| Metric | Type | Labels | Description | Typical Range |
|---|---|---|---|---|
| `tierfs_backend_operations_total` | Counter | `backend`, `op`, `outcome` | All backend calls | Growing |
| `tierfs_backend_operation_duration_seconds` | Histogram | `backend`, `op` | Per-call latency | p99 < 500ms local; < 5s S3 |
| `tierfs_backend_bytes_read_total` | Counter | `backend` | Bytes returned by Get() | Growing |
| `tierfs_backend_bytes_written_total` | Counter | `backend` | Bytes sent to Put() | Growing |
| `tierfs_meta_operations_total` | Counter | `op`, `outcome` | SQLite call counts | Growing |
| `tierfs_meta_operation_duration_seconds` | Histogram | `op` | SQLite latency | p99 < 5ms |
| `tierfs_replication_queue_depth` | Gauge | — | Pending CopyJobs | 0–50 typical |
| `tierfs_replication_jobs_total` | Counter | `from_tier`, `to_tier`, `outcome` | Copy completions | Growing |
| `tierfs_replication_job_duration_seconds` | Histogram | `from_tier`, `to_tier` | End-to-end copy time | 1s–60s per GiB |
| `tierfs_replication_bytes_transferred_total` | Counter | `from_tier`, `to_tier` | Bytes copied | Growing |
| `tierfs_eviction_events_total` | Counter | `from_tier` | Files deleted from tier | Grows with age of data |
| `tierfs_fuse_operations_total` | Counter | `op`, `outcome` | FUSE syscall counts | Growing |
| `tierfs_fuse_operation_duration_seconds` | Histogram | `op` | FUSE syscall latency | p99 < 10ms |
| `tierfs_fuse_staged_bytes_total` | Counter | — | Bytes fetched for cold reads | Low if promote_on_read enabled |
| `tierfs_tier_file_count` | Gauge | `tier` | Files currently on each tier | Decreases as files evict |
| `tierfs_tier_bytes_used` | Gauge | `tier` | Total bytes (from metadata) | Stays below capacity |

---

## Key PromQL Expressions

### 1. Replication Success Rate

```promql
rate(tierfs_replication_jobs_total{outcome="ok"}[5m])
/
rate(tierfs_replication_jobs_total[5m])
```

Should be ≥ 0.99. Below 0.95 is cause for investigation. Below 0.80 indicates a persistent backend problem.

### 2. Backend Error Rate by Tier

```promql
sum by (backend, op) (
  rate(tierfs_backend_operations_total{outcome="error"}[5m])
)
/
sum by (backend, op) (
  rate(tierfs_backend_operations_total[5m])
)
```

Alert if > 0.01 (1%) for any tier/op combination. A spike on `op="put"` for an S3 backend often indicates credential expiry or rate limiting.

### 3. Replication Queue Backlog Trend

```promql
# Is the queue depth growing or shrinking?
deriv(tierfs_replication_queue_depth[10m])
```

Positive and increasing means more files are being enqueued than workers can process. Increase `workers` or investigate the destination backend for slowness.

### 4. Read Tier Distribution (Cold Read Fraction)

```promql
rate(tierfs_fuse_staged_bytes_total[5m])
/
(
  rate(tierfs_backend_bytes_read_total{backend="tier0"}[5m])
  + rate(tierfs_fuse_staged_bytes_total[5m])
)
```

Measures what fraction of read bytes came from cold staging (S3/remote backend) vs hot local tier. If this is > 0.1 (10%), consider enabling `promote_on_read` for the affected rule.

### 5. Copy Throughput by Tier Pair

```promql
rate(tierfs_replication_bytes_transferred_total[5m])
```

Label `from_tier` and `to_tier`. Expected throughput depends on backend: NFS ≈ 100–500 MB/s, MinIO LAN ≈ 50–200 MB/s, Backblaze ≈ 10–50 MB/s (upload limited).

### 6. FUSE Operation Latency p99

```promql
histogram_quantile(0.99,
  sum by (op, le) (
    rate(tierfs_fuse_operation_duration_seconds_bucket[5m])
  )
)
```

Alert if `op="Create"` or `op="Open"` p99 exceeds 500ms. Latency spikes on `Create` usually indicate tier0 backend slowness; on `Open` they indicate staging (cold read) is occurring.

### 7. Eviction Rate

```promql
sum by (from_tier) (
  rate(tierfs_eviction_events_total[1h])
) * 3600
```

Events per hour. Should track predictably with the volume of data written 24h/7d/30d ago. A sudden drop to zero with growing `tier_file_count` indicates the evictor is stalling.

### 8. Staged Bytes Rate (Cold Read Indicator)

```promql
rate(tierfs_fuse_staged_bytes_total[5m]) / 1e6
```

MB/s being staged from cold backends. Values > 100 MB/s suggest heavy cold reads are competing with replication workers for backend bandwidth. Consider enabling `promote_on_read`.

---

## Grafana Dashboard

### Panel 1: Tier Storage Usage

- **Type:** Stat (one stat per tier)
- **PromQL:** `tierfs_tier_bytes_used`
- **Unit:** bytes (auto)
- **Thresholds:** 70% of capacity = yellow, 85% = red
- **Description:** Current bytes on each tier from metadata. Compare to `df` output on actual filesystem.

### Panel 2: Replication Queue Depth

- **Type:** Time series
- **PromQL:** `tierfs_replication_queue_depth`
- **Unit:** short
- **Description:** Pending copy jobs. Should trend toward zero after write bursts. Sustained high values indicate worker/backend issues.

### Panel 3: Backend Operation Latency p99

- **Type:** Time series
- **PromQL:** `histogram_quantile(0.99, sum by (backend, le) (rate(tierfs_backend_operation_duration_seconds_bucket[5m])))`
- **Unit:** seconds
- **Description:** Per-tier p99 latency for all backend operations. Spikes on S3 tiers during business hours suggest egress throttling.

### Panel 4: Replication Throughput

- **Type:** Time series
- **PromQL:** `sum by (from_tier, to_tier) (rate(tierfs_replication_bytes_transferred_total[5m]))`
- **Unit:** bytes/sec
- **Description:** Copy throughput per tier pair. Use to verify that workers are fully utilising available bandwidth.

### Panel 5: FUSE Operation Rate

- **Type:** Time series
- **PromQL:** `sum by (op) (rate(tierfs_fuse_operations_total[1m]))`
- **Unit:** ops/sec
- **Description:** Frigate/Immich activity visible through the FUSE layer. `Create` rate ≈ camera event rate; `Open` rate ≈ review/playback activity.

### Panel 6: Eviction Rate by Tier

- **Type:** Bar chart (last 1h)
- **PromQL:** `sum by (from_tier) (increase(tierfs_eviction_events_total[1h]))`
- **Unit:** files/hour
- **Description:** Files evicted from each tier per hour. Should be non-zero if data is accumulating past the eviction age.

---

## Alerting Rules

```yaml
# prometheus/tierfs_alerts.yml
groups:
  - name: tierfs
    rules:

      - alert: TierfsReplicationQueueHigh
        expr: tierfs_replication_queue_depth > 500
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "TierFS replication queue depth is high"
          description: >
            The replication queue has {{ $value }} pending jobs for more than 10 minutes.
            This may indicate a slow or unreachable destination backend.
            Check tierfs_backend_operations_total{outcome="error"} for the destination tier.

      - alert: TierfsReplicationErrorRateHigh
        expr: |
          (
            rate(tierfs_replication_jobs_total{outcome="error"}[5m])
            /
            rate(tierfs_replication_jobs_total[5m])
          ) > 0.05
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "TierFS replication error rate is above 5%"
          description: >
            {{ $value | humanizePercentage }} of replication jobs are failing.
            Files on tier0 will not be replicated; eviction is blocked.
            Check logs for the failing tier pair: from_tier={{ $labels.from_tier }}.

      - alert: TierfsBackendErrorRateHigh
        expr: |
          (
            sum by (backend, op) (rate(tierfs_backend_operations_total{outcome="error"}[5m]))
            /
            sum by (backend, op) (rate(tierfs_backend_operations_total[5m]))
          ) > 0.01
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "TierFS backend {{ $labels.backend }} op={{ $labels.op }} error rate > 1%"
          description: >
            Backend {{ $labels.backend }} is returning errors on {{ $labels.op }} operations.
            Possible causes: disk full, NFS mount gone, S3 credential expiry, network partition.

      - alert: TierfsMetaStoreLatencyHigh
        expr: |
          histogram_quantile(0.99,
            sum by (le) (rate(tierfs_meta_operation_duration_seconds_bucket[5m]))
          ) > 0.1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "TierFS SQLite metadata store p99 latency > 100ms"
          description: >
            SQLite writes are taking {{ $value }}s at p99. This may indicate disk I/O
            saturation on the metadata disk, or a very large metadata database.
            Check iostat on the meta_db filesystem.

      - alert: TierfsFuseOpLatencyHigh
        expr: |
          histogram_quantile(0.99,
            sum by (op, le) (rate(tierfs_fuse_operation_duration_seconds_bucket[5m]))
          ) > 0.5
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "TierFS FUSE {{ $labels.op }} p99 latency > 500ms"
          description: >
            FUSE {{ $labels.op }} operations are taking {{ $value }}s at p99.
            This directly impacts application performance. For Open: cold staging is slow.
            For Create: tier0 backend is slow.

      - alert: TierfsDown
        expr: up{job="tierfs"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "TierFS process is down"
          description: >
            The TierFS metrics endpoint is unreachable. The process may have crashed
            or the FUSE mount may be stale. Check systemctl status tierfs and
            mountpoint /share/CCTV.

      - alert: TierfsStageBytesHigh
        expr: rate(tierfs_fuse_staged_bytes_total[15m]) > 100e6
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "TierFS cold staging rate > 100 MB/s"
          description: >
            Files are being staged from cold backends at {{ $value | humanize }}B/s.
            This competes with replication workers for backend bandwidth and may slow
            down hot-tier writes. Consider enabling promote_on_read for frequently
            re-accessed file patterns.

      - alert: TierfsEvictionStalled
        expr: |
          (
            increase(tierfs_tier_file_count{tier="tier0"}[1h]) > 100
          ) and (
            increase(tierfs_eviction_events_total{from_tier="tier0"}[1h]) == 0
          )
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: "TierFS eviction from tier0 appears stalled"
          description: >
            tier0 file count is growing but no evictions have occurred in 1 hour.
            This may indicate that replication is not completing (no verified copies
            on cold tiers), or the evictor loop is stuck. Check replication queue depth
            and tierfs_replication_jobs_total{outcome="ok"}.
```

---

## Log Reference

### Log Levels in TierFS Context

| Level | When emitted | Frequency |
|---|---|---|
| `debug` | Every FUSE syscall entry/exit, every SQLite query, every replication step | Very high; use only for targeted debugging |
| `info` | Mount/unmount, successful copies, evictions, capacity pressure events, startup config summary | Normal production level |
| `warn` | Retried replication failures (attempt < max_retries), missing expected file on tier, slow backend (>5s), staged read fallback | Occasional; warrants attention if sustained |
| `error` | Exhausted retries, FUSE errors returned to caller, metadata inconsistencies, backend auth failures, digest mismatch | Should be near zero in steady state; each one needs investigation |

### Structured Field Reference

| Field | Type | When Present | Meaning |
|---|---|---|---|
| `ts` | string | Always | RFC3339Nano timestamp |
| `level` | string | Always | Log level |
| `logger` | string | Always | Component path, e.g. `tier-service.replicator` |
| `msg` | string | Always | Human-readable message |
| `rel_path` | string | File operations | Relative path within the FUSE mount |
| `tier` | string | Eviction, capacity | Tier name being operated on |
| `from_tier` | string | Replication, eviction | Source tier name |
| `to_tier` | string | Replication | Destination tier name |
| `from` | string | Replication | Full source URI |
| `to` | string | Replication | Full destination URI |
| `attempt` | int | Replication | Retry attempt number (1-based) |
| `size` | int64 | File operations | File size in bytes |
| `dur` | string | Backend decorators | Operation duration |
| `bytes` | int64 | Backend decorators | Bytes transferred |
| `error` | string | Error log lines | Error message from wrapped error |
| `digest` | string | Verify failure | Expected vs actual digest |
| `backend` | string | Backend operations | Backend name |
| `op` | string | Backend/meta decorators | Operation name |

### Useful `jq` Queries

```bash
# All errors in the last run
journalctl -u tierfs --no-pager | jq 'select(.level == "error")'

# Replication failures for a specific file
journalctl -u tierfs --no-pager | jq 'select(.rel_path == "recordings/cam1/clip.mp4")'

# Files that hit max retries (dropped jobs)
journalctl -u tierfs --no-pager | jq 'select(.msg | contains("max retries"))'

# Slowest backend operations (> 5 seconds)
journalctl -u tierfs --no-pager \
  | jq 'select(.dur != null) | .dur as $d | select(($d | rtrimstr("s") | tonumber) > 5)'

# Eviction events in the last hour
journalctl -u tierfs --since "1 hour ago" --no-pager \
  | jq 'select(.msg == "evicted file from tier") | {ts, rel_path, from_tier}'

# Digest mismatch events (data integrity failures)
journalctl -u tierfs --no-pager | jq 'select(.msg | contains("digest")) | select(.level == "error")'
```

---

## Troubleshooting

### Files not replicating to tier1

**Symptoms:** `tierfs_replication_queue_depth` growing; tier1 backend has no new files; `tierfs_replication_jobs_total{outcome="ok"}` not increasing.

**Diagnosis:**
```bash
# Check queue depth
curl -s http://localhost:9100/metrics | grep replication_queue_depth
# Check for errors
journalctl -u tierfs --since "1 hour ago" | jq 'select(.level == "error")'
```

**Resolution:** Look for `error`-level log entries from `tier-service.replicator`. Common causes: tier1 backend mount not present, NFS disconnect, wrong `uri` in config. For S3: check credential env vars, verify bucket exists, check endpoint URL and `path_style` setting.

---

### Replication queue growing unbounded

**Symptoms:** `tierfs_replication_queue_depth` increasing monotonically; application writes are fast but cold tier receives nothing.

**Diagnosis:** Compare `rate(tierfs_replication_jobs_total[5m])` to write rate. If jobs are being completed but queue still grows, writes are outpacing workers.

**Resolution:** Increase `[replication] workers`. As a rule of thumb, set workers = min(64, expected_concurrent_files × 2). If the destination is an S3 backend, network bandwidth is likely the bottleneck — check `tierfs_replication_bytes_transferred_total` rate.

---

### S3 authentication failures (MinIO/Backblaze)

**Symptoms:** `tierfs_backend_operations_total{backend="tier2",outcome="error"}` increasing; logs show `SignatureDoesNotMatch` or `InvalidAccessKeyId`.

**Diagnosis:**
```bash
journalctl -u tierfs | jq 'select(.error | contains("SignatureDoesNotMatch", "InvalidAccessKeyId", "403"))'
```

**Resolution:**
- Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables are set correctly.
- For MinIO: confirm `path_style = true` and the endpoint URL includes the port.
- For Backblaze B2: the `endpoint` must match the region shown in the B2 console; use the `keyID` as `access_key` and `applicationKey` as `secret_key`.
- Test credentials independently: `aws s3 ls s3://bucket --endpoint-url https://... --no-sign-request` (replace with actual creds).

---

### FUSE mount fails to start

**Symptoms:** TierFS exits at startup; logs show `mount: failed` or `permission denied` on `/dev/fuse`.

**Diagnosis:**
```bash
journalctl -u tierfs | head -20
ls -la /dev/fuse
cat /proc/filesystems | grep fuse
```

**Resolution:**
- Verify `/dev/fuse` exists and is readable: `ls -la /dev/fuse` should show `crw-rw-rw-`.
- Verify `fusermount3` is installed: `which fusermount3`.
- For non-root: add `user_allow_other` to `/etc/fuse.conf` OR add the tierfs user to the `fuse` group.
- For Docker: `--cap-add SYS_ADMIN --device /dev/fuse --security-opt apparmor:unconfined` are all required.
- Check the mount point exists and is empty: `mountpoint /share/CCTV` should return "not a mountpoint".

---

### SQLite "database is locked" errors

**Symptoms:** `error`-level log lines with `database is locked`; metadata operations fail intermittently.

**Diagnosis:**
```bash
journalctl -u tierfs | jq 'select(.error | contains("database is locked"))'
lsof /var/lib/tierfs/meta.db
```

**Resolution:** Only one TierFS process should open the database. Multiple instances sharing the same `meta_db` path will conflict. If running multiple FUSE mounts, each must have a separate `meta_db`. Verify no zombie processes are holding the database: `fuser /var/lib/tierfs/meta.db`.

---

### Files evicted too early

**Symptoms:** Files disappear from tier0 before the configured `evict_schedule` age.

**Diagnosis:**
```bash
# Check when a specific file arrived on tier0
journalctl -u tierfs | jq 'select(.rel_path == "recordings/cam1/clip.mp4" and .msg | contains("arrived"))'
# Check tier capacity pressure
curl -s http://localhost:9100/metrics | grep 'tier_bytes_used\|tier_file_count'
```

**Resolution:** Capacity pressure eviction ignores the age schedule and evicts oldest files when `used/capacity > capacity_threshold`. Either increase the tier's `capacity`, add more storage, or increase `capacity_threshold`. Check that `tierfs_tier_bytes_used{tier="tier0"}` is approaching the configured capacity.

---

### Pin-tier files still being moved

**Symptoms:** Files in `exports/**` (or other pinned paths) are disappearing from tier0.

**Diagnosis:** Check which rule matches the file:
```bash
# The evictor logs the matched rule name; look for the file
journalctl -u tierfs | jq 'select(.rel_path | contains("exports/")) | select(.msg | contains("evict"))'
```

**Resolution:** Ensure the `[[rule]]` with `pin_tier` appears **before** any more general rules in the config file. Rules are first-match; a `match = "**"` rule before the pinned rule will take precedence.

---

### High staged_bytes (excessive cold reads)

**Symptoms:** `tierfs_fuse_staged_bytes_total` growing fast; application reads are slow; users notice buffering during video playback.

**Diagnosis:**
```bash
curl -s http://localhost:9100/metrics | grep staged_bytes
# Find which files are being staged (look for "staging file" info logs)
journalctl -u tierfs --since "1 hour ago" | jq 'select(.msg == "staging remote file") | .rel_path' | sort | uniq -c | sort -rn | head -20
```

**Resolution:** Enable `promote_on_read = "tier0"` on the rule matching the frequently accessed files. This brings the file back to the hot tier after first access, eliminating staging on subsequent reads.

---

### Metadata and filesystem diverged after crash

**Symptoms:** After an unclean shutdown, some files are present on tier0 but not in metadata (or vice versa).

**Diagnosis:**
```bash
# List files in tier0 filesystem
find /data/tier0 -type f | sort > /tmp/fs-files.txt
# List files in metadata (requires sqlite3)
sqlite3 /var/lib/tierfs/meta.db "SELECT rel_path FROM files WHERE current_tier='tier0'" | sort > /tmp/meta-files.txt
diff /tmp/fs-files.txt /tmp/meta-files.txt
```

**Resolution:** Files in the filesystem but not metadata: they were written during the window between `writeHandle.Release()` and `OnWriteComplete()`. Re-run TierFS — it will attempt to re-process the file if the write handle is opened again. For truly orphaned files, manually insert into `meta.db`:
```sql
INSERT OR IGNORE INTO files(rel_path, current_tier, state, size, mod_time)
VALUES ('recordings/cam1/orphan.mp4', 'tier0', 'local', 524288000, strftime('%s','now')*1000000000);
```
Files in metadata but not filesystem (truly lost): delete from metadata:
```sql
DELETE FROM files WHERE rel_path = 'recordings/cam1/lost.mp4';
DELETE FROM file_tiers WHERE rel_path = 'recordings/cam1/lost.mp4';
```

---

### Slow FUSE write throughput

**Symptoms:** Frigate/application write throughput is lower than expected; `tierfs_fuse_operation_duration_seconds{op="Create"}` p99 is high.

**Diagnosis:**
```bash
# Check tier0 backend write latency
curl -s http://localhost:9100/metrics | grep 'backend_operation_duration.*tier0.*put'
# Check if replication is consuming tier0 read bandwidth
curl -s http://localhost:9100/metrics | grep 'replication_bytes_transferred'
```

**Resolution:**
1. Verify tier0 is a local file backend — writes should be near raw disk speed.
2. Check that the tier0 filesystem is not full (`df -h /data/tier0`).
3. If replication is running many concurrent reads of tier0 during Frigate's write window, reduce `workers` or investigate whether NFS read bandwidth to tier0 is saturated.
4. Ensure `stage_dir` is on a different filesystem from tier0 to avoid I/O contention during cold reads.

---

## Backup and Recovery

### Backing Up meta.db

The SQLite database is the only stateful component besides the actual file data. Back it up with:

```bash
# Hot backup (works while TierFS is running — WAL mode is safe)
sqlite3 /var/lib/tierfs/meta.db ".backup /backup/tierfs-meta-$(date +%Y%m%d).db"

# Verify backup
sqlite3 /backup/tierfs-meta-$(date +%Y%m%d).db "PRAGMA integrity_check;"
```

For automated backups, run this via cron or a systemd timer every hour. The database is typically 50–200 MB for millions of files.

### Clean Restart

On a clean shutdown (`SIGTERM`), TierFS:
1. Stops accepting new FUSE operations (unmounts FUSE).
2. Waits for in-flight FUSE operations to complete.
3. Stops the replication and eviction workers (waits for current jobs to finish).
4. Closes the SQLite database (writes WAL checkpoint).

A clean restart requires no recovery — metadata is consistent.

### Crash Recovery

SQLite WAL mode provides atomic crash recovery. On restart after a crash, SQLite automatically rolls back any uncommitted transaction. Files that were mid-write at crash time will appear in `StateWriting` in metadata; they should be considered corrupted. Re-trigger a write from the application to replace them.

### Re-scanning Filesystem into Metadata

If `meta.db` is lost entirely but file data remains on the backends, rebuild metadata by scanning the tier0 filesystem:

```bash
# This is a one-time recovery script pattern (adapt as needed)
find /data/tier0 -type f -printf "%P\n" | while read relpath; do
  size=$(stat -c%s "/data/tier0/$relpath")
  mtime=$(stat -c%Y "/data/tier0/$relpath")
  sqlite3 /var/lib/tierfs/meta.db \
    "INSERT OR IGNORE INTO files(rel_path,current_tier,state,size,mod_time)
     VALUES('$relpath','tier0','local',$size,${mtime}000000000);"
done
```

Then restart TierFS — it will re-enqueue all `StateLocal` files for replication.

---

## Upgrade Procedure

1. **Drain the replication queue**: Wait for `tierfs_replication_queue_depth` to reach 0, or set `[replication] workers = 0` and restart to pause workers while letting in-flight jobs finish.
2. **Graceful stop**: `systemctl stop tierfs` — waits up to `TimeoutStopSec=60s` for clean shutdown.
3. **Backup metadata**: `sqlite3 /var/lib/tierfs/meta.db ".backup /backup/pre-upgrade-meta.db"`
4. **Install new binary**: Replace `/usr/local/bin/tierfs`.
5. **Check release notes**: If the schema version changed, TierFS will run migrations automatically on first start. Check the release notes for any manual steps.
6. **Start**: `systemctl start tierfs`
7. **Verify**: Check `/healthz`, watch `tierfs_replication_queue_depth`, confirm no `error`-level log entries.
8. **Monitor for 30 minutes**: Watch eviction and replication metrics to confirm normal operation.
