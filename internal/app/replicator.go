// Package app contains the application-layer services: TierService, Replicator,
// Evictor, and Stager. These coordinate the domain ports without depending on
// any specific adapter implementation.
package app

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/digest"
	"github.com/mikey-austin/tierfs/internal/domain"
)

// CopyJob describes a single file replication operation between two tiers.
type CopyJob struct {
	RelPath  string
	FromTier string // source tier name
	ToTier   string // destination tier name
	Retries  int    // current retry count
}

// ReplicatorConfig holds settings for the replication worker pool.
type ReplicatorConfig struct {
	Workers       int
	MaxRetries    int
	RetryInterval time.Duration
	Verify        string      // "none" | "size" | "digest"
	WriteGuard    *WriteGuard // nil = no guard (for tests that don't need it)
}

// TierLookup resolves a tier name to its Backend. Implemented by TierService.
type TierLookup interface {
	BackendFor(tierName string) (domain.Backend, error)
}

// Replicator manages an async worker pool that copies files between tiers,
// then verifies the copy and updates the metadata store.
type Replicator struct {
	cfg   ReplicatorConfig
	meta  domain.MetadataStore
	tiers TierLookup
	log   *zap.Logger
	queue chan CopyJob
	wg    sync.WaitGroup
	stop  chan struct{}

	// Metrics (read with atomic, written under the worker goroutines).
	totalCopied atomic.Int64
	totalFailed atomic.Int64
	queueDepth  atomic.Int64
}

// NewReplicator creates a Replicator. Call Start() to begin processing.
func NewReplicator(cfg ReplicatorConfig, meta domain.MetadataStore, tiers TierLookup, log *zap.Logger) *Replicator {
	return &Replicator{
		cfg:   cfg,
		meta:  meta,
		tiers: tiers,
		log:   log.Named("replicator"),
		queue: make(chan CopyJob, 4096),
		stop:  make(chan struct{}),
	}
}

// Start launches the worker pool.
func (r *Replicator) Start() {
	for i := 0; i < r.cfg.Workers; i++ {
		r.wg.Add(1)
		go r.worker(i)
	}
}

// Stop drains the queue and shuts down all workers, waiting for completion.
func (r *Replicator) Stop() {
	close(r.stop)
	r.wg.Wait()
}

// Enqueue adds a job to the copy queue. Non-blocking; drops if queue is full
// and logs a warning. Callers that need guaranteed delivery should retry.
func (r *Replicator) Enqueue(job CopyJob) {
	select {
	case r.queue <- job:
		r.queueDepth.Add(1)
	default:
		r.log.Warn("replication queue full, dropping job",
			zap.String("rel_path", job.RelPath),
			zap.String("from", job.FromTier),
			zap.String("to", job.ToTier),
		)
	}
}

// Metrics returns a snapshot of replicator counters.
func (r *Replicator) Metrics() (copied, failed, depth int64) {
	return r.totalCopied.Load(), r.totalFailed.Load(), r.queueDepth.Load()
}

func (r *Replicator) worker(id int) {
	defer r.wg.Done()
	log := r.log.With(zap.Int("worker", id))
	for {
		select {
		case <-r.stop:
			return
		case job, ok := <-r.queue:
			if !ok {
				return
			}
			r.queueDepth.Add(-1)
			if err := r.process(log, job); err != nil {
				r.totalFailed.Add(1)
				if job.Retries < r.cfg.MaxRetries {
					job.Retries++
					log.Warn("copy failed, scheduling retry",
						zap.String("rel_path", job.RelPath),
						zap.Int("retry", job.Retries),
						zap.Error(err),
					)
					go func(j CopyJob) {
						select {
						case <-r.stop:
						case <-time.After(r.cfg.RetryInterval):
							r.Enqueue(j)
						}
					}(job)
				} else {
					log.Error("copy permanently failed after max retries",
						zap.String("rel_path", job.RelPath),
						zap.Error(err),
					)
				}
			} else {
				r.totalCopied.Add(1)
			}
		}
	}
}

func (r *Replicator) process(log *zap.Logger, job CopyJob) error {
	ctx := context.Background()

	// ── Write-guard check ────────────────────────────────────────────────────
	// Refuse to start the copy if the source file has an open write handle or
	// is still within its quiescence window. Re-enqueue for later rather than
	// copying a potentially incomplete file.
	if r.cfg.WriteGuard != nil {
		if active, reason := r.cfg.WriteGuard.IsWriteActive(job.RelPath); active {
			log.Info("delaying replication: file is write-active",
				zap.String("rel_path", job.RelPath),
				zap.String("reason", reason),
			)
			// Re-enqueue with a short delay. This uses the normal retry path
			// but does NOT consume a retry count — it's not an error.
			go func() {
				select {
				case <-r.stop:
				case <-time.After(r.cfg.RetryInterval):
					r.Enqueue(job) // job.Retries unchanged
				}
			}()
			return nil
		}
	}

	src, err := r.tiers.BackendFor(job.FromTier)
	if err != nil {
		return fmt.Errorf("resolve source backend %q: %w", job.FromTier, err)
	}
	dst, err := r.tiers.BackendFor(job.ToTier)
	if err != nil {
		return fmt.Errorf("resolve dest backend %q: %w", job.ToTier, err)
	}

	// Fetch the metadata we recorded at OnWriteComplete time.
	file, err := r.meta.GetFile(ctx, job.RelPath)
	if err != nil {
		return fmt.Errorf("get file meta: %w", err)
	}

	// ── Staleness re-check ───────────────────────────────────────────────────
	// Re-stat the source file and compare size + mtime against what metadata
	// recorded at OnWriteComplete time. If they differ the file has been
	// modified since we enqueued the job (e.g. a muxer added a subtitle track,
	// an application truncated and rewrote the file, or the FUSE open+write
	// path was used without going through Create). Abort, let OnWriteComplete
	// re-compute the digest and re-enqueue once the new write completes.
	const mtimePrecision = time.Microsecond
	if file.Digest != "" {
		liveInfo, serr := src.Stat(ctx, job.RelPath)
		if serr == nil {
			stale := liveInfo.Size != file.Size ||
				liveInfo.ModTime.Truncate(mtimePrecision) != file.ModTime.Truncate(mtimePrecision)
			if stale {
				log.Warn("aborting replication: source file changed since enqueue",
					zap.String("rel_path", job.RelPath),
					zap.Int64("meta_size", file.Size),
					zap.Int64("live_size", liveInfo.Size),
					zap.Time("meta_mtime", file.ModTime),
					zap.Time("live_mtime", liveInfo.ModTime),
				)
				// Don't count as a retry failure; the file will be re-enqueued
				// when TierService.OnWriteComplete fires for the new write.
				return nil
			}
		}
	}

	log.Info("copying file",
		zap.String("rel_path", job.RelPath),
		zap.String("from", src.URI(job.RelPath)),
		zap.String("to", dst.URI(job.RelPath)),
	)

	// Record start of replication.
	if err := r.meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   job.RelPath,
		TierName:  job.ToTier,
		ArrivedAt: time.Now(),
		Verified:  false,
	}); err != nil {
		return fmt.Errorf("add file tier: %w", err)
	}

	// Stream from source to destination.
	// When digest verification is enabled and we have an expected digest,
	// tee the bytes into a buffer so we can verify after the copy without
	// a second read pass over the destination.
	rc, size, err := src.Get(ctx, job.RelPath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}

	var (
		streamBody io.Reader = rc
		hasher     *digest.Hasher
	)
	if r.cfg.Verify == "digest" && file.Digest != "" {
		hasher = digest.NewHasher()
		streamBody = io.TeeReader(rc, hasher)
	}

	if err := dst.Put(ctx, job.RelPath, streamBody, size); err != nil {
		rc.Close()
		return fmt.Errorf("write to dest: %w", err)
	}
	rc.Close()

	// Verification.
	switch r.cfg.Verify {
	case "digest":
		if file.Digest == "" {
			break // digest not yet computed (file still writing); skip
		}
		if localPath, ok := dst.LocalPath(job.RelPath); ok {
			// Local destination: hash the written file directly. Fast and exact.
			if err := digest.Verify(localPath, file.Digest); err != nil {
				dst.Delete(ctx, job.RelPath) //nolint:errcheck
				return fmt.Errorf("digest verification: %w", err)
			}
		} else if hasher != nil {
			// Remote destination: use the digest computed inline during streaming.
			got := hasher.Sum()
			if got != file.Digest {
				dst.Delete(ctx, job.RelPath) //nolint:errcheck
				return fmt.Errorf("%w: want %s got %s", domain.ErrDigestMismatch, file.Digest, got)
			}
		}
		// If neither local path nor hashBuf available, trust the upload succeeded.

	case "size":
		fi, err := dst.Stat(ctx, job.RelPath)
		if err != nil {
			return fmt.Errorf("stat after copy: %w", err)
		}
		if fi.Size != file.Size {
			dst.Delete(ctx, job.RelPath) //nolint:errcheck
			return fmt.Errorf("size mismatch: want %d got %d", file.Size, fi.Size)
		}
	}

	// Mark verified.
	if err := r.meta.MarkTierVerified(ctx, job.RelPath, job.ToTier); err != nil {
		return fmt.Errorf("mark verified: %w", err)
	}

	// Transition file to synced state.
	file.State = domain.StateSynced
	if err := r.meta.UpsertFile(ctx, *file); err != nil {
		return fmt.Errorf("update file state: %w", err)
	}

	log.Info("copy complete",
		zap.String("rel_path", job.RelPath),
		zap.String("to", dst.URI(job.RelPath)),
	)
	return nil
}
