package app

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/digest"
	"github.com/mikey-austin/tierfs/internal/domain"
)

// TierService is the central application service. It owns the tier registry,
// implements TierLookup and TierCapacity for Replicator and Evictor, and
// exposes the file-level operations used by the FUSE layer.
type TierService struct {
	cfg          *config.Resolved
	meta         domain.MetadataStore
	backends     map[string]domain.Backend // tier name → wrapped backend
	replicator   *Replicator
	evictor      *Evictor
	guard        *WriteGuard
	log          *zap.Logger
	sweepStop    chan struct{}
	sweepDone    chan struct{}
	stager       *Stager
	stageTTL     time.Duration
	promoteGroup singleflight.Group
}

// NewTierService constructs TierService and registers all backends.
// backends must be keyed by tier name (matching cfg.Tiers[*].Name).
func NewTierService(
	cfg *config.Resolved,
	meta domain.MetadataStore,
	backends map[string]domain.Backend,
	stager *Stager,
	stageTTL time.Duration,
	log *zap.Logger,
) *TierService {
	ts := &TierService{
		cfg:       cfg,
		meta:      meta,
		backends:  backends,
		guard:     NewWriteGuard(cfg.Replication.WriteQuiescence),
		log:       log.Named("tier-service"),
		sweepStop: make(chan struct{}),
		sweepDone: make(chan struct{}),
		stager:    stager,
		stageTTL:  stageTTL,
	}

	replCfg := ReplicatorConfig{
		Workers:        cfg.Replication.Workers,
		MaxRetries:     cfg.Replication.MaxRetries,
		RetryInterval:  cfg.Replication.RetryInterval,
		Verify:         cfg.Replication.Verify,
		BackendTimeout: cfg.Replication.BackendTimeout,
		WriteGuard:     ts.guard,
	}
	ts.replicator = NewReplicator(replCfg, meta, ts, log)

	tierNames := make([]string, len(cfg.Tiers))
	for i, t := range cfg.Tiers {
		tierNames[i] = t.Name
	}
	evictCfg := EvictorConfig{
		CheckInterval:     cfg.Eviction.CheckInterval,
		CapacityThreshold: cfg.Eviction.CapacityThreshold,
		CapacityHeadroom:  cfg.Eviction.CapacityHeadroom,
		TierNames:         tierNames,
	}
	ts.evictor = NewEvictor(evictCfg, meta, cfg.Policy, ts.replicator, ts, ts, log)

	return ts
}

// Start begins background replication and eviction workers.
func (ts *TierService) Start() {
	ts.replicator.Start()
	ts.evictor.Start()
	ts.log.Info("tier service started",
		zap.Int("tiers", len(ts.cfg.Tiers)),
		zap.Int("replication_workers", ts.cfg.Replication.Workers),
	)

	// Re-queue any files that were awaiting replication before last shutdown.
	go ts.requeuePending()
	go ts.sweepLoop()
}

// Stop drains workers and shuts down all background goroutines.
func (ts *TierService) Stop() {
	close(ts.sweepStop)
	<-ts.sweepDone
	ts.evictor.Stop()
	ts.replicator.Stop()
	ts.log.Info("tier service stopped")
}

// ── TierLookup (used by Replicator and Evictor) ──────────────────────────────

// BackendFor returns the Backend for the named tier.
func (ts *TierService) BackendFor(tierName string) (domain.Backend, error) {
	b, ok := ts.backends[tierName]
	if !ok {
		return nil, fmt.Errorf("%w: %q", domain.ErrTierNotFound, tierName)
	}
	return b, nil
}

// TierNames returns the names of all configured tiers.
func (ts *TierService) TierNames() []string {
	names := make([]string, len(ts.cfg.Tiers))
	for i, t := range ts.cfg.Tiers {
		names[i] = t.Name
	}
	return names
}

// ── TierCapacity (used by Evictor) ───────────────────────────────────────────

// UsedBytes sums the sizes of all files currently on tierName from metadata.
func (ts *TierService) UsedBytes(ctx context.Context, tierName string) (int64, error) {
	files, err := ts.meta.FilesOnTier(ctx, tierName)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, f := range files {
		total += f.Size
	}
	return total, nil
}

// CapacityBytes returns the configured capacity ceiling for a tier.
func (ts *TierService) CapacityBytes(tierName string) (int64, bool) {
	t, ok := ts.cfg.TiersByName[tierName]
	if !ok {
		return 0, false
	}
	if t.Capacity.Unlimited {
		return 0, true
	}
	return t.Capacity.Bytes, false
}

// ── File operations (called by the FUSE layer) ───────────────────────────────

// WriteTarget returns the backend and tier name for a new file write.
// If the matching rule has a PinTier, that tier is used; otherwise tier0.
func (ts *TierService) WriteTarget(relPath string) (domain.Backend, string, error) {
	rule, err := ts.cfg.Policy.Match(relPath)
	if err != nil {
		// Fall back to hottest tier on no rule.
		hot := ts.cfg.HottestTier()
		b, berr := ts.BackendFor(hot.Name)
		return b, hot.Name, berr
	}
	tierName := ts.cfg.HottestTier().Name
	if rule.PinTier != "" {
		tierName = rule.PinTier
	}
	b, err := ts.BackendFor(tierName)
	return b, tierName, err
}

// ReadTarget returns the backend and local path (if any) for reading relPath.
// If the file is on a remote tier (no local path), the backend is returned for streaming.
func (ts *TierService) ReadTarget(ctx context.Context, relPath string) (domain.Backend, string, domain.File, error) {
	f, err := ts.meta.GetFile(ctx, relPath)
	if err != nil {
		return nil, "", domain.File{}, err
	}
	b, err := ts.BackendFor(f.CurrentTier)
	if err != nil {
		return nil, "", *f, err
	}
	localPath, _ := b.LocalPath(relPath)

	// Promote on read if the rule requests it.
	rule, rerr := ts.cfg.Policy.Match(relPath)
	if rerr == nil && rule.PromoteOnRead.Enabled {
		target := rule.PromoteOnRead.TargetTier
		if target == "" {
			target = ts.cfg.HottestTier().Name
		}
		if target != f.CurrentTier {
			ts.evictor.PromoteForRead(*f, target)
		}
	}

	return b, localPath, *f, nil
}

// PromoteToHot copies a file from its current cold tier to the hottest tier
// synchronously and updates metadata. It is called by the FUSE layer when a
// file is opened for writing and is not already on the hot tier.
//
// On success the file is on tier0, metadata reflects the new location, and
// the caller may open the tier0 copy as a writeHandle. The cold-tier copy is
// left in place; the normal eviction path will clean it up once tier0 is
// re-replicated.
//
// Returns the tier0 Backend and the absolute local path of the promoted file.
func (ts *TierService) PromoteToHot(ctx context.Context, relPath string) (domain.Backend, string, error) {
	f, err := ts.meta.GetFile(ctx, relPath)
	if err != nil {
		return nil, "", fmt.Errorf("promote: get metadata: %w", err)
	}

	hotTier := ts.cfg.HottestTier()
	if f.CurrentTier == hotTier.Name {
		// Already hot — nothing to do; caller should proceed normally.
		b, berr := ts.BackendFor(hotTier.Name)
		if berr != nil {
			return nil, "", berr
		}
		localPath, _ := b.LocalPath(relPath)
		return b, localPath, nil
	}

	// Deduplicate concurrent promotes for the same file.
	type promoteResult struct {
		backend   domain.Backend
		localPath string
	}
	result, err, _ := ts.promoteGroup.Do(relPath, func() (interface{}, error) {
		// Re-check after winning the singleflight race — another caller
		// may have already promoted this file.
		f2, err2 := ts.meta.GetFile(ctx, relPath)
		if err2 != nil {
			return nil, fmt.Errorf("promote: re-check metadata: %w", err2)
		}
		if f2.CurrentTier == hotTier.Name {
			b, berr := ts.BackendFor(hotTier.Name)
			if berr != nil {
				return nil, berr
			}
			lp, _ := b.LocalPath(relPath)
			return &promoteResult{backend: b, localPath: lp}, nil
		}

		src, serr := ts.BackendFor(f2.CurrentTier)
		if serr != nil {
			return nil, fmt.Errorf("promote: resolve source backend %q: %w", f2.CurrentTier, serr)
		}
		dst, derr := ts.BackendFor(hotTier.Name)
		if derr != nil {
			return nil, fmt.Errorf("promote: resolve hot backend %q: %w", hotTier.Name, derr)
		}

		ts.log.Info("promoting file for write",
			zap.String("path", relPath),
			zap.String("from", f2.CurrentTier),
			zap.String("to", hotTier.Name),
		)

		rc, size, gerr := src.Get(ctx, relPath)
		if gerr != nil {
			return nil, fmt.Errorf("promote: read from %q: %w", f2.CurrentTier, gerr)
		}
		if perr := dst.Put(ctx, relPath, rc, size); perr != nil {
			rc.Close()
			return nil, fmt.Errorf("promote: write to %q: %w", hotTier.Name, perr)
		}
		rc.Close()

		promoted := *f2
		promoted.CurrentTier = hotTier.Name
		promoted.State = domain.StateWriting
		if uerr := ts.meta.UpsertFile(ctx, promoted); uerr != nil {
			return nil, fmt.Errorf("promote: update metadata: %w", uerr)
		}

		if aerr := ts.meta.AddFileTier(ctx, domain.FileTier{
			RelPath:   relPath,
			TierName:  hotTier.Name,
			ArrivedAt: time.Now(),
			Verified:  false,
		}); aerr != nil {
			ts.log.Warn("promote: add file tier record", zap.Error(aerr))
		}

		localPath, _ := dst.LocalPath(relPath)
		return &promoteResult{backend: dst, localPath: localPath}, nil
	})
	if err != nil {
		return nil, "", err
	}
	pr := result.(*promoteResult)
	return pr.backend, pr.localPath, nil
}

// It computes the digest, updates metadata, and enqueues replication if needed.
func (ts *TierService) OnWriteComplete(ctx context.Context, relPath, tierName string, size int64, modTime time.Time) error {
	b, err := ts.BackendFor(tierName)
	if err != nil {
		return err
	}

	// Compute digest from the local path (fast; file is on SSD).
	// Also use the file's actual mtime from the filesystem to ensure the
	// staleness check in the replicator compares identical values.
	var dig string
	if localPath, ok := b.LocalPath(relPath); ok {
		dig, err = digest.ComputeFile(localPath)
		if err != nil {
			ts.log.Warn("digest computation failed", zap.String("path", relPath), zap.Error(err))
		}
		if st, serr := os.Stat(localPath); serr == nil {
			modTime = st.ModTime()
			size = st.Size()
		}
	}

	f := domain.File{
		RelPath:     relPath,
		CurrentTier: tierName,
		State:       domain.StateLocal,
		Size:        size,
		ModTime:     modTime,
		Digest:      dig,
	}
	if err := ts.meta.UpsertFile(ctx, f); err != nil {
		return err
	}
	if err := ts.meta.AddFileTier(ctx, domain.FileTier{
		RelPath:   relPath,
		TierName:  tierName,
		ArrivedAt: time.Now(),
		Verified:  true, // we just wrote it locally
	}); err != nil {
		return err
	}

	// Enqueue replication if the rule requires it.
	rule, _ := ts.cfg.Policy.Match(relPath)
	if rule.Replicate {
		// Find the next tier in the evict schedule, if any.
		for _, step := range rule.EvictSchedule {
			if step.ToTier != tierName {
				ts.replicator.Enqueue(CopyJob{
					RelPath:  relPath,
					FromTier: tierName,
					ToTier:   step.ToTier,
				})
				break
			}
		}
	}
	return nil
}

// Guard returns the WriteGuard so the FUSE layer can call Open/Close on it.
func (ts *TierService) Guard() *WriteGuard { return ts.guard }

// Replicator returns the Replicator for admin API access.
func (ts *TierService) Replicator() *Replicator { return ts.replicator }

// Config returns the resolved configuration.
func (ts *TierService) Config() *config.Resolved { return ts.cfg }

// Meta returns the metadata store.
func (ts *TierService) Meta() domain.MetadataStore { return ts.meta }

// HottestTierName returns the name of the tier with priority 0.
func (ts *TierService) HottestTierName() string {
	return ts.cfg.HottestTier().Name
}

// OnDelete removes a file from metadata and all tiers it is present on.
func (ts *TierService) OnDelete(ctx context.Context, relPath string) error {
	ts.guard.Forget(relPath)
	tiers, err := ts.meta.GetFileTiers(ctx, relPath)
	if err != nil {
		return err
	}
	for _, ft := range tiers {
		b, berr := ts.BackendFor(ft.TierName)
		if berr != nil {
			continue
		}
		if derr := b.Delete(ctx, relPath); derr != nil && derr != domain.ErrNotExist {
			ts.log.Warn("delete from tier failed",
				zap.String("path", relPath),
				zap.String("tier", ft.TierName),
				zap.Error(derr),
			)
		}
	}
	return ts.meta.DeleteFile(ctx, relPath)
}

// OnRename updates metadata and renames the file on all tiers it is present on.
// The operation is transactional: metadata is only updated after all backend
// renames (or copy+delete fallbacks) succeed.
func (ts *TierService) OnRename(ctx context.Context, oldPath, newPath string) error {
	f, err := ts.meta.GetFile(ctx, oldPath)
	if err != nil {
		return err
	}
	tiers, err := ts.meta.GetFileTiers(ctx, oldPath)
	if err != nil {
		return err
	}

	// Phase 1: Rename on every tier with rollback on failure.
	var completed []domain.FileTier
	var renameErr error
	for _, ft := range tiers {
		b, berr := ts.BackendFor(ft.TierName)
		if berr != nil {
			renameErr = fmt.Errorf("rename: backend %q not found: %w", ft.TierName, berr)
			break
		}

		// Try native rename first.
		if r, ok := b.(domain.Renamer); ok {
			if rerr := r.Rename(ctx, oldPath, newPath); rerr == nil {
				completed = append(completed, ft)
				continue
			} else {
				ts.log.Debug("native rename failed, trying copy+delete",
					zap.String("tier", ft.TierName), zap.Error(rerr))
			}
		}

		// Fallback: copy+delete.
		rc, size, gerr := b.Get(ctx, oldPath)
		if gerr != nil {
			renameErr = fmt.Errorf("rename: copy from %q failed: %w", ft.TierName, gerr)
			break
		}
		if perr := b.Put(ctx, newPath, rc, size); perr != nil {
			rc.Close()
			renameErr = fmt.Errorf("rename: put to %q failed: %w", ft.TierName, perr)
			break
		}
		rc.Close()
		if derr := b.Delete(ctx, oldPath); derr != nil && derr != domain.ErrNotExist {
			ts.log.Warn("rename copy+delete: delete old failed",
				zap.String("tier", ft.TierName), zap.Error(derr))
		}
		completed = append(completed, ft)
	}

	if renameErr != nil {
		// Rollback completed renames in reverse order.
		for i := len(completed) - 1; i >= 0; i-- {
			ft := completed[i]
			b, berr := ts.BackendFor(ft.TierName)
			if berr != nil {
				ts.log.Error("rename rollback: backend not found",
					zap.String("tier", ft.TierName), zap.Error(berr))
				continue
			}
			if r, ok := b.(domain.Renamer); ok {
				if rerr := r.Rename(ctx, newPath, oldPath); rerr == nil {
					continue
				}
			}
			// Fallback: copy+delete for rollback.
			rc, size, gerr := b.Get(ctx, newPath)
			if gerr != nil {
				ts.log.Error("rename rollback: get from new path failed",
					zap.String("tier", ft.TierName), zap.Error(gerr))
				continue
			}
			if perr := b.Put(ctx, oldPath, rc, size); perr != nil {
				rc.Close()
				ts.log.Error("rename rollback: put to old path failed",
					zap.String("tier", ft.TierName), zap.Error(perr))
				continue
			}
			rc.Close()
			_ = b.Delete(ctx, newPath)
		}
		return renameErr
	}

	// Phase 2: All backends succeeded — update metadata.
	if err := ts.meta.DeleteFile(ctx, oldPath); err != nil {
		return err
	}
	f.RelPath = newPath
	if err := ts.meta.UpsertFile(ctx, *f); err != nil {
		return err
	}
	for _, ft := range tiers {
		ft.RelPath = newPath
		if err := ts.meta.AddFileTier(ctx, ft); err != nil {
			ts.log.Warn("update file_tier on rename", zap.Error(err))
		}
	}
	return nil
}

func (ts *TierService) sweepLoop() {
	defer close(ts.sweepDone)
	ticker := time.NewTicker(ts.cfg.Replication.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ts.sweepStop:
			return
		case <-ticker.C:
			ts.requeuePending()
			if ts.stager != nil && ts.stageTTL > 0 {
				ts.stager.SweepStagingDir(ts.stageTTL)
			}
		}
	}
}

// requeuePending re-enqueues files left in StateLocal from a previous run.
func (ts *TierService) requeuePending() {
	ctx := context.Background()
	files, err := ts.meta.FilesAwaitingReplication(ctx)
	if err != nil {
		ts.log.Error("requeue pending: list files", zap.Error(err))
		return
	}

	// Skip files that already have a pending job to avoid flooding the queue.
	pending := ts.replicator.PendingJobs()
	pendingSet := make(map[string]struct{}, len(pending))
	for _, j := range pending {
		pendingSet[j.RelPath] = struct{}{}
	}

	var enqueued int
	for _, f := range files {
		if _, already := pendingSet[f.RelPath]; already {
			continue
		}
		rule, err := ts.cfg.Policy.Match(f.RelPath)
		if err != nil || !rule.Replicate {
			continue
		}
		for _, step := range rule.EvictSchedule {
			if step.ToTier != f.CurrentTier {
				ts.replicator.Enqueue(CopyJob{
					RelPath:  f.RelPath,
					FromTier: f.CurrentTier,
					ToTier:   step.ToTier,
				})
				enqueued++
				break
			}
		}
	}
	ts.log.Info("requeued pending replication jobs",
		zap.Int("awaiting", len(files)),
		zap.Int("enqueued", enqueued),
		zap.Int("skipped_already_pending", len(files)-enqueued),
	)
}

// BuildBackend creates a domain.Backend from a BackendConfig URI.
// Called by main() before injecting into TierService.
func BuildBackend(cfg config.BackendConfig) (domain.Backend, error) {
	u, err := url.Parse(cfg.URI)
	if err != nil {
		return nil, fmt.Errorf("backend %q: parse URI: %w", cfg.Name, err)
	}
	switch u.Scheme {
	case "file":
		// Import inline to avoid circular dependency; main() should call
		// file.New / s3.New directly. This helper is here for convenience.
		return nil, fmt.Errorf("use file.New() directly for file:// backends")
	case "s3":
		return nil, fmt.Errorf("use s3.New() directly for s3:// backends")
	default:
		return nil, fmt.Errorf("backend %q: unsupported scheme %q", cfg.Name, u.Scheme)
	}
}

// Stager manages temporary local copies of remote-tier files for FUSE reads.
type Stager struct {
	stageDir string
	log      *zap.Logger
}

// StageMeta is persisted alongside staged files for freshness checks.
type StageMeta struct {
	Digest  string    `json:"digest"`
	ModTime time.Time `json:"mtime"`
	Size    int64     `json:"size"`
}

// NewStager creates a Stager using stageDir as scratch space.
func NewStager(stageDir string, log *zap.Logger) *Stager {
	return &Stager{stageDir: stageDir, log: log.Named("stager")}
}

// StagePath returns a collision-free path in stageDir for the given relPath.
func (s *Stager) StagePath(relPath string) string {
	h := sha256.Sum256([]byte(relPath))
	hex := fmt.Sprintf("%x", h[:8])
	base := filepath.Base(relPath)
	return filepath.Join(s.stageDir, hex+"_"+base)
}

// MetaPath returns the sidecar metadata path for a staged file.
func (s *Stager) MetaPath(stagePath string) string {
	return stagePath + ".meta"
}

// WriteMeta writes sidecar metadata for a staged file.
func (s *Stager) WriteMeta(stagePath string, meta StageMeta) error {
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(s.MetaPath(stagePath), data, 0o644)
}

// ReadMeta reads sidecar metadata.
func (s *Stager) ReadMeta(stagePath string) (StageMeta, error) {
	data, err := os.ReadFile(s.MetaPath(stagePath))
	if err != nil {
		return StageMeta{}, err
	}
	var m StageMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return StageMeta{}, err
	}
	return m, nil
}

// IsStale checks whether a staged file matches the authoritative metadata.
func (s *Stager) IsStale(stagePath string, dgst string, modTime time.Time, size int64) bool {
	meta, err := s.ReadMeta(stagePath)
	if err != nil {
		return true
	}
	if size > 0 && meta.Size != size {
		return true
	}
	if dgst != "" && meta.Digest != dgst {
		return true
	}
	if !modTime.IsZero() && meta.ModTime.Truncate(time.Microsecond) != modTime.Truncate(time.Microsecond) {
		return true
	}
	return false
}

// CleanStale removes a staged file and its sidecar.
func (s *Stager) CleanStale(stagePath string) {
	os.Remove(stagePath)
	os.Remove(s.MetaPath(stagePath))
}

// SweepStagingDir removes staged files (and their .meta sidecars) that are
// older than ttl. This prevents the stage directory from growing unbounded.
func (s *Stager) SweepStagingDir(ttl time.Duration) (removed int) {
	entries, err := os.ReadDir(s.stageDir)
	if err != nil {
		s.log.Warn("sweep staging dir: read dir", zap.Error(err))
		return 0
	}
	cutoff := time.Now().Add(-ttl)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		// Skip .meta sidecar files; they are cleaned alongside their parent.
		if filepath.Ext(e.Name()) == ".meta" {
			continue
		}
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			stagePath := filepath.Join(s.stageDir, e.Name())
			s.log.Debug("sweep: removing stale staged file", zap.String("path", stagePath))
			os.Remove(stagePath)
			os.Remove(stagePath + ".meta")
			removed++
		}
	}
	if removed > 0 {
		s.log.Info("swept stale staged files", zap.Int("removed", removed))
	}
	return removed
}
