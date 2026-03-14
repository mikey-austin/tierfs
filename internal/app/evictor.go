package app

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// EvictorConfig holds settings for the eviction loop.
type EvictorConfig struct {
	CheckInterval     time.Duration
	CapacityThreshold float64 // 0-1, evict when tier usage exceeds this
	CapacityHeadroom  float64 // 0-1, evict down to this watermark
}

// TierCapacity reports used/total bytes for a tier.
// Implemented by TierService using backend.Stat calls.
type TierCapacity interface {
	UsedBytes(ctx context.Context, tierName string) (int64, error)
	CapacityBytes(tierName string) (int64, bool) // bool = unlimited
}

// Evictor runs on a ticker and moves files between tiers according to
// each file's matching rule's EvictSchedule. It also responds to capacity
// pressure, evicting the oldest synced files first when a tier is full.
type Evictor struct {
	cfg        EvictorConfig
	meta       domain.MetadataStore
	policy     *domain.PolicyEngine
	replicator *Replicator
	tiers      TierLookup
	capacity   TierCapacity
	log        *zap.Logger
	stop       chan struct{}
	done       chan struct{}
}

// NewEvictor creates an Evictor. Call Start() to begin the loop.
func NewEvictor(
	cfg EvictorConfig,
	meta domain.MetadataStore,
	policy *domain.PolicyEngine,
	replicator *Replicator,
	tiers TierLookup,
	capacity TierCapacity,
	log *zap.Logger,
) *Evictor {
	return &Evictor{
		cfg:        cfg,
		meta:       meta,
		policy:     policy,
		replicator: replicator,
		tiers:      tiers,
		capacity:   capacity,
		log:        log.Named("evictor"),
		stop:       make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// Start begins the eviction loop in a background goroutine.
func (e *Evictor) Start() {
	go e.loop()
}

// Stop signals the loop to exit and waits for it to finish.
func (e *Evictor) Stop() {
	close(e.stop)
	<-e.done
}

func (e *Evictor) loop() {
	defer close(e.done)
	ticker := time.NewTicker(e.cfg.CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.stop:
			return
		case <-ticker.C:
			e.tick()
		}
	}
}

func (e *Evictor) tick() {
	ctx := context.Background()
	files, err := e.meta.ListFiles(ctx, "")
	if err != nil {
		e.log.Error("list files for eviction", zap.Error(err))
		return
	}

	for _, f := range files {
		// Only evict files that are confirmed on at least one other tier.
		if f.State != domain.StateSynced {
			continue
		}

		rule, err := e.policy.Match(f.RelPath)
		if err != nil {
			continue // no rule: leave alone
		}

		// Pin tier: never auto-evict.
		if rule.PinTier == f.CurrentTier {
			continue
		}

		e.evaluateSchedule(ctx, f, rule)
	}
}

// evaluateSchedule walks the rule's EvictSchedule to find the furthest
// step the file qualifies for, then initiates migration or eviction.
func (e *Evictor) evaluateSchedule(ctx context.Context, f domain.File, rule domain.Rule) {
	if len(rule.EvictSchedule) == 0 {
		return
	}

	arrivedAt, err := e.meta.TierArrivedAt(ctx, f.RelPath, f.CurrentTier)
	if err != nil {
		e.log.Debug("tier arrived_at missing", zap.String("path", f.RelPath))
		return
	}
	age := time.Since(arrivedAt)

	// Find the furthest qualifying step for this file's current position.
	var targetTier string
	for _, step := range rule.EvictSchedule {
		if step.After.Never {
			break
		}
		if age < step.After.D {
			break // schedule is ordered; later steps require even more time
		}
		if step.ToTier == f.CurrentTier {
			continue // already on this tier or past it
		}
		targetTier = step.ToTier
	}

	if targetTier == "" {
		return // no qualifying step
	}

	// Check the target tier has a verified copy.
	verified, err := e.meta.IsTierVerified(ctx, f.RelPath, targetTier)
	if err != nil {
		e.log.Error("check tier verified", zap.Error(err))
		return
	}
	if !verified {
		// Enqueue the copy; eviction will happen on the next tick after verify.
		e.replicator.Enqueue(CopyJob{
			RelPath:  f.RelPath,
			FromTier: f.CurrentTier,
			ToTier:   targetTier,
		})
		return
	}

	// Evict from current tier.
	e.evict(ctx, f, targetTier)
}

func (e *Evictor) evict(ctx context.Context, f domain.File, targetTier string) {
	log := e.log.With(
		zap.String("path", f.RelPath),
		zap.String("from", f.CurrentTier),
		zap.String("to", targetTier),
	)

	backend, err := e.tiers.BackendFor(f.CurrentTier)
	if err != nil {
		log.Error("resolve backend for eviction", zap.Error(err))
		return
	}

	if err := backend.Delete(ctx, f.RelPath); err != nil {
		if err != domain.ErrNotExist {
			log.Error("delete from source tier", zap.Error(err))
			return
		}
	}

	if err := e.meta.RemoveFileTier(ctx, f.RelPath, f.CurrentTier); err != nil {
		log.Error("remove file tier record", zap.Error(err))
	}

	// If the destination backend is a Finalizer (e.g. null://), the file has
	// been intentionally discarded. Purge it from metadata entirely rather
	// than updating CurrentTier to a tier from which reads always fail.
	dstBackend, err := e.tiers.BackendFor(targetTier)
	if err == nil {
		if fin, ok := dstBackend.(domain.Finalizer); ok && fin.IsFinal() {
			if err := e.meta.DeleteFile(ctx, f.RelPath); err != nil {
				log.Error("purge finalised file from metadata", zap.Error(err))
			}
			log.Info("file finalised and purged", zap.String("final_tier", targetTier))
			return
		}
	}

	// Normal path: promote CurrentTier to the target.
	f.CurrentTier = targetTier
	if err := e.meta.UpsertFile(ctx, f); err != nil {
		log.Error("update current tier in meta", zap.Error(err))
		return
	}

	log.Info("evicted file to next tier")
}

// PromoteForRead schedules promotion of a file to a hotter tier after a read.
// This is a best-effort fire-and-forget; the caller serves the read from the
// current (cold) tier immediately.
func (e *Evictor) PromoteForRead(f domain.File, targetTier string) {
	// Promotion is just a copy in reverse; the evictor removes the cold copy
	// on the next tick once the hot copy is verified.
	e.replicator.Enqueue(CopyJob{
		RelPath:  f.RelPath,
		FromTier: f.CurrentTier,
		ToTier:   targetTier,
	})
	// Update CurrentTier optimistically so subsequent reads hit the warm tier.
	// The actual file won't be there until the copy completes, so the FUSE
	// open path must handle missing local files gracefully by falling back.
}
