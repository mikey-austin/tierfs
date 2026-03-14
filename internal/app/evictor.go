package app

import (
	"context"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/domain"
)

// EvictorConfig holds settings for the eviction loop.
type EvictorConfig struct {
	CheckInterval     time.Duration
	CapacityThreshold float64 // 0-1, evict when tier usage exceeds this
	CapacityHeadroom  float64 // 0-1, evict down to this watermark
	TierNames         []string
}

// TierCapacity reports used/total bytes for a tier.
// Implemented by TierService using backend.Stat calls.
type TierCapacity interface {
	UsedBytes(ctx context.Context, tierName string) (int64, error)
	CapacityBytes(tierName string) (int64, bool) // bool = unlimited
	TierNames() []string
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
	for _, tierName := range e.cfg.TierNames {
		e.tickTier(ctx, tierName)
	}
	e.capacityPass(ctx)
}

func (e *Evictor) tickTier(ctx context.Context, tierName string) {
	files, err := e.meta.EvictionCandidates(ctx, tierName, time.Now())
	if err != nil {
		e.log.Error("eviction candidates", zap.String("tier", tierName), zap.Error(err))
		return
	}

	for _, f := range files {
		rule, err := e.policy.Match(f.RelPath)
		if err != nil {
			continue
		}
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

func (e *Evictor) capacityPass(ctx context.Context) {
	for _, tierName := range e.capacity.TierNames() {
		capBytes, unlimited := e.capacity.CapacityBytes(tierName)
		if unlimited || capBytes <= 0 {
			continue
		}

		usedBytes, err := e.capacity.UsedBytes(ctx, tierName)
		if err != nil {
			e.log.Error("capacity check: used bytes", zap.String("tier", tierName), zap.Error(err))
			continue
		}

		ratio := float64(usedBytes) / float64(capBytes)
		if ratio <= e.cfg.CapacityThreshold {
			continue
		}

		e.log.Info("capacity threshold exceeded",
			zap.String("tier", tierName),
			zap.Float64("ratio", ratio),
			zap.Float64("threshold", e.cfg.CapacityThreshold),
		)

		targetBytes := int64(e.cfg.CapacityHeadroom * float64(capBytes))
		e.evictForCapacity(ctx, tierName, usedBytes, targetBytes)
	}
}

func (e *Evictor) evictForCapacity(ctx context.Context, tierName string, usedBytes, targetBytes int64) {
	files, err := e.meta.FilesOnTier(ctx, tierName)
	if err != nil {
		e.log.Error("capacity eviction: list files", zap.String("tier", tierName), zap.Error(err))
		return
	}

	type candidate struct {
		file       domain.File
		arrivedAt  time.Time
		targetTier string
	}
	var candidates []candidate

	for _, f := range files {
		if f.State != domain.StateSynced {
			continue
		}
		rule, err := e.policy.Match(f.RelPath)
		if err != nil {
			continue
		}
		if rule.PinTier == f.CurrentTier {
			continue
		}
		target := nextTierFromSchedule(rule, f.CurrentTier)
		if target == "" {
			continue
		}
		verified, err := e.meta.IsTierVerified(ctx, f.RelPath, target)
		if err != nil || !verified {
			if err == nil && !verified {
				e.replicator.Enqueue(CopyJob{
					RelPath:  f.RelPath,
					FromTier: f.CurrentTier,
					ToTier:   target,
				})
			}
			continue
		}
		arrivedAt, err := e.meta.TierArrivedAt(ctx, f.RelPath, tierName)
		if err != nil {
			continue
		}
		candidates = append(candidates, candidate{
			file:       f,
			arrivedAt:  arrivedAt,
			targetTier: target,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].arrivedAt.Before(candidates[j].arrivedAt)
	})

	remaining := usedBytes
	for _, c := range candidates {
		if remaining <= targetBytes {
			break
		}
		e.evict(ctx, c.file, c.targetTier)
		remaining -= c.file.Size
	}
}

// nextTierFromSchedule returns the next tier from the evict schedule,
// ignoring age requirements (for capacity-pressure eviction).
func nextTierFromSchedule(rule domain.Rule, currentTier string) string {
	for _, step := range rule.EvictSchedule {
		if step.ToTier != currentTier {
			return step.ToTier
		}
	}
	return ""
}
