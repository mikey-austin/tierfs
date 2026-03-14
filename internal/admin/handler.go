package admin

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/mikey-austin/tierfs/internal/app"
	"github.com/mikey-austin/tierfs/internal/config"
	"github.com/mikey-austin/tierfs/internal/observability/metrics"
)

// Handler serves the admin JSON API.
type Handler struct {
	svc     *app.TierService
	metrics *metrics.Registry
	logBuf  *LogBuffer
}

// NewHandler creates an admin API handler.
func NewHandler(svc *app.TierService, reg *metrics.Registry, logBuf *LogBuffer) *Handler {
	return &Handler{
		svc:     svc,
		metrics: reg,
		logBuf:  logBuf,
	}
}

// Register mounts all API routes on the given mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/config", h.handleConfig)
	mux.HandleFunc("/api/v1/tiers", h.handleTiers)
	mux.HandleFunc("/api/v1/files", h.handleFiles)
	mux.HandleFunc("/api/v1/replication/queue", h.handleReplicationQueue)
	mux.HandleFunc("/api/v1/writeguard", h.handleWriteGuard)
	mux.HandleFunc("/api/v1/metrics/snapshot", h.handleMetricsSnapshot)
	mux.HandleFunc("/api/v1/logs", h.handleLogs)
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

// ── /api/v1/config ──────────────────────────────────────────────────────────

type configResponse struct {
	Tiers    []tierConfig    `json:"tiers"`
	Backends []backendConfig `json:"backends"`
	Rules    []ruleConfig    `json:"rules"`
}

type tierConfig struct {
	Name       string   `json:"name"`
	Backend    string   `json:"backend"`
	Scheme     string   `json:"scheme"`
	Capacity   *int64   `json:"capacity"` // null = unlimited
	Priority   int      `json:"priority"`
	Transforms []string `json:"transforms"`
}

type backendConfig struct {
	Name string `json:"name"`
	URI  string `json:"uri"`
	Type string `json:"type"`
}

type ruleConfig struct {
	Name          string            `json:"name"`
	Match         string            `json:"match"`
	PinTier       string            `json:"pinTier"`
	EvictSchedule []evictStepConfig `json:"evictSchedule"`
	PromoteOnRead interface{}       `json:"promoteOnRead"` // false or string tier name
	Replicate     bool              `json:"replicate"`
}

type evictStepConfig struct {
	After string `json:"after"`
	To    string `json:"to"`
}

func (h *Handler) handleConfig(w http.ResponseWriter, _ *http.Request) {
	cfg := h.svc.Config()

	tiers := make([]tierConfig, len(cfg.Tiers))
	for i, t := range cfg.Tiers {
		u, _ := url.Parse(t.Backend.URI)
		tc := tierConfig{
			Name:     t.Name,
			Backend:  t.Backend.Name,
			Scheme:   u.Scheme + "://",
			Priority: t.Priority,
		}
		if !t.Capacity.Unlimited {
			b := t.Capacity.Bytes
			tc.Capacity = &b
		}
		tc.Transforms = resolveTransforms(t.Backend.Transform)
		tiers[i] = tc
	}

	backends := make([]backendConfig, 0, len(cfg.Backends))
	for _, b := range cfg.Backends {
		u, _ := url.Parse(b.URI)
		backends = append(backends, backendConfig{
			Name: b.Name,
			URI:  sanitizeURI(b.URI),
			Type: u.Scheme,
		})
	}

	rules := make([]ruleConfig, 0)
	for _, r := range cfg.Policy.Rules() {
		rc := ruleConfig{
			Name:      r.Name,
			Match:     r.Match,
			PinTier:   r.PinTier,
			Replicate: r.Replicate,
		}
		if r.PromoteOnRead.Enabled {
			if r.PromoteOnRead.TargetTier != "" {
				rc.PromoteOnRead = r.PromoteOnRead.TargetTier
			} else {
				rc.PromoteOnRead = true
			}
		} else {
			rc.PromoteOnRead = false
		}
		steps := make([]evictStepConfig, len(r.EvictSchedule))
		for j, s := range r.EvictSchedule {
			steps[j] = evictStepConfig{After: s.After.String(), To: s.ToTier}
		}
		rc.EvictSchedule = steps
		rules = append(rules, rc)
	}

	writeJSON(w, configResponse{Tiers: tiers, Backends: backends, Rules: rules})
}

func resolveTransforms(t config.BackendTransformConfig) []string {
	var out []string
	if t.Compression != nil {
		algo := t.Compression.Algorithm
		if algo == "" {
			algo = "zstd"
		}
		out = append(out, algo)
	}
	if t.Checksum != nil && t.Encryption == nil {
		out = append(out, "xxh3-128")
	}
	if t.Encryption != nil {
		out = append(out, "aes-256-gcm")
	}
	return out
}

func sanitizeURI(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	u.User = nil
	return u.String()
}

// ── /api/v1/tiers ───────────────────────────────────────────────────────────

type tierStatus struct {
	Name      string `json:"name"`
	FileCount int    `json:"fileCount"`
	BytesUsed int64  `json:"bytesUsed"`
}

func (h *Handler) handleTiers(w http.ResponseWriter, _ *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg := h.svc.Config()
	out := make([]tierStatus, len(cfg.Tiers))
	for i, t := range cfg.Tiers {
		files, err := h.svc.Meta().FilesOnTier(ctx, t.Name)
		ts := tierStatus{Name: t.Name}
		if err == nil {
			ts.FileCount = len(files)
			for _, f := range files {
				ts.BytesUsed += f.Size
			}
		}
		out[i] = ts
	}
	writeJSON(w, out)
}

// ── /api/v1/files ───────────────────────────────────────────────────────────

type filesResponse struct {
	Files  []fileEntry `json:"files"`
	Total  int         `json:"total"`
	Offset int         `json:"offset"`
	Limit  int         `json:"limit"`
}

type fileEntry struct {
	RelPath     string `json:"relPath"`
	CurrentTier string `json:"currentTier"`
	State       string `json:"state"`
	Size        int64  `json:"size"`
	ModTime     string `json:"modTime"`
	Digest      string `json:"digest"`
}

func (h *Handler) handleFiles(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := r.URL.Query()
	prefix := q.Get("prefix")
	tierFilter := q.Get("tier")
	stateFilter := q.Get("state")
	limit := intParam(q, "limit", 50)
	offset := intParam(q, "offset", 0)

	all, err := h.svc.Meta().ListFiles(ctx, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// In-memory filter.
	filtered := make([]fileEntry, 0, len(all))
	for _, f := range all {
		if tierFilter != "" && f.CurrentTier != tierFilter {
			continue
		}
		if stateFilter != "" && string(f.State) != stateFilter {
			continue
		}
		filtered = append(filtered, fileEntry{
			RelPath:     f.RelPath,
			CurrentTier: f.CurrentTier,
			State:       string(f.State),
			Size:        f.Size,
			ModTime:     f.ModTime.Format(time.RFC3339),
			Digest:      f.Digest,
		})
	}

	total := len(filtered)
	if offset > total {
		offset = total
	}
	end := offset + limit
	if end > total {
		end = total
	}

	writeJSON(w, filesResponse{
		Files:  filtered[offset:end],
		Total:  total,
		Offset: offset,
		Limit:  limit,
	})
}

// ── /api/v1/replication/queue ───────────────────────────────────────────────

type replicationResponse struct {
	Jobs        []app.CopyJob `json:"jobs"`
	TotalCopied int64         `json:"totalCopied"`
	TotalFailed int64         `json:"totalFailed"`
	QueueDepth  int64         `json:"queueDepth"`
}

func (h *Handler) handleReplicationQueue(w http.ResponseWriter, _ *http.Request) {
	repl := h.svc.Replicator()
	copied, failed, depth := repl.Metrics()
	jobs := repl.PendingJobs()

	writeJSON(w, replicationResponse{
		Jobs:        jobs,
		TotalCopied: copied,
		TotalFailed: failed,
		QueueDepth:  depth,
	})
}

// ── /api/v1/writeguard ──────────────────────────────────────────────────────

type writeGuardResponse struct {
	Entries []writeGuardEntry `json:"entries"`
}

type writeGuardEntry struct {
	RelPath       string `json:"relPath"`
	OpenCount     int    `json:"openCount"`
	LastClose     string `json:"lastClose,omitempty"`
	QuiescentSoon bool   `json:"quiescentSoon"`
}

func (h *Handler) handleWriteGuard(w http.ResponseWriter, _ *http.Request) {
	snap := h.svc.Guard().Snapshot()
	entries := make([]writeGuardEntry, 0, len(snap))
	for path, e := range snap {
		we := writeGuardEntry{
			RelPath:       path,
			OpenCount:     e.OpenCount,
			QuiescentSoon: e.QuiescentSoon,
		}
		if !e.LastClose.IsZero() {
			we.LastClose = e.LastClose.Format(time.RFC3339)
		}
		entries = append(entries, we)
	}
	writeJSON(w, writeGuardResponse{Entries: entries})
}

// ── /api/v1/metrics/snapshot ────────────────────────────────────────────────

func (h *Handler) handleMetricsSnapshot(w http.ResponseWriter, _ *http.Request) {
	families, err := h.metrics.Gather()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	out := make(map[string]interface{}, len(families))
	for _, mf := range families {
		name := mf.GetName()
		ms := mf.GetMetric()
		if len(ms) == 1 && len(ms[0].GetLabel()) == 0 {
			// Simple metric (no labels).
			m := ms[0]
			if g := m.GetGauge(); g != nil {
				out[name] = g.GetValue()
			} else if c := m.GetCounter(); c != nil {
				out[name] = c.GetValue()
			} else if h := m.GetHistogram(); h != nil {
				out[name+"_count"] = h.GetSampleCount()
				out[name+"_sum"] = h.GetSampleSum()
			}
		} else {
			// Labelled metric — group by label set.
			items := make([]map[string]interface{}, 0, len(ms))
			for _, m := range ms {
				item := make(map[string]interface{})
				for _, lp := range m.GetLabel() {
					item[lp.GetName()] = lp.GetValue()
				}
				if g := m.GetGauge(); g != nil {
					item["value"] = g.GetValue()
				} else if c := m.GetCounter(); c != nil {
					item["value"] = c.GetValue()
				} else if h := m.GetHistogram(); h != nil {
					item["count"] = h.GetSampleCount()
					item["sum"] = h.GetSampleSum()
				}
				items = append(items, item)
			}
			out[name] = items
		}
	}

	writeJSON(w, out)
}

// ── /api/v1/logs ────────────────────────────────────────────────────────────

func (h *Handler) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tail := intParam(q, "tail", 200)
	level := q.Get("level")
	entries := h.logBuf.Entries(level, tail)
	writeJSON(w, entries)
}

// ── helpers ─────────────────────────────────────────────────────────────────

func intParam(q url.Values, key string, def int) int {
	s := q.Get(key)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil || v < 0 {
		return def
	}
	return v
}
