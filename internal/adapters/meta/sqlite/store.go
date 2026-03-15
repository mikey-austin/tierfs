// Package sqlite implements the MetadataStore port using modernc.org/sqlite
// (pure Go, no CGO). A single write connection serialises mutations; a pool
// of read connections serves concurrent FUSE reads without blocking writers.
package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/mikey-austin/tierfs/internal/domain"
	_ "modernc.org/sqlite"
)

// Store implements domain.MetadataStore.
type Store struct {
	db *sql.DB
	mu sync.Mutex // serialises writes
}

// Open opens (or creates) the SQLite database at path and applies the schema.
func Open(path string) (*Store, error) {
	// Allow multiple concurrent readers with WAL; one writer at a time.
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite open %q: %w", path, err)
	}
	// Separate pool limits: many readers, serialised writer enforced by mu.
	db.SetMaxOpenConns(16)
	db.SetMaxIdleConns(8)

	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the database connection pool.
func (s *Store) Close() error { return s.db.Close() }

// ── File CRUD ────────────────────────────────────────────────────────────────

func (s *Store) UpsertFile(ctx context.Context, f domain.File) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	parentPath := ""
	if dir := path.Dir(f.RelPath); dir != "." {
		parentPath = dir
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO files (rel_path, parent_path, current_tier, state, size, mod_time, digest)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(rel_path) DO UPDATE SET
			parent_path  = excluded.parent_path,
			current_tier = excluded.current_tier,
			state        = excluded.state,
			size         = excluded.size,
			mod_time     = excluded.mod_time,
			digest       = excluded.digest`,
		f.RelPath, parentPath, f.CurrentTier, string(f.State),
		f.Size, f.ModTime.UnixNano(), f.Digest,
	)
	if err != nil {
		return fmt.Errorf("upsert file %q: %w", f.RelPath, err)
	}
	return nil
}

func (s *Store) GetFile(ctx context.Context, relPath string) (*domain.File, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT rel_path, current_tier, state, size, mod_time, digest
		FROM files WHERE rel_path = ?`, relPath)
	f, err := scanFile(row)
	if err == sql.ErrNoRows {
		return nil, domain.ErrNotExist
	}
	if err != nil {
		return nil, fmt.Errorf("get file %q: %w", relPath, err)
	}
	return f, nil
}

func (s *Store) DeleteFile(ctx context.Context, relPath string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	res, err := s.db.ExecContext(ctx, `DELETE FROM files WHERE rel_path = ?`, relPath)
	if err != nil {
		return fmt.Errorf("delete file %q: %w", relPath, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return domain.ErrNotExist
	}
	return nil
}

func (s *Store) ListFiles(ctx context.Context, prefix string) ([]domain.File, error) {
	var (
		rows *sql.Rows
		err  error
	)
	if prefix == "" {
		rows, err = s.db.QueryContext(ctx, `SELECT rel_path, current_tier, state, size, mod_time, digest FROM files`)
	} else {
		like := escapeLike(prefix) + "/%"
		rows, err = s.db.QueryContext(ctx, `
			SELECT rel_path, current_tier, state, size, mod_time, digest
			FROM files WHERE rel_path LIKE ? ESCAPE '\'`, like)
	}
	if err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}
	defer rows.Close()
	return scanFiles(rows)
}

// ── Tier presence ────────────────────────────────────────────────────────────

func (s *Store) AddFileTier(ctx context.Context, ft domain.FileTier) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO file_tiers (rel_path, tier_name, arrived_at, verified)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(rel_path, tier_name) DO UPDATE SET
			arrived_at = excluded.arrived_at,
			verified   = excluded.verified`,
		ft.RelPath, ft.TierName, ft.ArrivedAt.UnixNano(), boolInt(ft.Verified),
	)
	if err != nil {
		return fmt.Errorf("add file tier: %w", err)
	}
	return nil
}

func (s *Store) GetFileTiers(ctx context.Context, relPath string) ([]domain.FileTier, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT rel_path, tier_name, arrived_at, verified
		FROM file_tiers WHERE rel_path = ?`, relPath)
	if err != nil {
		return nil, fmt.Errorf("get file tiers: %w", err)
	}
	defer rows.Close()
	return scanFileTiers(rows)
}

func (s *Store) MarkTierVerified(ctx context.Context, relPath, tierName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		UPDATE file_tiers SET verified = 1
		WHERE rel_path = ? AND tier_name = ?`, relPath, tierName)
	return err
}

func (s *Store) RemoveFileTier(ctx context.Context, relPath, tierName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM file_tiers WHERE rel_path = ? AND tier_name = ?`, relPath, tierName)
	return err
}

func (s *Store) TierArrivedAt(ctx context.Context, relPath, tierName string) (time.Time, error) {
	var nanos int64
	err := s.db.QueryRowContext(ctx, `
		SELECT arrived_at FROM file_tiers
		WHERE rel_path = ? AND tier_name = ?`, relPath, tierName).Scan(&nanos)
	if err == sql.ErrNoRows {
		return time.Time{}, domain.ErrNotExist
	}
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, nanos), nil
}

func (s *Store) IsTierVerified(ctx context.Context, relPath, tierName string) (bool, error) {
	var v int
	err := s.db.QueryRowContext(ctx, `
		SELECT verified FROM file_tiers
		WHERE rel_path = ? AND tier_name = ?`, relPath, tierName).Scan(&v)
	if err == sql.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return v == 1, nil
}

// ── Evictor queries ──────────────────────────────────────────────────────────

func (s *Store) FilesOnTier(ctx context.Context, tierName string) ([]domain.File, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT rel_path, current_tier, state, size, mod_time, digest
		FROM files
		WHERE current_tier = ? AND state = 'synced'`, tierName)
	if err != nil {
		return nil, fmt.Errorf("files on tier: %w", err)
	}
	defer rows.Close()
	return scanFiles(rows)
}

func (s *Store) EvictionCandidates(ctx context.Context, tierName string, olderThan time.Time) ([]domain.File, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT f.rel_path, f.current_tier, f.state, f.size, f.mod_time, f.digest
		FROM files f
		INNER JOIN file_tiers ft ON f.rel_path = ft.rel_path AND ft.tier_name = f.current_tier
		WHERE f.current_tier = ?
		  AND f.state = 'synced'
		  AND ft.arrived_at < ?`,
		tierName, olderThan.UnixNano())
	if err != nil {
		return nil, fmt.Errorf("eviction candidates: %w", err)
	}
	defer rows.Close()
	return scanFiles(rows)
}

func (s *Store) FilesAwaitingReplication(ctx context.Context) ([]domain.File, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT rel_path, current_tier, state, size, mod_time, digest
		FROM files WHERE state = 'local'`)
	if err != nil {
		return nil, fmt.Errorf("files awaiting replication: %w", err)
	}
	defer rows.Close()
	return scanFiles(rows)
}

func (s *Store) OldestAwaitingReplication(ctx context.Context) (time.Time, error) {
	var nanos sql.NullInt64
	err := s.db.QueryRowContext(ctx, `
		SELECT MIN(mod_time) FROM files WHERE state = 'local'`).Scan(&nanos)
	if err != nil {
		return time.Time{}, fmt.Errorf("oldest awaiting replication: %w", err)
	}
	if !nanos.Valid {
		return time.Time{}, nil
	}
	return time.Unix(0, nanos.Int64), nil
}

// ── Directory listing ────────────────────────────────────────────────────────

// ListDir returns the immediate children of dirPath by splitting file paths.
// dirPath="" returns root-level entries.
//
// Uses the indexed parent_path column: direct children are an exact match
// (O(children) via index seek), subdirectory detection uses an index prefix scan.
func (s *Store) ListDir(ctx context.Context, dirPath string) ([]domain.FileInfo, error) {
	parentPath := strings.TrimSuffix(dirPath, "/")

	// Direct child files: parent_path exact match (index seek).
	rows, err := s.db.QueryContext(ctx, `
		SELECT rel_path, size, mod_time FROM files
		WHERE parent_path = ?`, parentPath)
	if err != nil {
		return nil, fmt.Errorf("list dir: %w", err)
	}
	defer rows.Close()

	type entry struct {
		size    int64
		modTime time.Time
		isDir   bool
	}
	seen := make(map[string]*entry)

	for rows.Next() {
		var relPath string
		var size, modTimeNano int64
		if err := rows.Scan(&relPath, &size, &modTimeNano); err != nil {
			return nil, err
		}
		name := path.Base(relPath)
		seen[name] = &entry{
			size:    size,
			modTime: time.Unix(0, modTimeNano),
			isDir:   false,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Subdirectory detection: find distinct parent_paths one level deeper
	// (index prefix scan on parent_path).
	var subRows *sql.Rows
	if parentPath == "" {
		subRows, err = s.db.QueryContext(ctx, `
			SELECT DISTINCT parent_path FROM files
			WHERE parent_path != ''`)
	} else {
		like := escapeLike(parentPath) + "/%"
		subRows, err = s.db.QueryContext(ctx, `
			SELECT DISTINCT parent_path FROM files
			WHERE parent_path LIKE ? ESCAPE '\'`, like)
	}
	if err != nil {
		return nil, fmt.Errorf("list dir subdirs: %w", err)
	}
	defer subRows.Close()

	for subRows.Next() {
		var pp string
		if err := subRows.Scan(&pp); err != nil {
			return nil, err
		}
		// Extract the immediate child directory name.
		var sub string
		if parentPath == "" {
			sub = pp
		} else {
			sub = strings.TrimPrefix(pp, parentPath+"/")
		}
		name := sub
		if idx := strings.IndexByte(sub, '/'); idx >= 0 {
			name = sub[:idx]
		}
		if _, ok := seen[name]; !ok {
			seen[name] = &entry{isDir: true}
		} else {
			seen[name].isDir = true
		}
	}
	if err := subRows.Err(); err != nil {
		return nil, err
	}

	prefix := parentPath
	if prefix != "" {
		prefix += "/"
	}
	out := make([]domain.FileInfo, 0, len(seen))
	for name, e := range seen {
		out = append(out, domain.FileInfo{
			RelPath: prefix + name,
			Name:    name,
			Size:    e.size,
			ModTime: e.modTime,
			IsDir:   e.isDir,
		})
	}
	return out, nil
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type scanner interface {
	Scan(dest ...interface{}) error
}

func scanFile(row scanner) (*domain.File, error) {
	var f domain.File
	var state string
	var modTimeNano int64
	if err := row.Scan(&f.RelPath, &f.CurrentTier, &state, &f.Size, &modTimeNano, &f.Digest); err != nil {
		return nil, err
	}
	f.State = domain.TierState(state)
	f.ModTime = time.Unix(0, modTimeNano)
	return &f, nil
}

func scanFiles(rows *sql.Rows) ([]domain.File, error) {
	var out []domain.File
	for rows.Next() {
		f, err := scanFile(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *f)
	}
	return out, rows.Err()
}

func scanFileTiers(rows *sql.Rows) ([]domain.FileTier, error) {
	var out []domain.FileTier
	for rows.Next() {
		var ft domain.FileTier
		var arrivedNano int64
		var verified int
		if err := rows.Scan(&ft.RelPath, &ft.TierName, &arrivedNano, &verified); err != nil {
			return nil, err
		}
		ft.ArrivedAt = time.Unix(0, arrivedNano)
		ft.Verified = verified == 1
		out = append(out, ft)
	}
	return out, rows.Err()
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// escapeLike escapes special LIKE characters in a path prefix.
func escapeLike(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, "%", `\%`)
	s = strings.ReplaceAll(s, "_", `\_`)
	return s
}
