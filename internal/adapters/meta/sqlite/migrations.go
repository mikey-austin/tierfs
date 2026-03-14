package sqlite

// schema contains the complete database DDL, applied as a single transaction
// on first open. WAL mode and a generous cache size are set via PRAGMA to
// maximise concurrent read throughput alongside the FUSE workload.
const schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous  = NORMAL;
PRAGMA cache_size   = -32768; -- 32 MiB page cache
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS files (
    rel_path     TEXT    NOT NULL PRIMARY KEY,
    current_tier TEXT    NOT NULL,
    state        TEXT    NOT NULL DEFAULT 'local',
    size         INTEGER NOT NULL DEFAULT 0,
    mod_time     INTEGER NOT NULL DEFAULT 0,  -- unix nanoseconds
    digest       TEXT    NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS file_tiers (
    rel_path   TEXT    NOT NULL,
    tier_name  TEXT    NOT NULL,
    arrived_at INTEGER NOT NULL, -- unix nanoseconds
    verified   INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (rel_path, tier_name),
    FOREIGN KEY (rel_path) REFERENCES files(rel_path) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_files_state        ON files(state);
CREATE INDEX IF NOT EXISTS idx_files_current_tier ON files(current_tier);
CREATE INDEX IF NOT EXISTS idx_file_tiers_tier    ON file_tiers(tier_name);
CREATE INDEX IF NOT EXISTS idx_file_tiers_arrived ON file_tiers(tier_name, arrived_at);
`
