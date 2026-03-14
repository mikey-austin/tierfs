package admin

import (
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

// LogEntry is a single captured log line.
type LogEntry struct {
	Time    time.Time         `json:"ts"`
	Level   string            `json:"level"`
	Logger  string            `json:"logger"`
	Message string            `json:"msg"`
	Fields  map[string]string `json:"fields,omitempty"`
}

// LogBuffer is a thread-safe ring buffer that captures log entries by
// implementing zapcore.Core. Wire it as a tee alongside the main logger.
type LogBuffer struct {
	mu      sync.RWMutex
	entries []LogEntry
	pos     int
	full    bool
	cap     int
	level   zapcore.Level
}

// NewLogBuffer creates a LogBuffer that retains the last cap entries.
func NewLogBuffer(cap int) *LogBuffer {
	return &LogBuffer{
		entries: make([]LogEntry, cap),
		cap:     cap,
		level:   zapcore.DebugLevel,
	}
}

// Entries returns a time-ordered snapshot of buffered entries.
func (b *LogBuffer) Entries(level string, tail int) []LogEntry {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var n int
	if b.full {
		n = b.cap
	} else {
		n = b.pos
	}

	out := make([]LogEntry, 0, n)
	start := 0
	if b.full {
		start = b.pos
	}
	for i := 0; i < n; i++ {
		e := b.entries[(start+i)%b.cap]
		if level != "" && e.Level != level {
			continue
		}
		out = append(out, e)
	}

	if tail > 0 && len(out) > tail {
		out = out[len(out)-tail:]
	}
	return out
}

// ── zapcore.Core implementation ─────────────────────────────────────────────

func (b *LogBuffer) Enabled(lvl zapcore.Level) bool {
	return lvl >= b.level
}

func (b *LogBuffer) With([]zapcore.Field) zapcore.Core {
	return b
}

func (b *LogBuffer) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if b.Enabled(ent.Level) {
		ce = ce.AddCore(ent, b)
	}
	return ce
}

func (b *LogBuffer) Write(ent zapcore.Entry, fields []zapcore.Field) error {
	fe := make(map[string]string, len(fields))
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	for k, v := range enc.Fields {
		fe[k] = fmt.Sprintf("%v", v)
	}

	entry := LogEntry{
		Time:    ent.Time,
		Level:   ent.Level.String(),
		Logger:  ent.LoggerName,
		Message: ent.Message,
		Fields:  fe,
	}

	b.mu.Lock()
	b.entries[b.pos] = entry
	b.pos = (b.pos + 1) % b.cap
	if b.pos == 0 {
		b.full = true
	}
	b.mu.Unlock()

	return nil
}

func (b *LogBuffer) Sync() error { return nil }
