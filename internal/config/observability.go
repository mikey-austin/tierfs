package config

import (
	"fmt"
	"strings"
)

// ObservabilityConfig is embedded in the root Config.
type ObservabilityConfig struct {
	Logging  LoggingConfig  `toml:"logging"`
	Metrics  MetricsConfig  `toml:"metrics"`
	Tracing  TracingConfig  `toml:"tracing"`
}

// LoggingConfig controls structured log output.
type LoggingConfig struct {
	// Level: debug | info | warn | error  (default: info)
	Level string `toml:"level"`
	// Format: json | console  (default: json for prod, console for dev)
	Format string `toml:"format"`
	// OutputPaths: list of sinks. "stdout", "stderr", or file paths.
	// Log files are rotated via lumberjack when a path is specified.
	OutputPaths []string `toml:"output_paths"`
	// Rotation controls log file rotation (only applies to file sinks).
	Rotation LogRotationConfig `toml:"rotation"`
	// Development enables caller info, stack traces on warn+, and coloured output.
	Development bool `toml:"development"`
}

// LogRotationConfig maps to lumberjack.Logger fields.
type LogRotationConfig struct {
	MaxSizeMB  int  `toml:"max_size_mb"`  // default 100
	MaxBackups int  `toml:"max_backups"`  // default 5
	MaxAgeDays int  `toml:"max_age_days"` // default 30
	Compress   bool `toml:"compress"`     // gzip rotated files
}

// MetricsConfig controls the Prometheus exposition endpoint.
type MetricsConfig struct {
	// Enabled: expose /metrics on the given address.
	Enabled bool   `toml:"enabled"`
	Address string `toml:"address"` // e.g. ":9100"
	// Path: HTTP path for metrics endpoint (default: /metrics).
	Path string `toml:"path"`
}

// TracingConfig controls OpenTelemetry trace export.
type TracingConfig struct {
	// Enabled: emit OTLP spans.
	Enabled bool `toml:"enabled"`
	// Endpoint: OTLP gRPC endpoint, e.g. "localhost:4317"
	Endpoint string `toml:"endpoint"`
	// ServiceName: resource attribute (default: "tierfs")
	ServiceName string `toml:"service_name"`
	// SampleRate: 0.0–1.0  (default: 1.0)
	SampleRate float64 `toml:"sample_rate"`
	// Insecure: skip TLS verification for local Jaeger/Tempo.
	Insecure bool `toml:"insecure"`
}

// LogLevelResolved converts a level string to a canonical form.
func ResolveLogLevel(level string) (string, error) {
	switch strings.ToLower(level) {
	case "", "info":
		return "info", nil
	case "debug":
		return "debug", nil
	case "warn", "warning":
		return "warn", nil
	case "error":
		return "error", nil
	default:
		return "", fmt.Errorf("unknown log level %q (debug|info|warn|error)", level)
	}
}

// defaults fills zero values with sensible production defaults.
func (o *ObservabilityConfig) Defaults() {
	if o.Logging.Level == "" {
		o.Logging.Level = "info"
	}
	if o.Logging.Format == "" {
		if o.Logging.Development {
			o.Logging.Format = "console"
		} else {
			o.Logging.Format = "json"
		}
	}
	if len(o.Logging.OutputPaths) == 0 {
		o.Logging.OutputPaths = []string{"stdout"}
	}
	if o.Logging.Rotation.MaxSizeMB == 0 {
		o.Logging.Rotation.MaxSizeMB = 100
	}
	if o.Logging.Rotation.MaxBackups == 0 {
		o.Logging.Rotation.MaxBackups = 5
	}
	if o.Logging.Rotation.MaxAgeDays == 0 {
		o.Logging.Rotation.MaxAgeDays = 30
	}
	if o.Metrics.Address == "" {
		o.Metrics.Address = ":9100"
	}
	if o.Metrics.Path == "" {
		o.Metrics.Path = "/metrics"
	}
	if o.Tracing.ServiceName == "" {
		o.Tracing.ServiceName = "tierfs"
	}
	if o.Tracing.SampleRate == 0 {
		o.Tracing.SampleRate = 1.0
	}
}
