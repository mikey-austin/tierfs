// Package logging constructs a zap.Logger from the LoggingConfig, wiring in
// lumberjack for log rotation when file sinks are configured.
package logging

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/mikey-austin/tierfs/internal/config"
)

// Build constructs a zap.Logger from the provided config.
// The returned logger must be synced by the caller on shutdown:
//
//	defer logger.Sync()
func Build(cfg config.LoggingConfig) (*zap.Logger, error) {
	level, err := parseLevel(cfg.Level)
	if err != nil {
		return nil, err
	}

	// Build encoder config.
	encCfg := zap.NewProductionEncoderConfig()
	encCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encCfg.EncodeDuration = zapcore.StringDurationEncoder
	if cfg.Development {
		encCfg = zap.NewDevelopmentEncoderConfig()
		encCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	}

	// Build encoder.
	var enc zapcore.Encoder
	switch strings.ToLower(cfg.Format) {
	case "json", "":
		enc = zapcore.NewJSONEncoder(encCfg)
	case "console":
		enc = zapcore.NewConsoleEncoder(encCfg)
	default:
		return nil, fmt.Errorf("unknown log format %q (json|console)", cfg.Format)
	}

	// Build write syncer(s).
	var syncer zapcore.WriteSyncer
	if len(cfg.OutputPaths) == 0 {
		cfg.OutputPaths = []string{"stdout"}
	}
	syncs := make([]zapcore.WriteSyncer, 0, len(cfg.OutputPaths))
	for _, path := range cfg.OutputPaths {
		ws, err := openSink(path, cfg.Rotation)
		if err != nil {
			return nil, fmt.Errorf("open log sink %q: %w", path, err)
		}
		syncs = append(syncs, ws)
	}
	syncer = zapcore.NewMultiWriteSyncer(syncs...)

	// Wire options.
	opts := []zap.Option{
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel),
	}
	if cfg.Development {
		opts = append(opts, zap.Development())
		opts = append(opts, zap.AddStacktrace(zapcore.WarnLevel))
	}

	core := zapcore.NewCore(enc, syncer, level)
	return zap.New(core, opts...), nil
}

// MustBuild is like Build but panics on error. Suitable for main().
func MustBuild(cfg config.LoggingConfig) *zap.Logger {
	l, err := Build(cfg)
	if err != nil {
		panic(fmt.Sprintf("build logger: %v", err))
	}
	return l
}

func parseLevel(s string) (zapcore.Level, error) {
	switch strings.ToLower(s) {
	case "", "info":
		return zapcore.InfoLevel, nil
	case "debug":
		return zapcore.DebugLevel, nil
	case "warn", "warning":
		return zapcore.WarnLevel, nil
	case "error":
		return zapcore.ErrorLevel, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}

// openSink returns a WriteSyncer for the given path.
// "stdout" and "stderr" map to the standard streams.
// Everything else is treated as a file path and wrapped with lumberjack.
func openSink(path string, rot config.LogRotationConfig) (zapcore.WriteSyncer, error) {
	switch path {
	case "stdout":
		return zapcore.AddSync(zapcore.Lock(zapcore.AddSync(newStdout()))), nil
	case "stderr":
		return zapcore.AddSync(zapcore.Lock(zapcore.AddSync(newStderr()))), nil
	default:
		lj := &lumberjack.Logger{
			Filename:   path,
			MaxSize:    rot.MaxSizeMB,
			MaxBackups: rot.MaxBackups,
			MaxAge:     rot.MaxAgeDays,
			Compress:   rot.Compress,
		}
		return zapcore.AddSync(lj), nil
	}
}
