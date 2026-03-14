package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/mikey-austin/tierfs/internal/config"
)

// Server is a lightweight HTTP server that exposes the /metrics endpoint.
type Server struct {
	srv *http.Server
	mux *http.ServeMux
	log *zap.Logger
}

// NewServer constructs a Server from MetricsConfig. Returns nil if disabled.
func NewServer(cfg config.MetricsConfig, reg *Registry, log *zap.Logger) *Server {
	if !cfg.Enabled {
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle(cfg.Path, reg.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		fmt.Fprintln(w, "ok")
	})
	return &Server{
		srv: &http.Server{
			Addr:         cfg.Address,
			Handler:      mux,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  30 * time.Second,
		},
		mux: mux,
		log: log.Named("metrics-server"),
	}
}

// Start begins serving in a background goroutine. Non-blocking.
func (s *Server) Start() {
	if s == nil {
		return
	}
	go func() {
		s.log.Info("metrics server listening", zap.String("addr", s.srv.Addr))
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("metrics server error", zap.Error(err))
		}
	}()
}

// Mux returns the underlying ServeMux so additional routes can be registered.
func (s *Server) Mux() *http.ServeMux {
	if s == nil {
		return nil
	}
	return s.mux
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	return s.srv.Shutdown(ctx)
}
