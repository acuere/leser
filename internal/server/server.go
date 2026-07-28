// Package server wires the HTTP server, health endpoints, and embedded UI.
package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"leser/internal/buildinfo"
	"leser/internal/logging"
)

// Server holds HTTP server dependencies.
type Server struct {
	log   *slog.Logger
	ui    fs.FS
	ready atomic.Bool
	http  *http.Server
}

// New constructs a Server. ui is the embedded web asset filesystem (may be nil
// during early milestones, in which case a placeholder is served). mounts
// register additional route groups (ingest, ops) on the same mux.
func New(log *slog.Logger, addr string, ui fs.FS, mounts ...func(*http.ServeMux)) *Server {
	s := &Server{log: log, ui: ui}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.HandleFunc("GET /version", s.handleVersion)
	mux.Handle("GET /", s.handleUI())
	for _, m := range mounts {
		m(mux)
	}

	s.http = &http.Server{
		Addr:              addr,
		Handler:           s.recover(s.trace(mux)),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	return s
}

// SetReady marks the server ready to accept dependent traffic (migrations applied,
// stores reachable). /readyz returns 503 until this is set.
func (s *Server) SetReady(v bool) { s.ready.Store(v) }

// ListenAndServe blocks serving until the context is cancelled, then drains
// best-effort (crash-only: correctness comes from recovery, not shutdown).
func (s *Server) ListenAndServe(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		s.log.Info("http listening", "addr", s.http.Addr)
		errc <- s.http.ListenAndServe()
	}()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.http.Shutdown(shutCtx)
	}
}

// handleHealthz reports process liveness. It must never touch dependencies.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz reports readiness to serve dependent traffic.
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// handleVersion returns build metadata.
func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, buildinfo.Get())
}

// handleUI serves embedded static assets, or a placeholder if none are embedded.
func (s *Server) handleUI() http.Handler {
	if s.ui == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			_, _ = w.Write([]byte("<!doctype html><title>leser</title><h1>leser</h1><p>UI not built yet.</p>"))
		})
	}
	return http.FileServerFS(s.ui)
}

// trace assigns a per-request trace ID and echoes it back for correlation.
func (s *Server) trace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := newTraceID()
		w.Header().Set("X-Trace-Id", id)
		next.ServeHTTP(w, r.WithContext(logging.WithTrace(r.Context(), id)))
	})
}

// recover converts a panic in any handler into a logged 500 without killing the
// process (Section 7: errors are values; the process does not die).
func (s *Server) recover(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				id := logging.TraceID(r.Context())
				s.log.Error("panic recovered", "trace_id", id, "panic", v, "path", r.URL.Path)
				writeJSON(w, http.StatusInternalServerError, map[string]string{
					"error":    "internal",
					"trace_id": id,
				})
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// newTraceID returns a random 128-bit hex trace ID.
func newTraceID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// writeJSON writes a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
