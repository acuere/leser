package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"leser/internal/logging"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	return New(logging.New("error"), ":0", nil)
}

func TestHealthzAlwaysOK(t *testing.T) {
	s := newTestServer(t)
	rr := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("healthz got %d", rr.Code)
	}
}

func TestReadyzGatedOnReady(t *testing.T) {
	s := newTestServer(t)
	rr := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready: got %d want 503", rr.Code)
	}
	s.SetReady(true)
	rr = httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("readyz after ready: got %d want 200", rr.Code)
	}
}

func TestTraceHeaderSet(t *testing.T) {
	s := newTestServer(t)
	rr := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rr.Header().Get("X-Trace-Id") == "" {
		t.Fatal("X-Trace-Id must be set on every response")
	}
}

func TestPanicRecovered(t *testing.T) {
	s := newTestServer(t)
	// Wrap a panicking handler with the same middleware chain.
	h := s.recover(s.trace(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("boom")
	})))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("panic must yield 500, got %d", rr.Code)
	}
	body, _ := io.ReadAll(rr.Body)
	if len(body) == 0 {
		t.Fatal("expected error body with trace id")
	}
}
