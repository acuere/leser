package ingest

import (
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"leser/internal/cache"
	"leser/internal/metadata"
	"leser/internal/ratelimit"
)

// KeyResolver resolves a DSN public key to its project (metadata.DB in
// production; a fake in tests).
type KeyResolver interface {
	LookupKey(ctx context.Context, publicKey string) (metadata.ProjectKey, error)
}

// Handler serves the Sentry-compatible ingest endpoints:
//
//	POST /api/{project_id}/envelope/  — newline-delimited envelope
//	POST /api/{project_id}/store/     — legacy single-event JSON
type Handler struct {
	pipe    *Pipeline
	keys    KeyResolver
	lim     Limits
	keyLRU  *cache.LRU[metadata.ProjectKey] // DSN auth is the hottest read path
	limiter *ratelimit.Limiter              // per-project fairness
}

// NewHandler builds the ingest HTTP handler.
func NewHandler(pipe *Pipeline, keys KeyResolver, lim Limits) *Handler {
	lim.defaults()
	return &Handler{
		pipe: pipe, keys: keys, lim: lim,
		// ~200B/entry, 4MB cap ≈ 20k keys; 60s TTL bounds revocation delay.
		keyLRU:  cache.New[metadata.ProjectKey](4<<20, 60*time.Second),
		limiter: ratelimit.New(ratelimit.DefaultLimit, 10_000),
	}
}

// Limiter exposes the per-project limiter (checkpoint wiring in serve).
func (h *Handler) Limiter() *ratelimit.Limiter { return h.limiter }

// lookupKey resolves a DSN public key through the byte-bounded LRU.
func (h *Handler) lookupKey(ctx context.Context, pub string) (metadata.ProjectKey, error) {
	if k, ok := h.keyLRU.Get(pub); ok {
		return k, nil
	}
	k, err := h.keys.LookupKey(ctx, pub)
	if err != nil {
		return k, err
	}
	h.keyLRU.Set(pub, k, int64(len(pub)+len(k.PublicKey))+64)
	return k, nil
}

// Register mounts the ingest routes on mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/{project_id}/envelope/", h.handle(recEnvelope))
	mux.HandleFunc("POST /api/{project_id}/store/", h.handle(recEventJSON))
}

// sentryAuthKey extracts the public key from X-Sentry-Auth, the sentry_key
// query parameter, or HTTP basic auth — the three forms real SDKs use.
func sentryAuthKey(r *http.Request) string {
	if h := r.Header.Get("X-Sentry-Auth"); h != "" {
		for _, part := range strings.Split(strings.TrimPrefix(h, "Sentry "), ",") {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) == 2 && kv[0] == "sentry_key" {
				return strings.TrimSpace(kv[1])
			}
		}
	}
	if k := r.URL.Query().Get("sentry_key"); k != "" {
		return k
	}
	if user, _, ok := r.BasicAuth(); ok {
		return user
	}
	return ""
}

// handle returns the endpoint handler for one record kind.
func (h *Handler) handle(kind byte) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		projectID, err := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
		if err != nil {
			jsonError(w, http.StatusNotFound, "unknown project")
			return
		}
		pub := sentryAuthKey(r)
		if pub == "" {
			h.pipe.drop(DropAuth)
			jsonError(w, http.StatusUnauthorized, "missing sentry_key")
			return
		}
		key, err := h.lookupKey(r.Context(), pub)
		if err != nil || key.ProjectID != projectID {
			h.pipe.drop(DropAuth)
			// 403 for a known-shape but wrong key; do not reveal which part failed.
			jsonError(w, http.StatusForbidden, "invalid sentry_key for project")
			return
		}

		// Per-project quota fairness: one noisy project cannot starve others.
		if ok, retry := h.limiter.Allow(projectID); !ok {
			h.pipe.drop(DropOverloaded)
			secs := int(retry.Seconds() + 0.5)
			w.Header().Set("Retry-After", strconv.Itoa(secs))
			w.Header().Set("X-Sentry-Rate-Limits", strconv.Itoa(secs)+"::project")
			jsonError(w, http.StatusTooManyRequests, "project over quota")
			return
		}

		body, err := h.readBody(w, r)
		if err != nil {
			var mal *ErrMalformed
			switch {
			case errors.As(err, &mal):
				h.pipe.drop(DropMalformed)
				jsonError(w, http.StatusBadRequest, mal.Reason)
			default:
				h.pipe.drop(DropTooLarge)
				jsonError(w, http.StatusRequestEntityTooLarge, "payload too large")
			}
			return
		}

		if err := h.pipe.Submit(r.Context(), kind, projectID, body); err != nil {
			if errors.Is(err, ErrOverloaded) {
				// Backpressure signalling per order.md §3: SDKs honor these.
				w.Header().Set("Retry-After", "30")
				w.Header().Set("X-Sentry-Rate-Limits", "30::organization")
				jsonError(w, http.StatusTooManyRequests, "over quota, retry later")
				return
			}
			jsonError(w, http.StatusInternalServerError, "ingest failure")
			return
		}

		// Ack: the envelope is durable in the WAL.
		eventID := ""
		if kind == recEventJSON {
			var probe struct {
				EventID string `json:"event_id"`
			}
			_ = json.Unmarshal(body, &probe)
			eventID = probe.EventID
		}
		writeJSON(w, http.StatusOK, map[string]string{"id": eventID})
	}
}

// readBody decompresses (gzip/deflate/zstd) and bounds the request body.
// The raw body is capped by MaxBytesReader; the decompressed stream is capped
// by MaxEnvelopeBytes — both, so a zip bomb dies at the second gate.
func (h *Handler) readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	raw := http.MaxBytesReader(w, r.Body, h.lim.MaxEnvelopeBytes)
	var rd io.Reader = raw
	switch strings.ToLower(r.Header.Get("Content-Encoding")) {
	case "", "identity":
	case "gzip":
		gz, err := gzip.NewReader(raw)
		if err != nil {
			return nil, malformed("bad gzip stream")
		}
		defer gz.Close()
		rd = gz
	case "deflate":
		zl, err := zlib.NewReader(raw)
		if err != nil {
			return nil, malformed("bad deflate stream")
		}
		defer zl.Close()
		rd = zl
	case "zstd":
		zr, err := zstd.NewReader(raw, zstd.WithDecoderMaxMemory(uint64(h.lim.MaxEnvelopeBytes)))
		if err != nil {
			return nil, malformed("bad zstd stream")
		}
		defer zr.Close()
		rd = zr.IOReadCloser()
	default:
		return nil, malformed("unsupported content-encoding")
	}

	limited := io.LimitReader(rd, h.lim.MaxEnvelopeBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			return nil, err
		}
		return nil, malformed("truncated body")
	}
	if int64(len(body)) > h.lim.MaxEnvelopeBytes {
		return nil, errors.New("decompressed body exceeds limit")
	}
	return body, nil
}

func jsonError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"detail": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
