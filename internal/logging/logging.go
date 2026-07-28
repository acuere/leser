// Package logging provides a structured JSON logger built on log/slog (stdlib).
package logging

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

// ParseLevel maps a human level string to slog.Level. Unknown -> Info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// New returns a JSON slog.Logger writing to stderr at the given level.
func New(level string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: ParseLevel(level),
	})
	return slog.New(h)
}

// traceKey is the context key type for the request trace ID.
type traceKey struct{}

// WithTrace stores a trace ID on the context.
func WithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceKey{}, id)
}

// TraceID extracts the trace ID from context, or "" if absent.
func TraceID(ctx context.Context) string {
	if v, ok := ctx.Value(traceKey{}).(string); ok {
		return v
	}
	return ""
}
