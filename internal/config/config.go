// Package config resolves configuration with the precedence:
//
//	flags > env vars > config file > defaults
//
// Every option is settable by env var. Secrets are redacted by Effective.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// Config holds all runtime options. JSON tags double as config-file keys.
// The `env` tag is the environment variable; `secret:"true"` redacts the value
// in Effective output.
type Config struct {
	// ListenAddr is the host:port the HTTP server binds.
	ListenAddr string `json:"listen_addr" env:"LESER_LISTEN"`
	// DataDir is the directory for SQLite metadata + the local event store.
	DataDir string `json:"data_dir" env:"LESER_DATA_DIR"`
	// LogLevel is one of debug|info|warn|error.
	LogLevel string `json:"log_level" env:"LESER_LOG_LEVEL"`
	// PublicURL is the externally reachable base URL, used to build DSNs.
	PublicURL string `json:"public_url" env:"LESER_PUBLIC_URL"`
	// SecretKey signs sessions and tokens. Auto-generated on first boot if empty.
	SecretKey string `json:"secret_key" env:"LESER_SECRET_KEY" secret:"true"`
}

// Default returns the baseline configuration with zero external dependencies.
func Default() Config {
	return Config{
		ListenAddr: ":8080",
		DataDir:    "./data",
		LogLevel:   "info",
		PublicURL:  "http://localhost:8080",
		SecretKey:  "",
	}
}

// Load builds a Config by layering: defaults, then the config file at path (if
// non-empty and present), then environment variables. Flag overrides are applied
// separately by the caller via Set, because only the caller knows which flags the
// user explicitly provided.
func Load(path string) (Config, error) {
	c := Default()
	if path != "" {
		if err := c.applyFile(path); err != nil {
			return c, err
		}
	}
	c.applyEnv(os.LookupEnv)
	return c, nil
}

// applyFile overlays a JSON config file. A missing file at an explicit path is an
// error; callers that treat the file as optional should check existence first.
func (c *Config) applyFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file %q: %w", path, err)
	}
	if err := json.Unmarshal(b, c); err != nil {
		return fmt.Errorf("parse config file %q: %w", path, err)
	}
	return nil
}

// applyEnv overlays environment variables. lookup is injected for testability.
func (c *Config) applyEnv(lookup func(string) (string, bool)) {
	set := func(dst *string, key string) {
		if v, ok := lookup(key); ok {
			*dst = v
		}
	}
	set(&c.ListenAddr, "LESER_LISTEN")
	set(&c.DataDir, "LESER_DATA_DIR")
	set(&c.LogLevel, "LESER_LOG_LEVEL")
	set(&c.PublicURL, "LESER_PUBLIC_URL")
	set(&c.SecretKey, "LESER_SECRET_KEY")
}

// Effective returns the config as a map with secret fields redacted, for
// `leser config show --effective`.
func (c Config) Effective() map[string]string {
	redact := func(v string) string {
		if v == "" {
			return ""
		}
		return "***redacted***"
	}
	return map[string]string{
		"listen_addr": c.ListenAddr,
		"data_dir":    c.DataDir,
		"log_level":   c.LogLevel,
		"public_url":  c.PublicURL,
		"secret_key":  redact(c.SecretKey),
	}
}

// Validate checks invariants that must hold before the server boots.
func (c Config) Validate() error {
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("listen_addr must not be empty")
	}
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("data_dir must not be empty")
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log_level %q invalid (want debug|info|warn|error)", c.LogLevel)
	}
	return nil
}
