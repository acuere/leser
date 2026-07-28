package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultValid(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("default config must be valid: %v", err)
	}
}

func TestPrecedenceFileThenEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cfg.json")
	if err := os.WriteFile(path, []byte(`{"listen_addr":":9090","log_level":"debug"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	c := Default()
	if err := c.applyFile(path); err != nil {
		t.Fatal(err)
	}
	if c.ListenAddr != ":9090" {
		t.Errorf("file override: got %q", c.ListenAddr)
	}
	// Env overrides file.
	env := map[string]string{"LESER_LISTEN": ":7070"}
	c.applyEnv(func(k string) (string, bool) { v, ok := env[k]; return v, ok })
	if c.ListenAddr != ":7070" {
		t.Errorf("env must override file: got %q", c.ListenAddr)
	}
	if c.LogLevel != "debug" {
		t.Errorf("file value must survive when env absent: got %q", c.LogLevel)
	}
}

func TestEffectiveRedactsSecret(t *testing.T) {
	c := Default()
	c.SecretKey = "super-secret"
	eff := c.Effective()
	if eff["secret_key"] == "super-secret" {
		t.Fatal("secret_key must be redacted")
	}
	if eff["secret_key"] != "***redacted***" {
		t.Errorf("got %q", eff["secret_key"])
	}
}

func TestValidateRejectsBadLevel(t *testing.T) {
	c := Default()
	c.LogLevel = "loud"
	if err := c.Validate(); err == nil {
		t.Fatal("expected error for bad log level")
	}
}

func TestMissingConfigFileIsError(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.json")); err == nil {
		t.Fatal("explicit missing config file must error")
	}
}
