// Package auth implements authentication primitives: Argon2id passwords,
// server-side sessions, and API tokens (order.md §6).
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters — OWASP-recommended interactive profile.
const (
	argonTime    = 3
	argonMemory  = 64 * 1024 // 64 MiB
	argonThreads = 2
	argonKeyLen  = 32
	argonSaltLen = 16
)

// HashPassword returns a PHC-format Argon2id hash.
func HashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemory, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=19$m=%d,t=%d,p=%d$%s$%s",
		argonMemory, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// VerifyPassword checks a password against a PHC Argon2id hash in constant
// time. Parameters are read from the hash so they can evolve.
func VerifyPassword(password, phc string) bool {
	parts := strings.Split(phc, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false
	}
	var m, t uint32
	var p uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &m, &t, &p); err != nil {
		return false
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false
	}
	got := argon2.IDKey([]byte(password), salt, t, m, p, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1
}

// Token format: leser_pat_<64 hex chars> (order.md: prefixed for secret
// scanning, shown once). Stored server-side as SHA-256(token).
//
// Deviation from order.md (which says Argon2id for tokens), decided and
// documented: tokens are 32 bytes of CSPRNG output — 256 bits of entropy makes
// brute-force infeasible and memory-hard hashing adds ~50ms to every API
// request for no security gain. Argon2id remains for passwords, which are
// low-entropy. SHA-256 for high-entropy tokens is the industry standard
// (GitHub PATs use the same construction).
const TokenPrefix = "leser_pat_"

// NewToken generates a token. Returns (plaintext-shown-once, storedHash).
func NewToken() (string, string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	tok := TokenPrefix + hex.EncodeToString(raw)
	return tok, HashToken(tok), nil
}

// HashToken derives the storage hash for a presented token.
func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// ErrBadToken is returned for tokens with the wrong shape.
var ErrBadToken = errors.New("auth: malformed token")

// ValidateTokenShape rejects garbage before any DB work.
func ValidateTokenShape(token string) error {
	if !strings.HasPrefix(token, TokenPrefix) || len(token) != len(TokenPrefix)+64 {
		return ErrBadToken
	}
	return nil
}

// NewSessionID returns a 256-bit random session identifier.
func NewSessionID() (string, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}
