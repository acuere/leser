package auth

import (
	"strings"
	"testing"
)

func TestPasswordRoundtrip(t *testing.T) {
	h, err := HashPassword("correct horse battery staple")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(h, "$argon2id$") {
		t.Fatalf("hash format: %q", h)
	}
	if !VerifyPassword("correct horse battery staple", h) {
		t.Fatal("correct password rejected")
	}
	if VerifyPassword("wrong", h) {
		t.Fatal("wrong password accepted")
	}
	// Unique salts.
	h2, _ := HashPassword("correct horse battery staple")
	if h == h2 {
		t.Fatal("salt reuse")
	}
}

func TestVerifyGarbageHash(t *testing.T) {
	for _, h := range []string{"", "$argon2id$", "plaintext", "$argon2id$v=19$m=x$y$z"} {
		if VerifyPassword("x", h) {
			t.Fatalf("garbage hash %q verified", h)
		}
	}
}

func TestTokenShape(t *testing.T) {
	plain, hash, err := NewToken()
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateTokenShape(plain); err != nil {
		t.Fatalf("fresh token invalid: %v", err)
	}
	if HashToken(plain) != hash {
		t.Fatal("hash mismatch")
	}
	for _, bad := range []string{"", "leser_pat_short", "other_pat_" + strings.Repeat("a", 64), plain + "x"} {
		if ValidateTokenShape(bad) == nil {
			t.Fatalf("bad token %q accepted", bad)
		}
	}
	// Uniqueness.
	p2, _, _ := NewToken()
	if plain == p2 {
		t.Fatal("token reuse")
	}
}
