package search

import "testing"

func TestParseFull(t *testing.T) {
	f, err := Parse(`is:unresolved level:error release:"1.2 beta" env:prod user:u-9 timeout connecting`)
	if err != nil {
		t.Fatal(err)
	}
	if f.Status != "unresolved" || f.Level != "error" || f.Release != "1.2 beta" ||
		f.Environment != "prod" || f.UserID != "u-9" || f.Text != "timeout connecting" {
		t.Fatalf("parsed: %+v", f)
	}
	if !f.NeedsEventLookup() {
		t.Fatal("release filter must require event lookup")
	}
}

func TestParseTextOnly(t *testing.T) {
	f, err := Parse("database connection lost")
	if err != nil {
		t.Fatal(err)
	}
	if f.Text != "database connection lost" || f.NeedsEventLookup() {
		t.Fatalf("parsed: %+v", f)
	}
}

func TestParseRejectsUnknownKey(t *testing.T) {
	if _, err := Parse("levle:error"); err == nil {
		t.Fatal("typo key must error, not silently match everything")
	}
	if _, err := Parse("is:resloved"); err == nil {
		t.Fatal("invalid status must error")
	}
}

func TestParseEmpty(t *testing.T) {
	f, err := Parse("")
	if err != nil || f != (Filter{}) {
		t.Fatalf("empty query: %+v %v", f, err)
	}
}
