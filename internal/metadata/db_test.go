package metadata

import (
	"context"
	"path/filepath"
	"testing"
)

func TestMigrateAndBootstrap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	proj, key, err := db.Bootstrap(ctx, "Acme Org", "My App")
	if err != nil {
		t.Fatal(err)
	}
	if proj.ID == 0 || key.PublicKey == "" || !key.Active {
		t.Fatalf("bootstrap: proj=%+v key=%+v", proj, key)
	}
	if key.ProjectID != proj.ID {
		t.Fatal("key not bound to project")
	}

	// Idempotent: second call returns the same identities.
	proj2, key2, err := db.Bootstrap(ctx, "Other", "Other")
	if err != nil {
		t.Fatal(err)
	}
	if proj2.ID != proj.ID || key2.PublicKey != key.PublicKey {
		t.Fatal("bootstrap not idempotent")
	}
}

func TestLookupKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	_, key, err := db.Bootstrap(ctx, "O", "P")
	if err != nil {
		t.Fatal(err)
	}

	got, err := db.LookupKey(ctx, key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if got.ProjectID != key.ProjectID {
		t.Fatalf("lookup: %+v", got)
	}
	if _, err := db.LookupKey(ctx, "nope"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestReopenKeepsData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	_, key, err := db.Bootstrap(ctx, "O", "P")
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	db2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	if _, err := db2.LookupKey(ctx, key.PublicKey); err != nil {
		t.Fatalf("key lost across reopen: %v", err)
	}
}
