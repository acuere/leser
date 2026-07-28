package metadata

import (
	"context"
	"path/filepath"
	"testing"
)

func openIssueDB(t *testing.T) (*DB, int64, int64) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	proj, _, err := db.Bootstrap(context.Background(), "O", "P")
	if err != nil {
		t.Fatal(err)
	}
	return db, proj.OrgID, proj.ID
}

func hit(org, proj int64, fp string, ts int64) IssueUpsert {
	return IssueUpsert{OrgID: org, ProjectID: proj, Fingerprint: fp, Basis: "stacktrace",
		Title: "Boom: " + fp, Level: "error", SeenAt: ts}
}

func TestIssueLifecycle(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()

	id1, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 100))
	if err != nil {
		t.Fatal(err)
	}
	id2, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 200))
	if err != nil {
		t.Fatal(err)
	}
	if id1 != id2 {
		t.Fatalf("same fingerprint split issues: %d vs %d", id1, id2)
	}
	iss, err := db.GetIssue(ctx, proj, id1)
	if err != nil {
		t.Fatal(err)
	}
	if iss.TimesSeen != 2 || iss.FirstSeen != 100 || iss.LastSeen != 200 {
		t.Fatalf("counts: %+v", iss)
	}

	// Resolve, then a new event → regressed (never silently resolved).
	if err := db.SetIssueStatus(ctx, proj, id1, IssueResolved); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 300)); err != nil {
		t.Fatal(err)
	}
	iss, _ = db.GetIssue(ctx, proj, id1)
	if iss.Status != IssueRegressed {
		t.Fatalf("status %q, want regressed", iss.Status)
	}
}

func TestIssueMergeSplit(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()

	idA, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 100)) // 1 hit
	idB, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 100))
	_, _ = db.UpsertIssue(ctx, hit(org, proj, "fp-b", 150)) // B has 2 hits

	if err := db.MergeIssues(ctx, proj, idA, idB); err != nil {
		t.Fatal(err)
	}
	// Future fp-b events count into A.
	got, err := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 200))
	if err != nil {
		t.Fatal(err)
	}
	if got != idA {
		t.Fatalf("merged event went to %d, want %d", got, idA)
	}
	a, _ := db.GetIssue(ctx, proj, idA)
	if a.TimesSeen != 4 { // 1 + 2 merged + 1 new
		t.Fatalf("target times_seen %d, want 4", a.TimesSeen)
	}
	// Merged issues disappear from the stream.
	list, _ := db.ListIssues(ctx, proj, "", 10)
	if len(list) != 1 || list[0].ID != idA {
		t.Fatalf("stream: %+v", list)
	}

	// Split: fp-b groups independently again.
	if err := db.SplitIssue(ctx, proj, idB); err != nil {
		t.Fatal(err)
	}
	got, _ = db.UpsertIssue(ctx, hit(org, proj, "fp-b", 300))
	if got != idB {
		t.Fatalf("post-split event went to %d, want %d", got, idB)
	}
}

func TestIssueMergeChainFlattens(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()
	idA, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 1))
	idB, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 1))
	idC, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-c", 1))

	if err := db.MergeIssues(ctx, proj, idB, idC); err != nil { // C -> B
		t.Fatal(err)
	}
	if err := db.MergeIssues(ctx, proj, idA, idB); err != nil { // B -> A, C repointed
		t.Fatal(err)
	}
	c, _ := db.GetIssue(ctx, proj, idC)
	if c.MergedInto != idA {
		t.Fatalf("chain not flattened: C merged into %d, want %d", c.MergedInto, idA)
	}
	// Events on fp-c land on A in one hop.
	got, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-c", 9))
	if got != idA {
		t.Fatalf("chained event went to %d, want %d", got, idA)
	}
}

func TestIssueTenantScope(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()
	id, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 1))

	// Wrong project cannot read, mutate, or merge it.
	if _, err := db.GetIssue(ctx, proj+999, id); err != ErrNotFound {
		t.Fatalf("cross-tenant read: %v", err)
	}
	if err := db.SetIssueStatus(ctx, proj+999, id, IssueResolved); err != ErrNotFound {
		t.Fatalf("cross-tenant write: %v", err)
	}
	if err := db.SplitIssue(ctx, proj+999, id); err != ErrNotFound {
		t.Fatalf("cross-tenant split: %v", err)
	}
}
