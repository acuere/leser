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

	o1, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 100))
	if err != nil {
		t.Fatal(err)
	}
	o2, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 200))
	if err != nil {
		t.Fatal(err)
	}
	if o1.ID != o2.ID {
		t.Fatalf("same fingerprint split issues: %d vs %d", o1.ID, o2.ID)
	}
	if !o1.IsNew || o2.IsNew {
		t.Fatalf("IsNew flags wrong: %+v %+v", o1, o2)
	}
	id1 := o1.ID
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
	o3, err := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 300))
	if err != nil {
		t.Fatal(err)
	}
	if !o3.Regressed {
		t.Fatal("regression not reported in outcome")
	}
	iss, _ = db.GetIssue(ctx, proj, id1)
	if iss.Status != IssueRegressed {
		t.Fatalf("status %q, want regressed", iss.Status)
	}
}

func TestIssueMergeSplit(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()

	oA, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 100)) // 1 hit
	idA := oA.ID
	oB, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 100))
	idB := oB.ID
	_, _ = db.UpsertIssue(ctx, hit(org, proj, "fp-b", 150)) // B has 2 hits

	if err := db.MergeIssues(ctx, proj, idA, idB); err != nil {
		t.Fatal(err)
	}
	// Future fp-b events count into A.
	gotO, err := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 200))
	if err != nil {
		t.Fatal(err)
	}
	got := gotO.ID
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
	gotO2, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 300))
	got = gotO2.ID
	if got != idB {
		t.Fatalf("post-split event went to %d, want %d", got, idB)
	}
}

func TestIssueMergeChainFlattens(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()
	oA, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 1))
	idA := oA.ID
	oB, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-b", 1))
	idB := oB.ID
	oC, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-c", 1))
	idC := oC.ID

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
	gotO3, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-c", 9))
	got := gotO3.ID
	if got != idA {
		t.Fatalf("chained event went to %d, want %d", got, idA)
	}
}

func TestIssueTenantScope(t *testing.T) {
	db, org, proj := openIssueDB(t)
	ctx := context.Background()
	oT, _ := db.UpsertIssue(ctx, hit(org, proj, "fp-a", 1))
	id := oT.ID

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
