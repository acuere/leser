package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"leser/internal/auth"
	"leser/internal/authz"
	"leser/internal/logging"
	"leser/internal/metadata"
)

// TestRouteTableComplete is the build-time enforcement (order.md §6): every
// route carries exactly one of {explicit permission, Public marker}; project
// routes declare object scope; permissions are known constants; no duplicates.
func TestRouteTableComplete(t *testing.T) {
	known := map[authz.Permission]bool{}
	for _, p := range authz.AllPermissions {
		known[p] = true
	}
	seen := map[string]bool{}
	for _, rt := range Routes() {
		key := rt.Method + " " + rt.Path
		if seen[key] {
			t.Errorf("%s: duplicate route", key)
		}
		seen[key] = true
		if rt.Public && rt.Perm != "" {
			t.Errorf("%s: both Public and Perm set", key)
		}
		if !rt.Public && rt.Perm == "" {
			t.Errorf("%s: NO PERMISSION DECLARED — deny-by-default violated", key)
		}
		if rt.Perm != "" && !known[rt.Perm] {
			t.Errorf("%s: unknown permission %q", key, rt.Perm)
		}
		if strings.Contains(rt.Path, "{project_id}") != rt.Project {
			t.Errorf("%s: Project flag does not match path pattern", key)
		}
		if rt.Handle == nil {
			t.Errorf("%s: nil handler", key)
		}
	}
}

// --- integration fixture: two orgs, users, projects ---

type fixture struct {
	api   *API
	mux   *http.ServeMux
	meta  *metadata.DB
	orgA  int64
	orgB  int64
	projA int64
	projB int64
	issA  int64
	issB  int64
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	meta, err := metadata.Open(filepath.Join(t.TempDir(), "meta.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { meta.Close() })
	ctx := context.Background()

	orgA, _ := meta.CreateOrg(ctx, "OrgA")
	orgB, _ := meta.CreateOrg(ctx, "OrgB")
	pA, _, err := meta.CreateProject(ctx, orgA, "AppA", "go")
	if err != nil {
		t.Fatal(err)
	}
	pB, _, err := meta.CreateProject(ctx, orgB, "AppB", "python")
	if err != nil {
		t.Fatal(err)
	}
	oA, _ := meta.UpsertIssue(ctx, metadata.IssueUpsert{OrgID: orgA, ProjectID: pA.ID, Fingerprint: "fpA", Title: "A boom", Level: "error", SeenAt: 1})
	issA := oA.ID
	oB, _ := meta.UpsertIssue(ctx, metadata.IssueUpsert{OrgID: orgB, ProjectID: pB.ID, Fingerprint: "fpB", Title: "B boom", Level: "error", SeenAt: 1})
	issB := oB.ID

	a := New(logging.New("error"), meta,
		func() any { return map[string]int{"ok": 1} },
		func() any { return map[string]string{"cfg": "x"} })
	mux := http.NewServeMux()
	a.Register(mux)
	return &fixture{api: a, mux: mux, meta: meta, orgA: orgA, orgB: orgB, projA: pA.ID, projB: pB.ID, issA: issA, issB: issB}
}

// mkUser creates a user with a password and returns a live session cookie.
func (f *fixture) mkUser(t *testing.T, org int64, email, role string) *http.Cookie {
	t.Helper()
	hash, err := auth.HashPassword("hunter2!secret")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.meta.CreateUser(context.Background(), org, email, hash, role); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(map[string]string{"email": email, "password": "hunter2!secret"})
	rr := f.do(t, http.MethodPost, "/api/0/login", body, nil, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("login %s: %d %s", email, rr.Code, rr.Body.String())
	}
	for _, c := range rr.Result().Cookies() {
		if c.Name == SessionCookie {
			return c
		}
	}
	t.Fatal("no session cookie")
	return nil
}

func (f *fixture) do(t *testing.T, method, path string, body []byte, cookie *http.Cookie, bearer string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rr := httptest.NewRecorder()
	f.mux.ServeHTTP(rr, req)
	return rr
}

func TestUnauthenticatedIs401(t *testing.T) {
	f := newFixture(t)
	for _, rt := range Routes() {
		if rt.Public {
			continue
		}
		path := strings.NewReplacer("{project_id}", "1", "{issue_id}", "1", "{token_id}", "1").Replace(rt.Path)
		rr := f.do(t, rt.Method, path, []byte(`{}`), nil, "")
		if rr.Code != http.StatusUnauthorized {
			t.Errorf("%s %s unauthenticated: got %d want 401", rt.Method, path, rr.Code)
		}
	}
}

// TestAdversarialCrossTenant: a user in Org A attempts every project-scoped
// route against Org B's resources and must get 403/404 for all of them
// (order.md §6). Generated from the route table, not hand-written.
func TestAdversarialCrossTenant(t *testing.T) {
	f := newFixture(t)
	cookie := f.mkUser(t, f.orgA, "owner-a@a.test", authz.RoleOwner)

	for _, rt := range Routes() {
		if !rt.Project {
			continue
		}
		path := strings.NewReplacer(
			"{project_id}", fmt.Sprint(f.projB), // victim project
			"{issue_id}", fmt.Sprint(f.issB),
			"{token_id}", "1",
		).Replace(rt.Path)
		body := []byte(`{"status":"resolved","target_id":1,"source_ids":[2]}`)
		rr := f.do(t, rt.Method, path, body, cookie, "")
		if rr.Code != http.StatusNotFound && rr.Code != http.StatusForbidden {
			t.Errorf("CROSS-TENANT LEAK: %s %s got %d, want 403/404", rt.Method, path, rr.Code)
		}
	}
	// And Org B's issue must be untouched.
	iss, err := f.meta.GetIssue(context.Background(), f.projB, f.issB)
	if err != nil || iss.Status != metadata.IssueUnresolved {
		t.Fatalf("victim issue mutated: %+v err=%v", iss, err)
	}
}

func TestRBACDenies(t *testing.T) {
	f := newFixture(t)
	ro := f.mkUser(t, f.orgA, "ro@a.test", authz.RoleReadOnly)

	// read_only can list issues…
	rr := f.do(t, http.MethodGet, fmt.Sprintf("/api/0/projects/%d/issues", f.projA), nil, ro, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("read_only list: %d", rr.Code)
	}
	// …but cannot resolve them.
	rr = f.do(t, http.MethodPut, fmt.Sprintf("/api/0/projects/%d/issues/%d/status", f.projA, f.issA),
		[]byte(`{"status":"resolved"}`), ro, "")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("read_only write: got %d want 403", rr.Code)
	}
	// …and cannot read the audit log.
	rr = f.do(t, http.MethodGet, "/api/0/audit", nil, ro, "")
	if rr.Code != http.StatusForbidden {
		t.Fatalf("read_only audit: got %d want 403", rr.Code)
	}
}

func TestSessionFlowAndTokens(t *testing.T) {
	f := newFixture(t)
	cookie := f.mkUser(t, f.orgA, "own@a.test", authz.RoleOwner)

	// Session works.
	rr := f.do(t, http.MethodGet, "/api/0/me", nil, cookie, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("me: %d", rr.Code)
	}

	// Create a token restricted to issue:read.
	rr = f.do(t, http.MethodPost, "/api/0/tokens",
		[]byte(`{"name":"ci","permissions":["issue:read","project:read"]}`), cookie, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("token create: %d %s", rr.Code, rr.Body.String())
	}
	var tok struct {
		ID    int64  `json:"id"`
		Token string `json:"token"`
	}
	json.Unmarshal(rr.Body.Bytes(), &tok)
	if !strings.HasPrefix(tok.Token, auth.TokenPrefix) {
		t.Fatalf("token shape: %q", tok.Token)
	}

	// Token can read…
	rr = f.do(t, http.MethodGet, fmt.Sprintf("/api/0/projects/%d/issues", f.projA), nil, nil, tok.Token)
	if rr.Code != http.StatusOK {
		t.Fatalf("token read: %d", rr.Code)
	}
	// …cannot write (strict subset, even though the owner could).
	rr = f.do(t, http.MethodPut, fmt.Sprintf("/api/0/projects/%d/issues/%d/status", f.projA, f.issA),
		[]byte(`{"status":"resolved"}`), nil, tok.Token)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("restricted token write: got %d want 403", rr.Code)
	}

	// Revoke → immediately dead.
	rr = f.do(t, http.MethodDelete, fmt.Sprintf("/api/0/tokens/%d", tok.ID), nil, cookie, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("revoke: %d", rr.Code)
	}
	rr = f.do(t, http.MethodGet, fmt.Sprintf("/api/0/projects/%d/issues", f.projA), nil, nil, tok.Token)
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("revoked token: got %d want 401", rr.Code)
	}

	// Logout revokes the session server-side.
	rr = f.do(t, http.MethodPost, "/api/0/logout", nil, cookie, "")
	if rr.Code != http.StatusOK {
		t.Fatalf("logout: %d", rr.Code)
	}
	rr = f.do(t, http.MethodGet, "/api/0/me", nil, cookie, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("dead session: got %d want 401", rr.Code)
	}
}

func TestBadLoginAudited(t *testing.T) {
	f := newFixture(t)
	f.mkUser(t, f.orgA, "u@a.test", authz.RoleMember)
	rr := f.do(t, http.MethodPost, "/api/0/login",
		[]byte(`{"email":"u@a.test","password":"wrong"}`), nil, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("bad login: %d", rr.Code)
	}
	rr = f.do(t, http.MethodPost, "/api/0/login",
		[]byte(`{"email":"ghost@a.test","password":"wrong"}`), nil, "")
	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("unknown user login: %d", rr.Code)
	}
}
