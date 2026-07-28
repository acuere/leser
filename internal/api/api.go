// Package api is the authenticated REST API (order.md §6, §9): every route is
// declared in one table with an explicit permission or Public marker — a test
// fails the build if any route lacks one. The UI is a client of this API only.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"leser/internal/alerts"
	"leser/internal/auth"
	"leser/internal/authz"
	"leser/internal/metadata"
	"leser/internal/search"
)

// SessionCookie is the session cookie name.
const SessionCookie = "leser_session"

// SessionTTL bounds session lifetime.
const SessionTTL = 7 * 24 * time.Hour

// Route is one declarative API route. Exactly one of Perm or Public applies.
type Route struct {
	Method  string
	Path    string // net/http pattern, e.g. /api/0/projects/{project_id}/issues
	Perm    authz.Permission
	Public  bool
	Project bool // pattern contains {project_id}: object-level scope enforced
	Handle  func(*API, *authz.AuthContext, http.ResponseWriter, *http.Request)
}

// API carries dependencies for handlers.
type API struct {
	log  *slog.Logger
	meta *metadata.DB
	// StatusFn supplies control-plane status (pipeline snapshot).
	StatusFn func() any
	// ConfigFn supplies redacted effective config.
	ConfigFn func() any
	// StatsFn supplies event aggregates for a project window (event store).
	StatsFn func(projectID, timeMin, timeMax, bucketNanos int64) (any, error)
	// EventsFn supplies recent raw events for a fingerprint (event store).
	// Gated behind event:read — raw payloads carry PII.
	EventsFn func(projectID int64, fingerprint string, limit int) (any, error)
	// DSNFor renders a project's ingest DSN for display (public URL lives in
	// config, which this package does not import).
	DSNFor func(publicKey string, projectID int64) string
	// FingerprintsFn resolves event-level search filters (release/env/user) to
	// the distinct fingerprints matching them, bounded (event store).
	FingerprintsFn func(projectID int64, release, environment, userID string) ([]string, error)
	// Locator enables Rung 3 request routing (order-2 §3): when set, every
	// project-scoped request is checked against project ownership and
	// proxied to the owning node if this node isn't it — "any node can serve
	// any API request by proxying to the owner." Nil means single-node
	// (Rung 0-2) behavior: always handle locally.
	Locator Locator
	proxies sync.Map // owner addr -> *httputil.ReverseProxy, reused across requests
	// NodeID, if set, is stamped on X-Leser-Node for every response this node
	// handles LOCALLY (never set when proxying, so a proxied response keeps
	// the owning node's stamp intact) — lets a caller, or a test, tell which
	// physical node actually served a request regardless of which node's
	// port it asked.
	NodeID string
}

// Locator resolves which node owns a project, for cross-node proxying.
// *cluster.Membership implements this; defined here (not imported from
// internal/cluster) so this package has no dependency on gossip/hashing
// internals — it only needs the routing decision.
type Locator interface {
	// OwnerAddr returns the API base address owning projectID and true, or
	// ("", false) if this node is the owner (or ownership isn't yet known,
	// in which case handling locally is the safe fallback).
	OwnerAddr(projectID int64) (addr string, isRemote bool)
}

// New builds the API.
func New(log *slog.Logger, meta *metadata.DB, statusFn, configFn func() any) *API {
	return &API{log: log, meta: meta, StatusFn: statusFn, ConfigFn: configFn}
}

// Routes is THE route table. Adding a handler anywhere else is a build error
// by convention and a test failure by enforcement.
func Routes() []Route {
	return []Route{
		{Method: "POST", Path: "/api/0/login", Public: true, Handle: (*API).handleLogin},
		{Method: "POST", Path: "/api/0/logout", Public: true, Handle: (*API).handleLogout},
		{Method: "GET", Path: "/api/0/me", Perm: authz.OrgRead, Handle: (*API).handleMe},
		{Method: "GET", Path: "/api/0/projects", Perm: authz.ProjectRead, Handle: (*API).handleProjects},
		{Method: "GET", Path: "/api/0/projects/{project_id}/stats", Perm: authz.IssueRead, Project: true, Handle: (*API).handleStats},
		{Method: "GET", Path: "/api/0/projects/{project_id}/issues", Perm: authz.IssueRead, Project: true, Handle: (*API).handleIssueList},
		{Method: "GET", Path: "/api/0/projects/{project_id}/issues/{issue_id}", Perm: authz.IssueRead, Project: true, Handle: (*API).handleIssueGet},
		{Method: "GET", Path: "/api/0/projects/{project_id}/issues/{issue_id}/events", Perm: authz.EventRead, Project: true, Handle: (*API).handleIssueEvents},
		{Method: "PUT", Path: "/api/0/projects/{project_id}/issues/{issue_id}/status", Perm: authz.IssueWrite, Project: true, Handle: (*API).handleIssueStatus},
		{Method: "POST", Path: "/api/0/projects/{project_id}/issues/merge", Perm: authz.IssueWrite, Project: true, Handle: (*API).handleIssueMerge},
		{Method: "POST", Path: "/api/0/projects/{project_id}/issues/{issue_id}/split", Perm: authz.IssueWrite, Project: true, Handle: (*API).handleIssueSplit},
		{Method: "GET", Path: "/api/0/projects/{project_id}/alerts", Perm: authz.AlertsWrite, Project: true, Handle: (*API).handleAlertList},
		{Method: "POST", Path: "/api/0/projects/{project_id}/alerts", Perm: authz.AlertsWrite, Project: true, Handle: (*API).handleAlertCreate},
		{Method: "DELETE", Path: "/api/0/projects/{project_id}/alerts/{rule_id}", Perm: authz.AlertsWrite, Project: true, Handle: (*API).handleAlertDelete},
		{Method: "GET", Path: "/api/0/ops/status", Perm: authz.OpsRead, Handle: (*API).handleOpsStatus},
		{Method: "GET", Path: "/api/0/ops/config", Perm: authz.OpsRead, Handle: (*API).handleOpsConfig},
		{Method: "GET", Path: "/api/0/audit", Perm: authz.AuditRead, Handle: (*API).handleAudit},
		{Method: "POST", Path: "/api/0/tokens", Perm: authz.TokenAdmin, Handle: (*API).handleTokenCreate},
		{Method: "DELETE", Path: "/api/0/tokens/{token_id}", Perm: authz.TokenAdmin, Handle: (*API).handleTokenRevoke},
	}
}

// Register mounts every route with the auth middleware.
func (a *API) Register(mux *http.ServeMux) {
	for _, rt := range Routes() {
		rt := rt
		mux.HandleFunc(rt.Method+" "+rt.Path, func(w http.ResponseWriter, r *http.Request) {
			a.dispatch(rt, w, r)
		})
	}
}

// dispatch authenticates, authorizes, then calls the handler. Deny by default.
func (a *API) dispatch(rt Route, w http.ResponseWriter, r *http.Request) {
	var actx *authz.AuthContext
	if !rt.Public {
		var ok bool
		actx, ok = a.authenticate(r)
		if !ok {
			writeErr(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !actx.Can(rt.Perm) {
			writeErr(w, http.StatusForbidden, "permission denied")
			return
		}
		if rt.Project {
			pid, err := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
			if err != nil || !actx.CanProject(rt.Perm, pid) {
				// 404 not 403: do not confirm the resource exists (order.md §6).
				writeErr(w, http.StatusNotFound, "not found")
				return
			}
			// Rung 3: route to the owning node if this isn't it. Runs AFTER
			// authorization so a caller with no access to the project gets
			// the same 404 regardless of which node happens to receive the
			// request — proxying never leaks project existence.
			if a.Locator != nil {
				if addr, remote := a.Locator.OwnerAddr(pid); remote {
					a.proxy(addr, w, r)
					return
				}
			}
		}
	}
	if a.NodeID != "" {
		w.Header().Set("X-Leser-Node", a.NodeID)
	}
	rt.Handle(a, actx, w, r)
}

// proxy forwards the request to another node's API and streams its response
// back verbatim. The original session cookie / bearer token travels with it
// unchanged — the owning node re-authenticates and re-authorizes
// independently (this MVP shares one metadata store across nodes, so the
// same session/token is valid everywhere; it does not trust the proxying
// node's auth decision).
func (a *API) proxy(addr string, w http.ResponseWriter, r *http.Request) {
	target, err := url.Parse(addr)
	if err != nil {
		writeErr(w, http.StatusBadGateway, "invalid owner address")
		return
	}
	rp, _ := a.proxies.LoadOrStore(addr, httputil.NewSingleHostReverseProxy(target))
	rp.(*httputil.ReverseProxy).ServeHTTP(w, r)
}

// authenticate resolves a session cookie or Bearer PAT to an AuthContext.
func (a *API) authenticate(r *http.Request) (*authz.AuthContext, bool) {
	ctx := r.Context()

	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		tok := strings.TrimPrefix(h, "Bearer ")
		if auth.ValidateTokenShape(tok) != nil {
			return nil, false
		}
		t, u, err := a.meta.TokenByHash(ctx, auth.HashToken(tok))
		if err != nil {
			return nil, false
		}
		actx := a.contextFor(u)
		if len(t.Permissions) > 0 {
			perms := make([]authz.Permission, 0, len(t.Permissions))
			for _, p := range t.Permissions {
				perms = append(perms, authz.Permission(p))
			}
			actx = actx.Restrict(perms) // strict subset, never escalation
		}
		return actx, true
	}

	if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
		u, err := a.meta.SessionUser(ctx, c.Value)
		if err != nil {
			return nil, false
		}
		return a.contextFor(u), true
	}
	return nil, false
}

// contextFor builds the AuthContext with the object-level tenancy oracle.
func (a *API) contextFor(u metadata.User) *authz.AuthContext {
	orgID := u.OrgID
	return authz.NewAuthContext(u.ID, orgID, u.Role, nil, func(projectID int64) bool {
		return a.meta.ProjectInOrg(context.Background(), orgID, projectID)
	})
}

// --- handlers ---

func (a *API) handleLogin(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	var req struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request body")
		return
	}
	u, err := a.meta.UserByEmail(r.Context(), req.Email)
	// Constant-shape response: verify against a dummy hash when unknown so
	// timing does not reveal account existence.
	hash := "$argon2id$v=19$m=65536,t=3,p=2$AAAAAAAAAAAAAAAAAAAAAA$AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	if err == nil {
		hash = u.PasswordHash
	}
	if !auth.VerifyPassword(req.Password, hash) || err != nil {
		a.audit(r, 0, 0, "login.failed", req.Email, "")
		writeErr(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	sid, err := auth.NewSessionID()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "session")
		return
	}
	if err := a.meta.CreateSession(r.Context(), sid, u.ID, SessionTTL); err != nil {
		writeErr(w, http.StatusInternalServerError, "session")
		return
	}
	http.SetCookie(w, &http.Cookie{
		Name: SessionCookie, Value: sid, Path: "/",
		HttpOnly: true, Secure: r.TLS != nil, SameSite: http.SameSiteLaxMode,
		MaxAge: int(SessionTTL.Seconds()),
	})
	a.audit(r, u.OrgID, u.ID, "login.success", u.Email, "")
	writeJSON(w, http.StatusOK, map[string]any{"email": u.Email, "role": u.Role})
}

func (a *API) handleLogout(_ *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	if c, err := r.Cookie(SessionCookie); err == nil && c.Value != "" {
		_ = a.meta.RevokeSession(r.Context(), c.Value)
	}
	http.SetCookie(w, &http.Cookie{Name: SessionCookie, Value: "", Path: "/", MaxAge: -1, HttpOnly: true})
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleMe(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"user_id": actx.UserID, "org_id": actx.OrgID, "role": actx.Role,
	})
}

func (a *API) handleProjects(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	ps, err := a.meta.ListProjects(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list projects")
		return
	}
	// Tenant filter: only the caller's org.
	type pj struct {
		metadata.ProjectInfo
		DSN string `json:"dsn,omitempty"`
	}
	out := make([]pj, 0, len(ps))
	for _, p := range ps {
		if p.OrgID != actx.OrgID {
			continue
		}
		row := pj{ProjectInfo: p}
		if a.DSNFor != nil {
			row.DSN = a.DSNFor(p.PublicKey, p.ID)
		}
		out = append(out, row)
	}
	writeJSON(w, http.StatusOK, out)
}

// handleStats serves aggregates (counts by bucket, top-N tags, unique users).
// Aggregates carry no raw payloads, so issue:read suffices — read_only roles
// see these while raw event payloads stay denied.
func (a *API) handleStats(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	if a.StatsFn == nil {
		writeErr(w, http.StatusNotImplemented, "stats not wired")
		return
	}
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	q := r.URL.Query()
	tmin, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	tmax, _ := strconv.ParseInt(q.Get("until"), 10, 64)
	bucket, _ := strconv.ParseInt(q.Get("bucket"), 10, 64)
	if tmin == 0 {
		tmin = time.Now().Add(-24 * time.Hour).UnixNano()
	}
	st, err := a.StatsFn(pid, tmin, tmax, bucket)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "aggregate")
		return
	}
	writeJSON(w, http.StatusOK, st)
}

func (a *API) handleIssueList(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))

	// The search query language: ?query=is:unresolved level:error boom
	// (?status= kept as a shorthand for older callers).
	f := metadata.IssueFilter{Status: r.URL.Query().Get("status")}
	if q := r.URL.Query().Get("query"); q != "" {
		parsed, err := search.Parse(q)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if parsed.Status != "" {
			f.Status = parsed.Status
		}
		f.Level = parsed.Level
		f.TitleLike = parsed.Text
		f.Fingerprint = parsed.Fingerprint
		if parsed.NeedsEventLookup() {
			if a.FingerprintsFn == nil {
				writeErr(w, http.StatusNotImplemented, "event-backed search not wired")
				return
			}
			fps, err := a.FingerprintsFn(pid, parsed.Release, parsed.Environment, parsed.UserID)
			if err != nil {
				writeErr(w, http.StatusInternalServerError, "event search")
				return
			}
			if fps == nil {
				fps = []string{} // non-nil empty = matched nothing
			}
			f.Fingerprints = fps
		}
	}

	issues, err := a.meta.ListIssuesFiltered(r.Context(), pid, f, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list issues")
		return
	}
	if issues == nil {
		issues = []metadata.Issue{}
	}
	writeJSON(w, http.StatusOK, issues)
}

func (a *API) handleIssueGet(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	iid, _ := strconv.ParseInt(r.PathValue("issue_id"), 10, 64)
	iss, err := a.meta.GetIssue(r.Context(), pid, iid)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get issue")
		return
	}
	writeJSON(w, http.StatusOK, iss)
}

// handleIssueEvents returns recent raw events for an issue's fingerprint.
// event:read only — read_only roles get 403 here while aggregates stay open.
func (a *API) handleIssueEvents(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	if a.EventsFn == nil {
		writeErr(w, http.StatusNotImplemented, "events not wired")
		return
	}
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	iid, _ := strconv.ParseInt(r.PathValue("issue_id"), 10, 64)
	iss, err := a.meta.GetIssue(r.Context(), pid, iid)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get issue")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 50 {
		limit = 5
	}
	events, err := a.EventsFn(pid, iss.Fingerprint, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "events")
		return
	}
	writeJSON(w, http.StatusOK, events)
}

func (a *API) handleIssueStatus(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	iid, _ := strconv.ParseInt(r.PathValue("issue_id"), 10, 64)
	var req struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request body")
		return
	}
	err := a.meta.SetIssueStatus(r.Context(), pid, iid, req.Status)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "issue.status", strconv.FormatInt(iid, 10), req.Status)
	writeJSON(w, http.StatusOK, map[string]string{"status": req.Status})
}

func (a *API) handleIssueMerge(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	var req struct {
		TargetID int64   `json:"target_id"`
		Sources  []int64 `json:"source_ids"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&req); err != nil || len(req.Sources) == 0 {
		writeErr(w, http.StatusBadRequest, "target_id and source_ids required")
		return
	}
	if len(req.Sources) > 100 {
		writeErr(w, http.StatusBadRequest, "too many sources (max 100)")
		return
	}
	err := a.meta.MergeIssues(r.Context(), pid, req.TargetID, req.Sources...)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "issue.merge", strconv.FormatInt(req.TargetID, 10), "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "merged"})
}

func (a *API) handleIssueSplit(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	iid, _ := strconv.ParseInt(r.PathValue("issue_id"), 10, 64)
	err := a.meta.SplitIssue(r.Context(), pid, iid)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "split")
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "issue.split", strconv.FormatInt(iid, 10), "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "split"})
}

func (a *API) handleAlertList(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	rules, err := a.meta.ListAlertRules(r.Context(), pid)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list alerts")
		return
	}
	writeJSON(w, http.StatusOK, rules)
}

func (a *API) handleAlertCreate(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	var req struct {
		Name       string `json:"name"`
		Condition  string `json:"condition"`
		Threshold  int64  `json:"threshold"`
		WebhookURL string `json:"webhook_url"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad request body")
		return
	}
	id, err := a.meta.CreateAlertRule(r.Context(), alerts.Rule{
		OrgID: actx.OrgID, ProjectID: pid, Name: req.Name, Condition: req.Condition,
		Threshold: req.Threshold, WebhookURL: req.WebhookURL, Enabled: true,
	})
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "alert.create", req.Name, req.Condition)
	writeJSON(w, http.StatusOK, map[string]int64{"id": id})
}

func (a *API) handleAlertDelete(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	pid, _ := strconv.ParseInt(r.PathValue("project_id"), 10, 64)
	rid, _ := strconv.ParseInt(r.PathValue("rule_id"), 10, 64)
	err := a.meta.DeleteAlertRule(r.Context(), pid, rid)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "delete alert")
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "alert.delete", strconv.FormatInt(rid, 10), "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (a *API) handleOpsStatus(_ *authz.AuthContext, w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.StatusFn())
}

func (a *API) handleOpsConfig(_ *authz.AuthContext, w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, a.ConfigFn())
}

func (a *API) handleAudit(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	entries, err := a.meta.AuditList(r.Context(), actx.OrgID, limit)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "audit")
		return
	}
	writeJSON(w, http.StatusOK, entries)
}

func (a *API) handleTokenCreate(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name        string   `json:"name"`
		Permissions []string `json:"permissions"`
		ExpiresDays int      `json:"expires_days"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req); err != nil || req.Name == "" {
		writeErr(w, http.StatusBadRequest, "name required")
		return
	}
	plain, hash, err := auth.NewToken()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "token")
		return
	}
	var exp int64
	if req.ExpiresDays > 0 {
		exp = time.Now().Add(time.Duration(req.ExpiresDays) * 24 * time.Hour).UnixNano()
	}
	id, err := a.meta.CreateToken(r.Context(), metadata.APIToken{
		OrgID: actx.OrgID, UserID: actx.UserID, Name: req.Name,
		Permissions: req.Permissions, ExpiresAt: exp,
	}, hash)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "token")
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "token.create", req.Name, "")
	// Shown exactly once.
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "token": plain})
}

func (a *API) handleTokenRevoke(actx *authz.AuthContext, w http.ResponseWriter, r *http.Request) {
	tid, _ := strconv.ParseInt(r.PathValue("token_id"), 10, 64)
	err := a.meta.RevokeToken(r.Context(), actx.OrgID, tid)
	if errors.Is(err, metadata.ErrNotFound) {
		writeErr(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "revoke")
		return
	}
	a.audit(r, actx.OrgID, actx.UserID, "token.revoke", strconv.FormatInt(tid, 10), "")
	writeJSON(w, http.StatusOK, map[string]string{"status": "revoked"})
}

// audit appends an audit entry; failure is logged loudly, never fatal.
func (a *API) audit(r *http.Request, orgID, actorID int64, action, target, detail string) {
	err := a.meta.Audit(r.Context(), metadata.AuditEntry{
		OrgID: orgID, ActorID: actorID, Action: action, Target: target, Detail: detail,
		IP: clientIP(r), UserAgent: r.UserAgent(),
	})
	if err != nil {
		a.log.Error("AUDIT WRITE FAILED", "action", action, "err", err)
	}
}

func clientIP(r *http.Request) string {
	if i := strings.LastIndex(r.RemoteAddr, ":"); i > 0 {
		return r.RemoteAddr[:i]
	}
	return r.RemoteAddr
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"detail": msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
