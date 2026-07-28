// Command leser is the single static binary: server, CLI, and migrations.
package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"leser/internal/alerts"
	"leser/internal/api"
	"leser/internal/auth"
	"leser/internal/authz"
	"leser/internal/buildinfo"
	"leser/internal/cluster"
	"leser/internal/config"
	"leser/internal/eventstore"
	"leser/internal/ingest"
	"leser/internal/logging"
	"leser/internal/metadata"
	"leser/internal/server"
	"leser/internal/wal"
	"leser/web"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

// newRouter builds the CLI command table. New commands land here as one Register
// call rather than a new switch arm.
func newRouter() *Router {
	r := NewRouter("leser", os.Stderr)
	r.Register(Command{Name: "serve", Summary: "Boot the server (self-migrate, serve API + UI)", Run: cmdServe})
	r.Register(Command{Name: "config", Summary: "Inspect configuration (config show --effective)", Run: cmdConfig})
	r.Register(Command{Name: "version", Summary: "Print build metadata", Run: cmdVersion})
	r.Register(Command{Name: "send-test-event", Summary: "Send a test event through a DSN to verify the pipeline", Run: cmdSendTestEvent})
	r.Register(Command{Name: "user", Summary: "Manage users (user create --email --password --role)", Run: cmdUser})
	r.Register(Command{Name: "token", Summary: "Manage API tokens (token create --email --name)", Run: cmdToken})
	return r
}

// randomPassword returns a 24-char URL-safe random password.
func randomPassword() (string, error) {
	const chars = "abcdefghijkmnpqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b[:]), nil
}

// cmdUser implements `leser user create` against the local database.
func cmdUser(args []string) int {
	if len(args) == 0 || args[0] != "create" {
		fmt.Fprintln(os.Stderr, "usage: leser user create --email E [--password P] [--role R] [--data-dir D] [--json]")
		return 2
	}
	fs := flag.NewFlagSet("user create", flag.ContinueOnError)
	email := fs.String("email", "", "email (required)")
	password := fs.String("password", "", "password (generated when empty)")
	role := fs.String("role", authz.RoleMember, "owner|manager|admin|member|billing|read_only")
	dataDir := fs.String("data-dir", "./data", "data directory")
	asJSON := fs.Bool("json", false, "output JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *email == "" {
		fmt.Fprintln(os.Stderr, "leser: --email is required")
		return 2
	}
	if authz.RolePermissions(*role) == nil {
		fmt.Fprintf(os.Stderr, "leser: unknown role %q\n", *role)
		return 2
	}
	pw := *password
	if pw == "" {
		var err error
		if pw, err = randomPassword(); err != nil {
			fmt.Fprintf(os.Stderr, "leser: %v\n", err)
			return 1
		}
	}
	meta, err := metadata.Open(filepath.Join(*dataDir, "metadata.db"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: open metadata: %v (is --data-dir right?)\n", err)
		return 1
	}
	defer meta.Close()
	ctx := context.Background()
	proj, _, err := meta.Bootstrap(ctx, "Default", "my-app")
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	hash, err := auth.HashPassword(pw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	id, err := meta.CreateUser(ctx, proj.OrgID, *email, hash, *role)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: create user: %v\n", err)
		return 1
	}
	_ = meta.Audit(ctx, metadata.AuditEntry{OrgID: proj.OrgID, Action: "user.create", Target: *email, Detail: *role})
	if *asJSON {
		return emitJSON(map[string]any{"id": id, "email": *email, "role": *role, "password": pw})
	}
	fmt.Printf("created user %s (id %d, role %s)\npassword: %s\n", *email, id, *role, pw)
	return 0
}

// cmdToken implements `leser token create` against the local database.
func cmdToken(args []string) int {
	if len(args) == 0 || args[0] != "create" {
		fmt.Fprintln(os.Stderr, "usage: leser token create --email E --name N [--perms p1,p2] [--expires-days D] [--data-dir D] [--json]")
		return 2
	}
	fs := flag.NewFlagSet("token create", flag.ContinueOnError)
	email := fs.String("email", "", "owning user email (required)")
	name := fs.String("name", "", "token name (required)")
	perms := fs.String("perms", "", "comma-separated permission subset (empty = user's full set)")
	expDays := fs.Int("expires-days", 90, "expiry in days (0 = never)")
	dataDir := fs.String("data-dir", "./data", "data directory")
	asJSON := fs.Bool("json", false, "output JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	if *email == "" || *name == "" {
		fmt.Fprintln(os.Stderr, "leser: --email and --name are required")
		return 2
	}
	meta, err := metadata.Open(filepath.Join(*dataDir, "metadata.db"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: open metadata: %v\n", err)
		return 1
	}
	defer meta.Close()
	ctx := context.Background()
	u, err := meta.UserByEmail(ctx, *email)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: user %s not found — create with `leser user create`\n", *email)
		return 1
	}
	plain, hash, err := auth.NewToken()
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	var permList []string
	if *perms != "" {
		permList = strings.Split(*perms, ",")
	}
	var exp int64
	if *expDays > 0 {
		exp = time.Now().Add(time.Duration(*expDays) * 24 * time.Hour).UnixNano()
	}
	id, err := meta.CreateToken(ctx, metadata.APIToken{
		OrgID: u.OrgID, UserID: u.ID, Name: *name, Permissions: permList, ExpiresAt: exp,
	}, hash)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	_ = meta.Audit(ctx, metadata.AuditEntry{OrgID: u.OrgID, ActorID: u.ID, Action: "token.create", Target: *name, Detail: "cli"})
	if *asJSON {
		return emitJSON(map[string]any{"id": id, "token": plain})
	}
	fmt.Printf("token %s (id %d) — shown once:\n%s\n", *name, id, plain)
	return 0
}

// cmdSendTestEvent posts a minimal envelope to the DSN's ingest endpoint,
// verifying the full path end to end (order.md §10).
func cmdSendTestEvent(args []string) int {
	fs := flag.NewFlagSet("send-test-event", flag.ContinueOnError)
	dsnFlag := fs.String("dsn", "", "DSN printed by `leser serve` (required)")
	asJSON := fs.Bool("json", false, "output JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dsnFlag == "" {
		fmt.Fprintln(os.Stderr, "leser: --dsn is required (it was printed by `leser serve`)")
		return 2
	}
	u, err := url.Parse(*dsnFlag)
	if err != nil || u.User == nil || len(u.Path) < 2 {
		fmt.Fprintf(os.Stderr, "leser: invalid DSN %q (want scheme://key@host/project_id)\n", *dsnFlag)
		return 2
	}
	key := u.User.Username()
	projectID := u.Path[1:]
	endpoint := fmt.Sprintf("%s://%s/api/%s/envelope/", u.Scheme, u.Host, projectID)

	eventID := fmt.Sprintf("%032x", time.Now().UnixNano())
	event := fmt.Sprintf(`{"event_id":%q,"level":"error","message":"leser test event","exception":{"values":[{"type":"TestError","value":"pipeline verification"}]}}`, eventID)
	body := fmt.Sprintf("{\"event_id\":%q}\n{\"type\":\"event\",\"length\":%d}\n%s\n", eventID, len(event), event)

	req, err := http.NewRequest(http.MethodPost, endpoint, strings.NewReader(body))
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	req.Header.Set("X-Sentry-Auth", "Sentry sentry_key="+key+", sentry_version=7")
	req.Header.Set("Content-Type", "application/x-sentry-envelope")
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: send failed: %v\n  next: is `leser serve` running and the DSN host reachable?\n", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		fmt.Fprintf(os.Stderr, "leser: ingest returned %d: %s\n", resp.StatusCode, string(b))
		return 1
	}
	if *asJSON {
		return emitJSON(map[string]string{"status": "ok", "event_id": eventID, "endpoint": endpoint})
	}
	fmt.Printf("ok: event %s accepted by %s\n", eventID, endpoint)
	return 0
}

// run dispatches subcommands and returns a process exit code.
func run(args []string) int {
	return newRouter().Dispatch(args)
}

// cmdServe boots everything: metadata (self-migrating), WAL, event store,
// ingest pipeline, HTTP. Prints the DSN so the 60-second path holds.
func cmdServe(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to JSON config file (optional)")
	listen := fs.String("listen", "", "override listen address (host:port)")
	dataDir := fs.String("data-dir", "", "override data directory")
	logLevel := fs.String("log-level", "", "override log level (debug|info|warn|error)")
	role := fs.String("role", "all", "all|ingest|worker|query — Rung 2 role separation (order-2 §3); non-'all' roles require a shared --data-dir")
	clusterNodeID := fs.String("cluster-node-id", "", "Rung 3: this node's cluster identity (required to enable clustering)")
	clusterAPIAddr := fs.String("cluster-api-addr", "", "Rung 3: this node's API address as reachable by OTHER nodes, e.g. http://10.0.1.4:8080")
	clusterBind := fs.String("cluster-bind", "0.0.0.0", "Rung 3: gossip bind address")
	clusterPort := fs.Int("cluster-port", 7946, "Rung 3: gossip port")
	clusterJoin := fs.String("cluster-join", "", "Rung 3: comma-separated peer gossip addresses (host:port) to join; empty starts a new/solo cluster")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	clustered := *clusterNodeID != ""
	if clustered && *clusterAPIAddr == "" {
		fmt.Fprintln(os.Stderr, "leser: --cluster-api-addr is required when --cluster-node-id is set")
		return 2
	}
	doIngest, doWorker, doQuery := true, true, true
	switch *role {
	case "all":
	case "ingest":
		doWorker, doQuery = false, false
	case "worker":
		doIngest, doQuery = false, false
	case "query":
		doIngest, doWorker = false, false
	default:
		fmt.Fprintf(os.Stderr, "leser: --role must be all|ingest|worker|query, got %q\n", *role)
		return 2
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	// Flags win over env/file/defaults; apply only those explicitly set.
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "listen":
			cfg.ListenAddr = *listen
		case "data-dir":
			cfg.DataDir = *dataDir
		case "log-level":
			cfg.LogLevel = *logLevel
		}
	})
	if err := cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "leser: invalid config: %v\n", err)
		return 1
	}

	log := logging.New(cfg.LogLevel)
	log = log.With("role", *role)
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		log.Error("create data dir", "err", err)
		return 1
	}

	// Metadata: opens + self-migrates + bootstraps default org/project/DSN.
	// SQLite's own WAL mode handles concurrent multi-process access when
	// --data-dir is genuinely shared storage (order-2 Rung 2). Every role
	// opens it: worker/query need it for issues/alerts/audit; ingest needs
	// it for DSN key lookup.
	meta, err := metadata.Open(filepath.Join(cfg.DataDir, "metadata.db"))
	if err != nil {
		log.Error("open metadata store", "err", err)
		return 1
	}
	defer meta.Close()
	proj, key, err := meta.Bootstrap(context.Background(), "Default", "my-app")
	if err != nil {
		log.Error("bootstrap", "err", err)
		return 1
	}

	// WAL: the role that ingests OWNS the log (single writer, structural).
	// Every other role attaches ReadOnly and live-tails it via Refresh —
	// pure shared-directory coordination, no RPC between roles.
	walOpts := wal.Options{}
	if !doIngest {
		walOpts.ReadOnly = true
	}
	w, err := wal.Open(filepath.Join(cfg.DataDir, "wal"), walOpts)
	if err != nil {
		log.Error("open wal", "err", err)
		return 1
	}
	defer w.Close()

	// Event store: needed by any role that consumes (worker, to compact) or
	// serves reads (query, via Refresh-discovered segments the worker wrote).
	var store *eventstore.Store
	if doWorker || doQuery {
		store, err = eventstore.Open(filepath.Join(cfg.DataDir, "events"), eventstore.Options{})
		if err != nil {
			log.Error("open event store", "err", err)
			return 1
		}
	}

	// First-boot admin account: created once, credentials printed once. Only
	// the role(s) serving login need to bother; the insert is idempotent
	// (unique email) so a race across simultaneously-starting roles just
	// means the loser logs a warning, never a fatal error.
	adminNote := ""
	if doQuery {
		if n, cerr := meta.CountUsers(context.Background()); cerr == nil && n == 0 {
			pw, aerr := randomPassword()
			if aerr != nil {
				log.Error("generate admin password", "err", aerr)
				return 1
			}
			hash, herr := auth.HashPassword(pw)
			if herr != nil {
				log.Error("hash admin password", "err", herr)
				return 1
			}
			if _, uerr := meta.CreateUser(context.Background(), proj.OrgID, "admin@leser.local", hash, authz.RoleOwner); uerr != nil {
				log.Warn("admin bootstrap skipped (likely created by a concurrently-starting role)", "err", uerr)
			} else {
				adminNote = fmt.Sprintf("  Admin:    admin@leser.local / %s   (shown once — change it)\n", pw)
			}
		}
	}

	// Alert engine + issue grouping only run where events are actually
	// consumed (worker role).
	var engine *alerts.Engine
	var sink ingest.IssueSink
	if doWorker {
		engine = alerts.New(log, meta, alerts.Options{})
		sink = &issueSink{db: meta, engine: engine}
	}
	pipe := ingest.NewPipeline(log, w, store, sink, ingest.PipelineOptions{}, ingest.Limits{})

	var mounts []func(*http.ServeMux)
	var ih *ingest.Handler
	if doIngest {
		ih = ingest.NewHandler(pipe, meta, ingest.Limits{})
		mounts = append(mounts, ih.Register)
	}
	var membership *cluster.Membership
	if clustered {
		var joinAddrs []string
		if *clusterJoin != "" {
			joinAddrs = strings.Split(*clusterJoin, ",")
		}
		membership, err = cluster.Join(cluster.Config{
			Self:     cluster.NodeInfo{NodeID: *clusterNodeID, APIAddr: *clusterAPIAddr},
			BindAddr: *clusterBind,
			BindPort: *clusterPort,
			Join:     joinAddrs,
		})
		if err != nil {
			log.Error("cluster join", "err", err)
			return 1
		}
		if w := membership.JoinWarning(); w != nil {
			log.Warn("cluster join reported some unreachable peers (gossip will retry)", "err", w)
		}
		defer membership.Leave(2 * time.Second)
	}

	if doQuery {
		apiHandler := api.New(log, meta,
			func() any { return pipe.Status() },
			func() any { return cfg.Effective() },
		)
		if membership != nil {
			apiHandler.Locator = membership
			apiHandler.NodeID = *clusterNodeID
		}
		apiHandler.DSNFor = func(publicKey string, projectID int64) string {
			return dsnFor(cfg.PublicURL, publicKey, projectID)
		}
		apiHandler.FingerprintsFn = func(projectID int64, release, environment, userID string) ([]string, error) {
			seen := map[string]bool{}
			var fps []string
			err := store.Scan(eventstore.Query{
				ProjectID: projectID, Release: release, Environment: environment,
				UserID: userID, Limit: 5000,
			}, func(e eventstore.Event) error {
				if !seen[e.Fingerprint] {
					seen[e.Fingerprint] = true
					fps = append(fps, e.Fingerprint)
				}
				if len(fps) >= 500 { // bound the IN set
					return nil
				}
				return nil
			})
			return fps, err
		}
		apiHandler.StatsFn = func(projectID, tmin, tmax, bucket int64) (any, error) {
			return store.Aggregate(eventstore.Query{ProjectID: projectID, TimeMin: tmin, TimeMax: tmax}, bucket, 10)
		}
		apiHandler.EventsFn = func(projectID int64, fingerprint string, limit int) (any, error) {
			type apiEvent struct {
				EventID     string          `json:"event_id"`
				Timestamp   int64           `json:"timestamp"`
				Level       string          `json:"level"`
				Release     string          `json:"release"`
				Environment string          `json:"environment"`
				UserID      string          `json:"user_id"`
				Message     string          `json:"message"`
				Payload     json.RawMessage `json:"payload"`
			}
			out := []apiEvent{}
			err := store.Scan(eventstore.Query{ProjectID: projectID, Fingerprint: fingerprint, Limit: limit},
				func(e eventstore.Event) error {
					out = append(out, apiEvent{
						EventID: e.EventID, Timestamp: e.Timestamp, Level: e.Level,
						Release: e.Release, Environment: e.Environment, UserID: e.UserID,
						Message: e.Message, Payload: json.RawMessage(e.Payload),
					})
					return nil
				})
			return out, err
		}
		mounts = append(mounts, apiHandler.Register)
	}

	ui := web.Assets()
	if !doQuery {
		ui = nil // ingest/worker-only nodes serve no dashboard
	}
	srv := server.New(log, cfg.ListenAddr, ui, mounts...)
	srv.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Consumer: WAL → event store. Crash-only; resumes from committed offset.
	if doWorker {
		go func() {
			if err := pipe.Run(ctx); err != nil && ctx.Err() == nil {
				log.Error("pipeline exited", "err", err)
				stop()
			}
		}()
		go engine.Run(ctx)
	}

	if doIngest {
		// Rate-limit state: restore last checkpoint, then checkpoint every
		// 30s. Approximate-after-restart is correct by design (order-2 §2.5).
		if state, rlErr := meta.RateLimitLoad(ctx); rlErr == nil && len(state) > 0 {
			ih.Limiter().Restore(state)
		}
		go func() {
			tick := time.NewTicker(30 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if err := meta.RateLimitSave(ctx, ih.Limiter().Snapshot()); err != nil && ctx.Err() == nil {
						log.Error("rate limit checkpoint", "err", err)
					}
				}
			}
		}()
	}
	if doIngest && !doWorker {
		// role=ingest alone: no co-located Run() sets consumerLag from a live
		// Reader, so learn the worker's progress from the shared offset file
		// instead — backpressure stays a function of real log lag either way.
		go func() {
			tick := time.NewTicker(200 * time.Millisecond)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					pipe.RefreshLagFromWAL(ingest.ConsumerName)
				}
			}
		}()
	}
	if doQuery && !doWorker {
		// role=query alone never flushes the store itself, so it never sees
		// new segments the worker compacts unless it polls for them. This is
		// the query node's one honest staleness window: a query node lags
		// the worker's FlushAge (default 10s) behind on newly-compacted data,
		// on top of whatever is still sitting in the worker's hot buffer.
		go func() {
			tick := time.NewTicker(2 * time.Second)
			defer tick.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-tick.C:
					if err := store.Refresh(); err != nil {
						log.Error("event store refresh", "err", err)
					}
				}
			}
		}()
	}

	dsn := dsnFor(cfg.PublicURL, key.PublicKey, proj.ID)
	var banner strings.Builder
	fmt.Fprintf(&banner, "\n  leser is up (role=%s).\n\n  Project:  %s (id %d)\n", *role, proj.Name, proj.ID)
	if doIngest {
		fmt.Fprintf(&banner, "  DSN:      %s\n", dsn)
	}
	if doQuery {
		fmt.Fprintf(&banner, "  UI:       %s\n", cfg.PublicURL)
	}
	banner.WriteString(adminNote)
	fmt.Print(banner.String())
	log.Info("ready", "project", proj.Slug)

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Error("server exited", "err", err)
		return 1
	}
	log.Info("shutdown complete")
	return 0
}

// issueSink adapts metadata.DB to the pipeline's IssueSink, caching the
// project→org mapping (immutable once created) and feeding the alert engine
// with each outcome. Alert evaluation lives here so the pipeline stays
// storage-only.
type issueSink struct {
	db     *metadata.DB
	engine *alerts.Engine // may be nil
	mu     sync.Mutex
	m      map[int64]int64
}

func (s *issueSink) UpsertIssue(ctx context.Context, u ingest.IssueHit) (int64, error) {
	s.mu.Lock()
	if s.m == nil {
		s.m = map[int64]int64{}
	}
	orgID, ok := s.m[u.ProjectID]
	s.mu.Unlock()
	if !ok {
		var err error
		orgID, err = s.db.ProjectOrg(ctx, u.ProjectID)
		if err != nil {
			return 0, err
		}
		s.mu.Lock()
		s.m[u.ProjectID] = orgID
		s.mu.Unlock()
	}
	out, err := s.db.UpsertIssue(ctx, metadata.IssueUpsert{
		OrgID: orgID, ProjectID: u.ProjectID,
		Fingerprint: u.Fingerprint, Basis: u.Basis,
		Title: u.Title, Level: u.Level, SeenAt: u.SeenAt,
	})
	if err != nil {
		return 0, err
	}
	if s.engine != nil {
		s.engine.Evaluate(ctx, alerts.IssueActivity{
			OrgID: orgID, ProjectID: u.ProjectID, IssueID: out.ID,
			Title: u.Title, Level: u.Level, Status: out.Status,
			TimesSeen: out.TimesSeen, IsNew: out.IsNew, Regressed: out.Regressed,
		})
	}
	return out.ID, nil
}

// dsnFor builds a Sentry-compatible DSN from the public URL, key, and project.
func dsnFor(publicURL, publicKey string, projectID int64) string {
	u, err := url.Parse(publicURL)
	if err != nil || u.Host == "" {
		return fmt.Sprintf("http://%s@localhost:8080/%d", publicKey, projectID)
	}
	return fmt.Sprintf("%s://%s@%s/%d", u.Scheme, publicKey, u.Host, projectID)
}

// cmdVersion prints build metadata, with --json for machine consumption.
func cmdVersion(args []string) int {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "output JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	info := buildinfo.Get()
	if *asJSON {
		return emitJSON(info)
	}
	fmt.Printf("leser %s (commit %s, %s)\n", info.Version, info.Commit, info.GoVersion)
	return 0
}

// cmdConfig implements `config show --effective`.
func cmdConfig(args []string) int {
	if len(args) == 0 || args[0] != "show" {
		fmt.Fprintln(os.Stderr, "usage: leser config show [--effective] [--json] [--config PATH]")
		return 2
	}
	fs := flag.NewFlagSet("config show", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "path to JSON config file (optional)")
	_ = fs.Bool("effective", false, "show resolved config (default and only mode)")
	asJSON := fs.Bool("json", false, "output JSON")
	if err := fs.Parse(args[1:]); err != nil {
		return 2
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: %v\n", err)
		return 1
	}
	eff := cfg.Effective()
	if *asJSON {
		return emitJSON(eff)
	}
	for _, k := range []string{"listen_addr", "data_dir", "log_level", "public_url", "secret_key"} {
		fmt.Printf("%-12s %s\n", k, eff[k])
	}
	return 0
}

// emitJSON writes v as indented JSON to stdout.
func emitJSON(v any) int {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "leser: encode json: %v\n", err)
		return 1
	}
	fmt.Println(string(b))
	return 0
}
