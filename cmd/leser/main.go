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

	"leser/internal/api"
	"leser/internal/auth"
	"leser/internal/authz"
	"leser/internal/buildinfo"
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
	if err := fs.Parse(args); err != nil {
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
	if err := os.MkdirAll(cfg.DataDir, 0o750); err != nil {
		log.Error("create data dir", "err", err)
		return 1
	}

	// Metadata: opens + self-migrates + bootstraps default org/project/DSN.
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

	// WAL — the ingest spine.
	w, err := wal.Open(filepath.Join(cfg.DataDir, "wal"), wal.Options{})
	if err != nil {
		log.Error("open wal", "err", err)
		return 1
	}
	defer w.Close()

	// Event store — hot buffer + Parquet segments.
	store, err := eventstore.Open(filepath.Join(cfg.DataDir, "events"), eventstore.Options{})
	if err != nil {
		log.Error("open event store", "err", err)
		return 1
	}

	// First-boot admin account: created once, credentials printed once.
	adminNote := ""
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
			log.Error("create admin", "err", uerr)
			return 1
		}
		adminNote = fmt.Sprintf("  Admin:    admin@leser.local / %s   (shown once — change it)\n", pw)
	}

	pipe := ingest.NewPipeline(log, w, store, &issueSink{db: meta}, ingest.PipelineOptions{}, ingest.Limits{})
	ih := ingest.NewHandler(pipe, meta, ingest.Limits{})
	apiHandler := api.New(log, meta,
		func() any { return pipe.Status() },
		func() any { return cfg.Effective() },
	)
	apiHandler.StatsFn = func(projectID, tmin, tmax, bucket int64) (any, error) {
		return store.Aggregate(eventstore.Query{ProjectID: projectID, TimeMin: tmin, TimeMax: tmax}, bucket, 10)
	}

	srv := server.New(log, cfg.ListenAddr, web.Assets(),
		ih.Register,
		apiHandler.Register,
	)
	srv.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Consumer: WAL → event store. Crash-only; resumes from committed offset.
	go func() {
		if err := pipe.Run(ctx); err != nil && ctx.Err() == nil {
			log.Error("pipeline exited", "err", err)
			stop()
		}
	}()

	// Rate-limit state: restore last checkpoint, then checkpoint every 30s.
	// Approximate-after-restart is correct enough by design (order-2 §2.5).
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

	dsn := dsnFor(cfg.PublicURL, key.PublicKey, proj.ID)
	fmt.Printf("\n  leser is up.\n\n  Project:  %s (id %d)\n  DSN:      %s\n  UI:       %s\n%s\n", proj.Name, proj.ID, dsn, cfg.PublicURL, adminNote)
	log.Info("ready", "project", proj.Slug, "dsn", dsn)

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Error("server exited", "err", err)
		return 1
	}
	log.Info("shutdown complete")
	return 0
}

// issueSink adapts metadata.DB to the pipeline's IssueSink, caching the
// project→org mapping (immutable once created).
type issueSink struct {
	db *metadata.DB
	mu sync.Mutex
	m  map[int64]int64
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
	return s.db.UpsertIssue(ctx, metadata.IssueUpsert{
		OrgID: orgID, ProjectID: u.ProjectID,
		Fingerprint: u.Fingerprint, Basis: u.Basis,
		Title: u.Title, Level: u.Level, SeenAt: u.SeenAt,
	})
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
