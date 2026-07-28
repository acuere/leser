// Command leser is the single static binary: server, CLI, and migrations.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

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
	return r
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

	pipe := ingest.NewPipeline(log, w, store, &issueSink{db: meta}, ingest.PipelineOptions{}, ingest.Limits{})
	ih := ingest.NewHandler(pipe, meta, ingest.Limits{})

	srv := server.New(log, cfg.ListenAddr, web.Assets(),
		ih.Register,
		func(mux *http.ServeMux) {
			mux.HandleFunc("GET /api/ops/status", func(rw http.ResponseWriter, _ *http.Request) {
				rw.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(rw).Encode(pipe.Status())
			})
			// Temporary read surface until the authed API lands (order.md M4):
			// the issue stream for a project. Replaced by /api/0/... with RBAC.
			mux.HandleFunc("GET /api/ops/issues", func(rw http.ResponseWriter, r *http.Request) {
				pid, err := strconv.ParseInt(r.URL.Query().Get("project"), 10, 64)
				if err != nil {
					rw.WriteHeader(http.StatusBadRequest)
					return
				}
				issues, err := meta.ListIssues(r.Context(), pid, r.URL.Query().Get("status"), 100)
				if err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
					return
				}
				rw.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(rw).Encode(issues)
			})
			mux.HandleFunc("GET /api/ops/projects", func(rw http.ResponseWriter, r *http.Request) {
				ps, err := meta.ListProjects(r.Context())
				if err != nil {
					rw.WriteHeader(http.StatusInternalServerError)
					return
				}
				type pj struct {
					metadata.ProjectInfo
					DSN string `json:"dsn"`
				}
				out := make([]pj, 0, len(ps))
				for _, p := range ps {
					out = append(out, pj{p, dsnFor(cfg.PublicURL, p.PublicKey, p.ID)})
				}
				rw.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(rw).Encode(out)
			})
			mux.HandleFunc("GET /api/ops/config", func(rw http.ResponseWriter, _ *http.Request) {
				rw.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(rw).Encode(cfg.Effective()) // secrets redacted
			})
		},
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

	dsn := dsnFor(cfg.PublicURL, key.PublicKey, proj.ID)
	fmt.Printf("\n  leser is up.\n\n  Project:  %s (id %d)\n  DSN:      %s\n  UI:       %s\n  Ops:      %s/api/ops/status\n\n", proj.Name, proj.ID, dsn, cfg.PublicURL, cfg.PublicURL)
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
