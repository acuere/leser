// Command leser is the single static binary: server, CLI, and migrations.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"leser/internal/buildinfo"
	"leser/internal/config"
	"leser/internal/logging"
	"leser/internal/server"
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
	return r
}

// run dispatches subcommands and returns a process exit code.
func run(args []string) int {
	return newRouter().Dispatch(args)
}

// cmdServe boots the HTTP server.
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

	srv := server.New(log, cfg.ListenAddr, web.Assets())
	// Milestone 1 has no external dependencies to reach; readiness is immediate.
	// Later milestones gate this on migrations applied + stores reachable.
	srv.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := srv.ListenAndServe(ctx); err != nil {
		log.Error("server exited", "err", err)
		return 1
	}
	log.Info("shutdown complete")
	return 0
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
