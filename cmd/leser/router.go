package main

import (
	"fmt"
	"io"
	"sort"
	"strings"
)

// Command is one CLI subcommand. Run receives the args after the command name
// and returns a process exit code. Subcommands (e.g. "config show") parse their
// own remaining args.
type Command struct {
	Name    string
	Summary string
	Run     func(args []string) int
}

// Router is a tiny stdlib-only subcommand dispatcher. It replaces the hand-rolled
// switch so new commands are one Register call (scales to the M4 command set).
type Router struct {
	name string
	out  io.Writer
	cmds map[string]Command
	// order preserves registration order only as a fallback; help sorts by name.
	order []string
}

// NewRouter creates a Router whose help/errors are written to out.
func NewRouter(name string, out io.Writer) *Router {
	return &Router{name: name, out: out, cmds: map[string]Command{}}
}

// Register adds a command. A duplicate name panics at startup (programmer error,
// caught by any run of the binary), never at request time.
func (r *Router) Register(c Command) {
	if _, dup := r.cmds[c.Name]; dup {
		panic(fmt.Sprintf("router: duplicate command %q", c.Name))
	}
	r.cmds[c.Name] = c
	r.order = append(r.order, c.Name)
}

// Dispatch routes args[0] to a command, handling help and unknown commands.
// Returns the process exit code.
func (r *Router) Dispatch(args []string) int {
	if len(args) == 0 {
		r.Usage()
		return 2
	}
	name, rest := args[0], args[1:]
	switch name {
	case "-h", "--help", "help":
		r.Usage()
		return 0
	}
	c, ok := r.cmds[name]
	if !ok {
		fmt.Fprintf(r.out, "%s: unknown command %q\n\n", r.name, name)
		r.Usage()
		return 2
	}
	return c.Run(rest)
}

// Usage prints the command list, sorted by name.
func (r *Router) Usage() {
	names := make([]string, 0, len(r.cmds))
	for n := range r.cmds {
		names = append(names, n)
	}
	sort.Strings(names)

	var b strings.Builder
	fmt.Fprintf(&b, "%s — open-source error tracking & observability\n\n", r.name)
	fmt.Fprintf(&b, "Usage: %s <command> [flags]\n\nCommands:\n", r.name)
	width := 0
	for _, n := range names {
		if len(n) > width {
			width = len(n)
		}
	}
	for _, n := range names {
		fmt.Fprintf(&b, "  %-*s  %s\n", width, n, r.cmds[n].Summary)
	}
	fmt.Fprintf(&b, "  %-*s  %s\n", width, "help", "Show this help")
	fmt.Fprint(r.out, b.String())
}
