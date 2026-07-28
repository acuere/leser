package main

import (
	"io"
	"testing"
)

func TestDispatchRoutesAndReturnsCode(t *testing.T) {
	r := NewRouter("leser", io.Discard)
	r.Register(Command{Name: "ping", Summary: "x", Run: func([]string) int { return 7 }})
	if got := r.Dispatch([]string{"ping"}); got != 7 {
		t.Fatalf("dispatch code: got %d want 7", got)
	}
}

func TestDispatchPassesRestArgs(t *testing.T) {
	r := NewRouter("leser", io.Discard)
	var seen []string
	r.Register(Command{Name: "echo", Run: func(a []string) int { seen = a; return 0 }})
	r.Dispatch([]string{"echo", "--flag", "val"})
	if len(seen) != 2 || seen[0] != "--flag" || seen[1] != "val" {
		t.Fatalf("rest args: got %v", seen)
	}
}

func TestUnknownCommandExit2(t *testing.T) {
	r := NewRouter("leser", io.Discard)
	if got := r.Dispatch([]string{"nope"}); got != 2 {
		t.Fatalf("unknown cmd: got %d want 2", got)
	}
}

func TestNoArgsExit2(t *testing.T) {
	r := NewRouter("leser", io.Discard)
	if got := r.Dispatch(nil); got != 2 {
		t.Fatalf("no args: got %d want 2", got)
	}
}

func TestHelpExit0(t *testing.T) {
	r := NewRouter("leser", io.Discard)
	if got := r.Dispatch([]string{"help"}); got != 0 {
		t.Fatalf("help: got %d want 0", got)
	}
}

func TestDuplicateRegisterPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate command")
		}
	}()
	r := NewRouter("leser", io.Discard)
	r.Register(Command{Name: "dup", Run: func([]string) int { return 0 }})
	r.Register(Command{Name: "dup", Run: func([]string) int { return 0 }})
}
