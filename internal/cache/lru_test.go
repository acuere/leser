package cache

import (
	"fmt"
	"testing"
	"time"
)

func TestByteBoundEviction(t *testing.T) {
	c := New[string](100, 0)
	for i := 0; i < 20; i++ {
		c.Set(fmt.Sprintf("k%d", i), "v", 10) // 20×10 = 200 bytes > 100 cap
	}
	_, _, cur := c.Stats()
	if cur > 100 {
		t.Fatalf("byte bound violated: %d > 100", cur)
	}
	// Oldest evicted, newest present.
	if _, ok := c.Get("k0"); ok {
		t.Fatal("k0 should be evicted")
	}
	if _, ok := c.Get("k19"); !ok {
		t.Fatal("k19 should be present")
	}
}

func TestOversizeRejected(t *testing.T) {
	c := New[string](100, 0)
	c.Set("big", "v", 500)
	if _, ok := c.Get("big"); ok {
		t.Fatal("oversize entry must be rejected, not evict the world")
	}
}

func TestTTLExpiry(t *testing.T) {
	c := New[string](1000, 10*time.Millisecond)
	c.Set("k", "v", 10)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("fresh entry missing")
	}
	time.Sleep(20 * time.Millisecond)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expired entry served")
	}
}

func TestLRUOrdering(t *testing.T) {
	c := New[int](30, 0)
	c.Set("a", 1, 10)
	c.Set("b", 2, 10)
	c.Set("c", 3, 10)
	c.Get("a")        // a now most recent
	c.Set("d", 4, 10) // evicts b (least recent)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("recently-used a evicted")
	}
}

func TestDelete(t *testing.T) {
	c := New[int](100, 0)
	c.Set("k", 1, 10)
	c.Delete("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("deleted key served")
	}
}
