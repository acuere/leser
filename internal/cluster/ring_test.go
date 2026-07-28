package cluster

import (
	"fmt"
	"math"
	"testing"
)

func TestOwnerForEmptyRing(t *testing.T) {
	r := NewRing()
	if _, ok := r.OwnerFor("x"); ok {
		t.Fatal("empty ring must report no owner")
	}
}

func TestDeterministicAcrossNodes(t *testing.T) {
	// Same member set built in different orders must produce identical
	// ownership — every node computes this independently from gossip state
	// that can arrive in any order.
	a := NewRing()
	a.SetNodes([]string{"n1", "n2", "n3"})
	b := NewRing()
	b.SetNodes([]string{"n3", "n1", "n2"})

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("project-%d", i)
		oa, _ := a.OwnerFor(key)
		ob, _ := b.OwnerFor(key)
		if oa != ob {
			t.Fatalf("key %s: ring built n1,n2,n3 says %s, ring built n3,n1,n2 says %s", key, oa, ob)
		}
	}
}

func TestDistributionIsReasonablyBalanced(t *testing.T) {
	r := NewRing()
	nodes := []string{"n1", "n2", "n3", "n4"}
	r.SetNodes(nodes)

	counts := map[string]int{}
	const keys = 10_000
	for i := 0; i < keys; i++ {
		owner, ok := r.OwnerFor(fmt.Sprintf("project-%d", i))
		if !ok {
			t.Fatal("no owner")
		}
		counts[owner]++
	}
	if len(counts) != len(nodes) {
		t.Fatalf("only %d of %d nodes received any keys: %v", len(counts), len(nodes), counts)
	}
	want := float64(keys) / float64(len(nodes))
	for node, c := range counts {
		dev := math.Abs(float64(c)-want) / want
		if dev > 0.25 { // 100 vnodes/node keeps skew well under this
			t.Errorf("node %s got %d keys (%.0f%% of even split), want within 25%% of %.0f", node, c, dev*100, want)
		}
	}
}

func TestAddingNodeMovesOnlyItsShare(t *testing.T) {
	before := NewRing()
	before.SetNodes([]string{"n1", "n2", "n3"})

	after := NewRing()
	after.SetNodes([]string{"n1", "n2", "n3", "n4"})

	moved := 0
	const keys = 5000
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("project-%d", i)
		ob, _ := before.OwnerFor(key)
		oa, _ := after.OwnerFor(key)
		if ob != oa {
			moved++
		}
	}
	// Ideal consistent hashing moves ~1/4 of keys when going 3->4 nodes.
	frac := float64(moved) / float64(keys)
	if frac < 0.10 || frac > 0.40 {
		t.Fatalf("moved %.0f%% of keys adding a 4th node, want roughly 25%% (consistent hashing property)", frac*100)
	}
}

func TestNodesReflectsMembership(t *testing.T) {
	r := NewRing()
	r.SetNodes([]string{"b", "a", "c", "a"})
	got := r.Nodes()
	want := []string{"a", "b", "c"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}
