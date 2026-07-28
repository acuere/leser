package cluster

import (
	"fmt"
	"net"
	"testing"
	"time"
)

// freePort finds an available UDP/TCP port for memberlist's gossip transport.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForMembers(t *testing.T, m *Membership, want int) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if len(m.Members()) >= want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: timed out waiting for %d members, have %d: %v",
		m.self.NodeID, want, len(m.Members()), m.Members())
}

// TestGossipConvergence starts two REAL memberlist instances (actual UDP
// gossip on localhost, not a fake), joins the second to the first, and
// asserts both converge to seeing each other — the literal "embedded gossip
// library" requirement from order-2 §3.
func TestGossipConvergence(t *testing.T) {
	p1, p2 := freePort(t), freePort(t)

	m1, err := Join(Config{
		Self:        NodeInfo{NodeID: "node-a", APIAddr: "http://127.0.0.1:19001"},
		BindAddr:    "127.0.0.1",
		BindPort:    p1,
		RingRefresh: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m1.Leave(time.Second)

	m2, err := Join(Config{
		Self:        NodeInfo{NodeID: "node-b", APIAddr: "http://127.0.0.1:19002"},
		BindAddr:    "127.0.0.1",
		BindPort:    p2,
		Join:        []string{fmt.Sprintf("127.0.0.1:%d", p1)},
		RingRefresh: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Leave(time.Second)
	if m2.JoinWarning() != nil {
		t.Fatalf("join to a live peer should not warn: %v", m2.JoinWarning())
	}

	waitForMembers(t, m1, 2)
	waitForMembers(t, m2, 2)

	// Both nodes' rings must agree on ownership for the same key set —
	// convergence isn't just "sees 2 members", it's "computes the same ring".
	for i := 0; i < 50; i++ {
		key := int64(i)
		o1, r1 := m1.OwnerAddr(key)
		o2, r2 := m2.OwnerAddr(key)
		// From node1's perspective, either it owns the key (r1=false, o1="")
		// or node2 does (o1==node2's addr); symmetric for node2.
		if r1 && o1 != m2.self.APIAddr {
			t.Fatalf("node1 says project %d routes to unexpected addr %s", key, o1)
		}
		if r2 && o2 != m1.self.APIAddr {
			t.Fatalf("node2 says project %d routes to unexpected addr %s", key, o2)
		}
		if r1 == r2 {
			t.Fatalf("project %d: both nodes think they own it, or both think the other does (r1=%v r2=%v)", key, r1, r2)
		}
	}
}

func TestSingleNodeOwnsEverythingLocally(t *testing.T) {
	p := freePort(t)
	m, err := Join(Config{
		Self:        NodeInfo{NodeID: "solo", APIAddr: "http://127.0.0.1:19003"},
		BindAddr:    "127.0.0.1",
		BindPort:    p,
		RingRefresh: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Leave(time.Second)
	waitForMembers(t, m, 1)

	for i := int64(0); i < 20; i++ {
		if addr, remote := m.OwnerAddr(i); remote || addr != "" {
			t.Fatalf("single-node cluster must own everything locally: project %d -> remote=%v addr=%q", i, remote, addr)
		}
	}
}
