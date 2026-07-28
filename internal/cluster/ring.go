// Package cluster is Rung 3 (order-2 §3): consistent-hash project ownership
// over an embedded gossip membership (memberlist — "a library, not a
// service"), so any node can serve any API request by proxying it to the
// node that owns the project. This package implements ownership ROUTING
// only. It does not implement per-shard data isolation: WAL and event-store
// directories are not yet split per shard, so today every node still reads
// the same shared storage (Rung 2's model). Real segment ownership and
// rebalancing (order-2 §3: "Rebalancing moves whole segments, never rows")
// are future work — stated here rather than implied by the package existing.
package cluster

import (
	"hash/fnv"
	"sort"
	"strconv"
	"sync"
)

// vnodesPerNode bounds ownership skew: with N physical nodes, a project's
// hash lands within roughly 1/(N*vnodesPerNode) of an even split.
const vnodesPerNode = 100

type ringEntry struct {
	hash uint32
	node string
}

// Ring is a consistent-hash ring over node IDs. Safe for concurrent use.
// Reads never block writers longer than a slice-copy swap.
type Ring struct {
	mu      sync.RWMutex
	entries []ringEntry // sorted by hash
	nodes   map[string]bool
}

// NewRing returns an empty ring (every OwnerFor call is unresolved until
// SetNodes is called at least once).
func NewRing() *Ring {
	return &Ring{nodes: map[string]bool{}}
}

// SetNodes replaces the full membership set and rebuilds the ring.
// Deterministic: the same node set always produces the same ring, regardless
// of call order — required so every node computes identical ownership from
// the same gossip-converged membership list.
func (r *Ring) SetNodes(nodeIDs []string) {
	uniq := make(map[string]bool, len(nodeIDs))
	for _, id := range nodeIDs {
		uniq[id] = true
	}
	sorted := make([]string, 0, len(uniq))
	for id := range uniq {
		sorted = append(sorted, id)
	}
	sort.Strings(sorted)

	entries := make([]ringEntry, 0, len(sorted)*vnodesPerNode)
	for _, id := range sorted {
		for v := 0; v < vnodesPerNode; v++ {
			entries = append(entries, ringEntry{hash: hashKey(id + "#" + strconv.Itoa(v)), node: id})
		}
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].hash < entries[j].hash })

	r.mu.Lock()
	r.entries = entries
	r.nodes = uniq
	r.mu.Unlock()
}

// OwnerFor returns the node ID owning key, and false if the ring is empty.
func (r *Ring) OwnerFor(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.entries) == 0 {
		return "", false
	}
	h := hashKey(key)
	i := sort.Search(len(r.entries), func(i int) bool { return r.entries[i].hash >= h })
	if i == len(r.entries) {
		i = 0 // wrap around the ring
	}
	return r.entries[i].node, true
}

// Nodes returns the current member set (for tests/observability).
func (r *Ring) Nodes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.nodes))
	for id := range r.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// hashKey hashes s and runs it through murmur3's 32-bit finalizer. Plain
// FNV-1a has weak avalanche on structured, near-sequential inputs like
// "node#0".."node#99" — measured: with vnode labels differing only in a
// trailing digit, raw FNV32a put 54% of 10k keys on one of four nodes. The
// finalizer fixes it to within a few percent of even. (Same class of bug as
// the HyperLogLog sketch in internal/eventstore — see its hll.go comment.)
func hashKey(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	x := h.Sum32()
	x ^= x >> 16
	x *= 0x85ebca6b
	x ^= x >> 13
	x *= 0xc2b2ae35
	x ^= x >> 16
	return x
}
