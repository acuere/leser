package cluster

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/memberlist"
)

// NodeInfo is what a node announces to the cluster over gossip: enough for
// any other node to route a proxied request to it.
type NodeInfo struct {
	NodeID  string `json:"node_id"`
	APIAddr string `json:"api_addr"` // e.g. http://10.0.1.4:8080, reachable from every other node
}

// Config configures gossip membership.
type Config struct {
	Self        NodeInfo
	BindAddr    string   // gossip bind, e.g. "0.0.0.0"
	BindPort    int      // gossip port, e.g. 7946
	Join        []string // existing peers' gossip addresses (host:port), may be empty for the first node
	RingRefresh time.Duration
}

// Membership wraps memberlist and maintains a consistent-hash Ring rebuilt
// from the live member list. Ownership is available the moment gossip
// converges — typically sub-second on a LAN, bounded by RingRefresh.
type Membership struct {
	ml   *memberlist.Memberlist
	ring *Ring
	self NodeInfo

	mu   sync.RWMutex
	meta map[string]NodeInfo // nodeID -> announced info, from each member's gossip metadata

	joinErr error // set if Join() with peer addresses reported a problem (non-fatal; see Join)

	quit chan struct{}
	done chan struct{}
}

// Join starts gossiping, optionally connecting to existing peers, and
// returns once this node's own membership is established. It does not wait
// for peers to be reachable — Join addresses that are down are logged and
// skipped, matching memberlist's own semantics (a partial join is not fatal;
// gossip fills in the rest as peers become reachable).
func Join(cfg Config) (*Membership, error) {
	if cfg.RingRefresh <= 0 {
		cfg.RingRefresh = 500 * time.Millisecond
	}
	metaJSON, err := json.Marshal(cfg.Self)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode self node info: %w", err)
	}

	m := &Membership{
		ring: NewRing(),
		self: cfg.Self,
		meta: map[string]NodeInfo{},
		quit: make(chan struct{}),
		done: make(chan struct{}),
	}

	mc := memberlist.DefaultLANConfig()
	mc.Name = cfg.Self.NodeID
	mc.BindAddr = cfg.BindAddr
	mc.BindPort = cfg.BindPort
	mc.AdvertisePort = cfg.BindPort
	mc.Delegate = &staticDelegate{meta: metaJSON}

	ml, err := memberlist.Create(mc)
	if err != nil {
		return nil, fmt.Errorf("cluster: start gossip: %w", err)
	}
	m.ml = ml

	if len(cfg.Join) > 0 {
		// Non-fatal: memberlist.Join returns an error if SOME peers were
		// unreachable even if others succeeded, and it's not fatal if ALL are
		// down yet (they may come up later, or this may be the first node up
		// in a rolling restart). The ring simply reflects whatever converges;
		// callers can surface JoinWarning() through their own logger.
		_, m.joinErr = ml.Join(cfg.Join)
	}

	m.refreshRing()
	go m.refreshLoop(cfg.RingRefresh)
	return m, nil
}

// staticDelegate announces a fixed metadata blob and participates in no
// custom broadcast/state protocol — this MVP needs membership + metadata
// only, not full state replication.
type staticDelegate struct{ meta []byte }

func (d *staticDelegate) NodeMeta(limit int) []byte {
	if len(d.meta) > limit {
		return d.meta[:limit] // memberlist enforces this; truncation would corrupt JSON, so keep NodeInfo small
	}
	return d.meta
}
func (d *staticDelegate) NotifyMsg([]byte)                           {}
func (d *staticDelegate) GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (d *staticDelegate) LocalState(join bool) []byte                { return nil }
func (d *staticDelegate) MergeRemoteState(buf []byte, join bool)     {}

func (m *Membership) refreshLoop(interval time.Duration) {
	defer close(m.done)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-m.quit:
			return
		case <-t.C:
			m.refreshRing()
		}
	}
}

// refreshRing rebuilds the ring and metadata map from the current member
// list. memberlist itself is the source of truth (gossip-converged); this
// just re-derives our local view every tick rather than reacting to
// individual join/leave events — simpler, and cheap enough at this cadence.
func (m *Membership) refreshRing() {
	members := m.ml.Members()
	ids := make([]string, 0, len(members))
	meta := make(map[string]NodeInfo, len(members))
	for _, mem := range members {
		var info NodeInfo
		if err := json.Unmarshal(mem.Meta, &info); err != nil || info.NodeID == "" {
			continue // a peer mid-startup may not have published metadata yet
		}
		ids = append(ids, info.NodeID)
		meta[info.NodeID] = info
	}
	m.ring.SetNodes(ids)
	m.mu.Lock()
	m.meta = meta
	m.mu.Unlock()
}

// OwnerAddr implements the api.Locator interface: returns the API address
// that owns projectID and whether it differs from this node. addr=="" when
// the owner is this node or is not yet known (caller should handle locally).
func (m *Membership) OwnerAddr(projectID int64) (addr string, isRemote bool) {
	nodeID, ok := m.ring.OwnerFor(fmt.Sprintf("project-%d", projectID))
	if !ok || nodeID == m.self.NodeID {
		return "", false
	}
	m.mu.RLock()
	info, ok := m.meta[nodeID]
	m.mu.RUnlock()
	if !ok {
		return "", false // owner known by ID but its metadata hasn't gossiped to us yet
	}
	return info.APIAddr, true
}

// Members returns the current NodeInfo set (observability / tests).
func (m *Membership) Members() []NodeInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]NodeInfo, 0, len(m.meta))
	for _, info := range m.meta {
		out = append(out, info)
	}
	return out
}

// Self returns this node's own announced info.
func (m *Membership) Self() NodeInfo { return m.self }

// JoinWarning returns the (non-fatal) error from joining configured peers,
// if any — some peers being unreachable at start does not stop this node;
// gossip fills in the rest as they come up. Callers should log this, not
// treat it as fatal.
func (m *Membership) JoinWarning() error { return m.joinErr }

// Leave gracefully announces departure (best-effort; crash-only design means
// correctness never depends on this running) and stops the refresh loop.
func (m *Membership) Leave(timeout time.Duration) error {
	close(m.quit)
	<-m.done
	if err := m.ml.Leave(timeout); err != nil {
		return err
	}
	return m.ml.Shutdown()
}
