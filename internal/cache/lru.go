// Package cache is an in-process LRU bounded by BYTES, not entry count —
// entry counts lie (order-2 §2.5). Replaces Redis/Memcached for single-node.
package cache

import (
	"container/list"
	"sync"
	"time"
)

// entry is one cached value with its accounted size and expiry.
type entry[V any] struct {
	key     string
	val     V
	bytes   int64
	expires time.Time
}

// LRU is a byte-bounded, TTL-aware LRU. Safe for concurrent use.
type LRU[V any] struct {
	mu       sync.Mutex
	maxBytes int64
	curBytes int64
	ttl      time.Duration
	ll       *list.List // front = most recent
	items    map[string]*list.Element

	hits, misses uint64
}

// New creates an LRU capped at maxBytes with a per-entry TTL (0 = no expiry).
func New[V any](maxBytes int64, ttl time.Duration) *LRU[V] {
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return &LRU[V]{maxBytes: maxBytes, ttl: ttl, ll: list.New(), items: map[string]*list.Element{}}
}

// Get returns the value and whether it was present and fresh.
func (c *LRU[V]) Get(key string) (V, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var zero V
	el, ok := c.items[key]
	if !ok {
		c.misses++
		return zero, false
	}
	en := el.Value.(*entry[V])
	if c.ttl > 0 && time.Now().After(en.expires) {
		c.removeElement(el)
		c.misses++
		return zero, false
	}
	c.ll.MoveToFront(el)
	c.hits++
	return en.val, true
}

// Set inserts or replaces a value with an explicit byte cost. Values larger
// than the whole cache are rejected rather than evicting everything.
func (c *LRU[V]) Set(key string, val V, bytes int64) {
	if bytes <= 0 {
		bytes = 1
	}
	if bytes > c.maxBytes {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeElement(el)
	}
	en := &entry[V]{key: key, val: val, bytes: bytes, expires: time.Now().Add(c.ttl)}
	c.items[key] = c.ll.PushFront(en)
	c.curBytes += bytes
	for c.curBytes > c.maxBytes {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
	}
}

// Delete removes a key (e.g. on DSN revocation).
func (c *LRU[V]) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.removeElement(el)
	}
}

// removeElement must be called with mu held.
func (c *LRU[V]) removeElement(el *list.Element) {
	en := el.Value.(*entry[V])
	c.ll.Remove(el)
	delete(c.items, en.key)
	c.curBytes -= en.bytes
}

// Stats returns (hits, misses, currentBytes).
func (c *LRU[V]) Stats() (uint64, uint64, int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.curBytes
}
