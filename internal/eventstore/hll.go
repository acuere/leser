package eventstore

import (
	"encoding/base64"
	"errors"
	"hash/fnv"
	"math"
	"math/bits"
)

// HLL is a HyperLogLog sketch for unique counts (order-2 §2.3: sketches are
// maintained at compaction time, not computed on read). Precision 12 →
// 4096 registers, 4KB, standard error ≈ 1.6% — right-sized for "affected
// users" on an issue.
type HLL struct {
	regs []uint8
}

const (
	hllP = 12
	hllM = 1 << hllP // 4096
)

// NewHLL returns an empty sketch.
func NewHLL() *HLL { return &HLL{regs: make([]uint8, hllM)} }

// Add inserts a value.
func (h *HLL) Add(s string) {
	f := fnv.New64a()
	f.Write([]byte(s))
	// FNV-1a has poor avalanche in its high bits (sequential keys collide on
	// the register index); run murmur3's fmix64 finalizer to fix that.
	x := fmix64(f.Sum64())
	idx := x >> (64 - hllP)
	rank := uint8(bits.LeadingZeros64(x<<hllP|1)) + 1
	if rank > h.regs[idx] {
		h.regs[idx] = rank
	}
}

// fmix64 is murmur3's 64-bit finalizer: full avalanche, so every output bit
// depends on every input bit.
func fmix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Merge unions another sketch into this one.
func (h *HLL) Merge(o *HLL) {
	for i, v := range o.regs {
		if v > h.regs[i] {
			h.regs[i] = v
		}
	}
}

// Estimate returns the cardinality estimate with small-range correction.
func (h *HLL) Estimate() uint64 {
	alpha := 0.7213 / (1 + 1.079/float64(hllM))
	var sum float64
	zeros := 0
	for _, v := range h.regs {
		sum += 1 / float64(uint64(1)<<v)
		if v == 0 {
			zeros++
		}
	}
	e := alpha * hllM * hllM / sum
	if e <= 2.5*hllM && zeros > 0 { // linear counting for small cardinalities
		e = hllM * math.Log(float64(hllM)/float64(zeros))
	}
	return uint64(e + 0.5)
}

// MarshalText encodes the sketch for parquet footer metadata (base64).
func (h *HLL) MarshalText() string {
	return base64.StdEncoding.EncodeToString(h.regs)
}

// UnmarshalHLL decodes a footer-stored sketch.
func UnmarshalHLL(s string) (*HLL, error) {
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if len(b) != hllM {
		return nil, errors.New("eventstore: bad hll size")
	}
	return &HLL{regs: b}, nil
}
