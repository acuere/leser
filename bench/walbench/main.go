// WAL append benchmark spike (order-2 Milestone 1).
// Measures real events/sec for the record framing and durability policies the WAL
// will use: [len][crc32c][ts][type][payload]. Pure stdlib.
//
// macOS caveat measured explicitly: fsync() on Darwin does NOT flush to physical
// media; only fcntl(F_FULLFSYNC) does. We measure both so the memo is honest.
package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"time"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// frame builds one WAL record: [u32 len][u32 crc32c][i64 ts][u8 type][payload].
// len covers crc..payload. crc covers ts..payload.
func frame(buf []byte, ts int64, typ byte, payload []byte) []byte {
	body := make([]byte, 8+1+len(payload))
	binary.LittleEndian.PutUint64(body[0:8], uint64(ts))
	body[8] = typ
	copy(body[9:], payload)
	crc := crc32.Checksum(body, castagnoli)

	rec := buf[:0]
	var hdr [8]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(4+len(body)))
	binary.LittleEndian.PutUint32(hdr[4:8], crc)
	rec = append(rec, hdr[:]...)
	rec = append(rec, body...)
	return rec
}

type mode struct {
	name string
	// fsyncEvery: 0 = never, 1 = every record, N = every N records.
	fsyncEvery int
	// batchWindow>0: flush+fsync when the time window elapses (batched fsync).
	batchWindow time.Duration
	fullFsync   bool // use F_FULLFSYNC (real durability on macOS)
}

func runMode(dir string, m mode, n, payloadSize int) (float64, float64, error) {
	path := filepath.Join(dir, "seg.wal")
	f, err := os.Create(path)
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 1<<20)

	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	buf := make([]byte, 0, 32+payloadSize)

	doSync := func() error {
		if err := w.Flush(); err != nil {
			return err
		}
		if m.fullFsync {
			return fullFsync(f)
		}
		return f.Sync()
	}

	var totalBytes int64
	start := time.Now()
	lastBatch := start
	for i := 0; i < n; i++ {
		rec := frame(buf, time.Now().UnixNano(), 1, payload)
		nn, err := w.Write(rec)
		if err != nil {
			return 0, 0, err
		}
		totalBytes += int64(nn)

		switch {
		case m.batchWindow > 0:
			if time.Since(lastBatch) >= m.batchWindow {
				if err := doSync(); err != nil {
					return 0, 0, err
				}
				lastBatch = time.Now()
			}
		case m.fsyncEvery == 1:
			if err := doSync(); err != nil {
				return 0, 0, err
			}
		case m.fsyncEvery > 1 && (i+1)%m.fsyncEvery == 0:
			if err := doSync(); err != nil {
				return 0, 0, err
			}
		}
	}
	if err := doSync(); err != nil { // final flush
		return 0, 0, err
	}
	elapsed := time.Since(start).Seconds()
	eps := float64(n) / elapsed
	mbps := float64(totalBytes) / (1 << 20) / elapsed
	return eps, mbps, nil
}

func main() {
	const n = 200_000
	const payloadSize = 512 // typical small event framing unit
	dir, err := os.MkdirTemp("", "walbench")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	fmt.Printf("WAL append benchmark — %d records, %dB payload, dir=%s\n", n, payloadSize, dir)
	fmt.Printf("%-28s %14s %12s\n", "mode", "events/sec", "MB/sec")
	fmt.Println("-------------------------------------------------------")

	modes := []mode{
		{name: "no-fsync (OS cache)", fsyncEvery: 0},
		{name: "fsync_batched (2ms)", batchWindow: 2 * time.Millisecond},
		{name: "fsync_batched (every 1k)", fsyncEvery: 1000},
		{name: "fsync_always (fsync)", fsyncEvery: 1},
		{name: "fsync_always (F_FULLFSYNC)", fsyncEvery: 1, fullFsync: true},
		{name: "batched 2ms + F_FULLFSYNC", batchWindow: 2 * time.Millisecond, fullFsync: true},
	}
	for _, m := range modes {
		// Per-record physical flushes are ~1000x slower; use a smaller sample so
		// the run completes, throughput is a rate so sample size is irrelevant.
		nn := n
		if m.fsyncEvery == 1 { // per-record physical flush: cap the sample
			nn = 3000
		}
		eps, mbps, err := runMode(dir, m, nn, payloadSize)
		if err != nil {
			fmt.Printf("%-28s ERROR: %v\n", m.name, err)
			continue
		}
		fmt.Printf("%-28s %14.0f %12.1f\n", m.name, eps, mbps)
	}
}
