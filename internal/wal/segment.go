package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// On-disk formats are versioned (order-2 §5). Segment layout:
//
//	header:  [8B magic "LESRWAL1"][u32 version][u64 baseOffset]   = 20 bytes
//	records: repeated [u32 bodyLen][u32 crc32c][body]
//	body:    [u64 unixNanos][u8 type][payload]
//
// crc32c (Castagnoli) covers body. A record whose length or CRC does not
// validate marks the end of the readable prefix; recovery truncates there.
const (
	segMagic     = "LESRWAL1"
	segVersion   = 1
	segHeaderLen = 8 + 4 + 8
	recHeaderLen = 4 + 4
	bodyFixedLen = 8 + 1
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrCorrupt reports an unreadable record before the expected end of a
	// sealed segment. For the active segment this is handled by truncation.
	ErrCorrupt = errors.New("wal: corrupt record")
	// errTorn is an internal marker for an incomplete/invalid tail record.
	errTorn = errors.New("wal: torn record")
)

// Record is one entry read back from the log.
type Record struct {
	Offset  uint64 // logical, monotonically increasing across segments
	Time    int64  // unix nanos captured at append
	Type    byte
	Payload []byte
}

// segmentName formats the file name for a segment starting at base.
func segmentName(base uint64) string {
	return fmt.Sprintf("%020d.seg", base)
}

// parseSegmentName extracts the base offset from a segment file name.
func parseSegmentName(name string) (uint64, bool) {
	if !strings.HasSuffix(name, ".seg") || len(name) != 24 {
		return 0, false
	}
	n, err := strconv.ParseUint(name[:20], 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// listSegments returns segment base offsets in dir, ascending.
func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var bases []uint64
	for _, e := range ents {
		if b, ok := parseSegmentName(e.Name()); ok {
			bases = append(bases, b)
		}
	}
	sort.Slice(bases, func(i, j int) bool { return bases[i] < bases[j] })
	return bases, nil
}

// createSegment writes a fresh segment file with its header.
func createSegment(dir string, base uint64) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, segmentName(base)), os.O_CREATE|os.O_RDWR|os.O_EXCL, 0o640)
	if err != nil {
		return nil, err
	}
	var hdr [segHeaderLen]byte
	copy(hdr[0:8], segMagic)
	binary.LittleEndian.PutUint32(hdr[8:12], segVersion)
	binary.LittleEndian.PutUint64(hdr[12:20], base)
	if _, err := f.Write(hdr[:]); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// readSegmentHeader validates the header and returns the base offset.
func readSegmentHeader(f *os.File) (uint64, error) {
	var hdr [segHeaderLen]byte
	if _, err := io.ReadFull(f, hdr[:]); err != nil {
		return 0, fmt.Errorf("wal: segment header: %w", err)
	}
	if string(hdr[0:8]) != segMagic {
		return 0, fmt.Errorf("wal: bad magic")
	}
	if v := binary.LittleEndian.Uint32(hdr[8:12]); v != segVersion {
		return 0, fmt.Errorf("wal: unsupported segment version %d", v)
	}
	return binary.LittleEndian.Uint64(hdr[12:20]), nil
}

// appendRecord encodes a record into buf (reused) and returns the encoded bytes.
func appendRecord(buf []byte, ts int64, typ byte, payload []byte) []byte {
	bodyLen := bodyFixedLen + len(payload)
	need := recHeaderLen + bodyLen
	if cap(buf) < need {
		buf = make([]byte, 0, need)
	}
	rec := buf[:need]
	binary.LittleEndian.PutUint32(rec[0:4], uint32(bodyLen))
	body := rec[recHeaderLen:]
	binary.LittleEndian.PutUint64(body[0:8], uint64(ts))
	body[8] = typ
	copy(body[9:], payload)
	binary.LittleEndian.PutUint32(rec[4:8], crc32.Checksum(body, castagnoli))
	return rec
}

// scanRecord reads one record at pos in r. Returns the record (payload is a
// fresh copy), the position after it, or errTorn if the bytes at pos do not
// form a complete valid record.
func scanRecord(r io.ReaderAt, pos int64, size int64, maxRecord int, offset uint64) (Record, int64, error) {
	var hdr [recHeaderLen]byte
	if pos+recHeaderLen > size {
		return Record{}, pos, errTorn
	}
	if _, err := r.ReadAt(hdr[:], pos); err != nil {
		return Record{}, pos, errTorn
	}
	bodyLen := int(binary.LittleEndian.Uint32(hdr[0:4]))
	if bodyLen < bodyFixedLen || bodyLen > maxRecord {
		return Record{}, pos, errTorn
	}
	end := pos + recHeaderLen + int64(bodyLen)
	if end > size {
		return Record{}, pos, errTorn
	}
	body := make([]byte, bodyLen)
	if _, err := r.ReadAt(body, pos+recHeaderLen); err != nil {
		return Record{}, pos, errTorn
	}
	if crc32.Checksum(body, castagnoli) != binary.LittleEndian.Uint32(hdr[4:8]) {
		return Record{}, pos, errTorn
	}
	return Record{
		Offset:  offset,
		Time:    int64(binary.LittleEndian.Uint64(body[0:8])),
		Type:    body[8],
		Payload: body[9:],
	}, end, nil
}

// scanSegment scans f from the header, counting valid records. It returns the
// record count and the byte size of the valid prefix; it never mutates the
// file. The caller truncates the active segment to validSize on open —
// crash-only recovery exercised every boot (order-2 §5).
func scanSegment(f *os.File, maxRecord int) (records uint64, validSize int64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	size := fi.Size()
	pos := int64(segHeaderLen)
	for {
		_, next, serr := scanRecord(f, pos, size, maxRecord, 0)
		if serr != nil {
			break // torn/invalid tail: valid prefix ends here
		}
		records++
		pos = next
	}
	return records, pos, nil
}
