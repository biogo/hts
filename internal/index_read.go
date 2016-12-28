// Copyright ©2014 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/biogo/hts/bgzf"
)

// ReadIndex reads the Index from the given io.Reader.
func ReadIndex(r io.Reader, n int32, typ string) (Index, error) {
	var (
		idx Index
		err error
	)
	idx.Refs, err = readIndices(r, n, typ)
	if err != nil {
		return idx, err
	}
	var nUnmapped uint64
	err = binary.Read(r, binary.LittleEndian, &nUnmapped)
	if err == nil {
		idx.Unmapped = &nUnmapped
	} else if err != io.EOF {
		return idx, err
	}
	idx.IsSorted = true

	// Set the index of the last record to max int to
	// prevent addition of records out of order. This
	// means that the only way to append to an index is
	// to re-index and add to that created index.
	// TODO(kortschak) See if index appending is feasible
	// and needed.
	idx.LastRecord = int(^uint(0) >> 1)

	return idx, nil
}

func readIndices(r io.Reader, n int32, typ string) ([]RefIndex, error) {
	var err error
	idx := make([]RefIndex, n)
	for i := range idx {
		idx[i].Bins, idx[i].Stats, err = readBins(r, typ)
		if err != nil {
			return nil, err
		}
		idx[i].Intervals, err = readIntervals(r, typ)
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}

func readBins(r io.Reader, typ string) ([]Bin, *ReferenceStats, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, nil
	}
	var stats *ReferenceStats
	bins := make([]Bin, n)
	for i := 0; i < len(bins); i++ {
		err = binary.Read(r, binary.LittleEndian, &bins[i].Bin)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: failed to read bin number: %v", typ, err)
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: failed to read bin count: %v", typ, err)
		}
		if bins[i].Bin == StatsDummyBin {
			if n != 2 {
				return nil, nil, fmt.Errorf("%s: malformed dummy bin header", typ)
			}
			stats, err = readStats(r, typ)
			if err != nil {
				return nil, nil, err
			}
			bins = bins[:len(bins)-1]
			i--
			continue
		}
		bins[i].Chunks, err = readChunks(r, n, typ)
		if err != nil {
			return nil, nil, err
		}
	}
	if !sort.IsSorted(byBinNumber(bins)) {
		sort.Sort(byBinNumber(bins))
	}
	return bins, stats, nil
}

func readChunks(r io.Reader, n int32, typ string) ([]bgzf.Chunk, error) {
	if n == 0 {
		return nil, nil
	}
	chunks := make([]bgzf.Chunk, n)
	var buf [16]byte
	for i := range chunks {
		// Get the begin and end offset in a single read.
		_, err := io.ReadFull(r, buf[:])
		if err != nil {
			return nil, fmt.Errorf("%s: failed to read chunk virtual offset: %v", typ, err)
		}
		chunks[i].Begin = makeOffset(binary.LittleEndian.Uint64(buf[:8]))
		chunks[i].End = makeOffset(binary.LittleEndian.Uint64(buf[8:]))
	}
	if !sort.IsSorted(byBeginOffset(chunks)) {
		sort.Sort(byBeginOffset(chunks))
	}
	return chunks, nil
}

func readStats(r io.Reader, typ string) (*ReferenceStats, error) {
	var (
		vOff  uint64
		stats ReferenceStats
		err   error
	)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to read index stats chunk begin virtual offset: %v", typ, err)
	}
	stats.Chunk.Begin = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to read index stats chunk end virtual offset: %v", typ, err)
	}
	stats.Chunk.End = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &stats.Mapped)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to read index stats mapped count: %v", typ, err)
	}
	err = binary.Read(r, binary.LittleEndian, &stats.Unmapped)
	if err != nil {
		return nil, fmt.Errorf("%s: failed to read index stats unmapped count: %v", typ, err)
	}
	return &stats, nil
}

func readIntervals(r io.Reader, typ string) ([]bgzf.Offset, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	offsets := make([]bgzf.Offset, n)
	// chunkSize determines the number of offsets consumed by each binary.Read.
	const chunkSize = 512
	var vOffs [chunkSize]uint64
	for i := 0; i < int(n); i += chunkSize {
		l := min(int(n)-i, len(vOffs))
		err = binary.Read(r, binary.LittleEndian, vOffs[:l])
		if err != nil {
			return nil, fmt.Errorf("%s: failed to read tile interval virtual offset: %v", typ, err)
		}
		for k := 0; k < l; k++ {
			offsets[i+k] = makeOffset(vOffs[k])
		}
	}

	if !sort.IsSorted(byVirtOffset(offsets)) {
		sort.Sort(byVirtOffset(offsets))
	}
	return offsets, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
