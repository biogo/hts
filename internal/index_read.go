// Copyright ©2014 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package internal

import (
	"code.google.com/p/biogo.bam/bgzf"

	"encoding/binary"
	"fmt"
	"io"
	"sort"
)

// ReadIndex reads the Index from the given io.Reader.
func ReadIndex(r io.Reader, typ string) (Index, error) {
	var (
		idx Index
		err error
	)
	idx.Refs, err = readIndices(r, typ)
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
	return idx, nil
}

func readIndices(r io.Reader, typ string) ([]RefIndex, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
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
	var (
		vOff uint64
		err  error
	)
	chunks := make([]bgzf.Chunk, n)
	for i := range chunks {
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to read chunk begin virtual offset: %v", typ, err)
		}
		chunks[i].Begin = makeOffset(vOff)
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to read chunk end virtual offset: %v", typ, err)
		}
		chunks[i].End = makeOffset(vOff)
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
	var vOff uint64
	offsets := make([]bgzf.Offset, n)
	for i := range offsets {
		err := binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, fmt.Errorf("%s: failed to read tile interval virtual offset: %v", typ, err)
		}
		offsets[i] = makeOffset(vOff)
	}
	if !sort.IsSorted(byVirtOffset(offsets)) {
		sort.Sort(byVirtOffset(offsets))
	}
	return offsets, nil
}
