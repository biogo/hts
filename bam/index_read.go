// Copyright ©2014 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"

	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"
)

// ReadIndex reads the BAI Index from the given io.Reader.
func ReadIndex(r io.Reader) (*Index, error) {
	var (
		idx   Index
		magic [4]byte
		err   error
	)
	err = binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return nil, err
	}
	if magic != baiMagic {
		return nil, errors.New("bam: magic number mismatch")
	}
	idx.refs, err = readIndices(r)
	if err != nil {
		return nil, err
	}
	var nUnmapped uint64
	err = binary.Read(r, binary.LittleEndian, &nUnmapped)
	if err == nil {
		idx.unmapped = &nUnmapped
	} else if err != io.EOF {
		return nil, err
	}
	idx.isSorted = true
	return &idx, nil
}

func readIndices(r io.Reader) ([]refIndex, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	idx := make([]refIndex, n)
	for i := range idx {
		idx[i].bins, idx[i].stats, err = readBins(r)
		if err != nil {
			return nil, err
		}
		idx[i].intervals, err = readIntervals(r)
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}

func readBins(r io.Reader) ([]bin, *ReferenceStats, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, nil
	}
	var stats *ReferenceStats
	bins := make([]bin, n)
	for i := 0; i < len(bins); i++ {
		err = binary.Read(r, binary.LittleEndian, &bins[i].bin)
		if err != nil {
			return nil, nil, fmt.Errorf("bam: failed to read bin number: %v", err)
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, nil, fmt.Errorf("bam: failed to read bin count: %v", err)
		}
		if bins[i].bin == statsDummyBin {
			if n != 2 {
				return nil, nil, errors.New("bam: malformed dummy bin header")
			}
			stats, err = readStats(r)
			if err != nil {
				return nil, nil, err
			}
			bins = bins[:len(bins)-1]
			i--
			continue
		}
		bins[i].chunks, err = readChunks(r, n)
		if err != nil {
			return nil, nil, err
		}
	}
	if !sort.IsSorted(byBinNumber(bins)) {
		sort.Sort(byBinNumber(bins))
	}
	return bins, stats, nil
}

func readChunks(r io.Reader, n int32) ([]bgzf.Chunk, error) {
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
			return nil, fmt.Errorf("bam: failed to read chunk begin virtual offset: %v", err)
		}
		chunks[i].Begin = makeOffset(vOff)
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, fmt.Errorf("bam: failed to read chunk end virtual offset: %v", err)
		}
		chunks[i].End = makeOffset(vOff)
	}
	if !sort.IsSorted(byBeginOffset(chunks)) {
		sort.Sort(byBeginOffset(chunks))
	}
	return chunks, nil
}

func readStats(r io.Reader) (*ReferenceStats, error) {
	var (
		vOff  uint64
		stats ReferenceStats
		err   error
	)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats chunk begin virtual offset: %v", err)
	}
	stats.Chunk.Begin = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats chunk end virtual offset: %v", err)
	}
	stats.Chunk.End = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &stats.Mapped)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats mapped count: %v", err)
	}
	err = binary.Read(r, binary.LittleEndian, &stats.Unmapped)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats unmapped count: %v", err)
	}
	return &stats, nil
}

func readIntervals(r io.Reader) ([]bgzf.Offset, error) {
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
			return nil, fmt.Errorf("bam: failed to read tile interval virtual offset: %v", err)
		}
		offsets[i] = makeOffset(vOff)
	}
	if !sort.IsSorted(byVirtOffset(offsets)) {
		sort.Sort(byVirtOffset(offsets))
	}
	return offsets, nil
}
