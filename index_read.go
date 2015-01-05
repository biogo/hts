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
	idx.References, err = readIndices(r)
	if err != nil {
		return nil, err
	}
	var nUnmapped uint64
	err = binary.Read(r, binary.LittleEndian, &nUnmapped)
	if err == nil {
		idx.Unmapped = &nUnmapped
	} else if err != io.EOF {
		return nil, err
	}
	idx.isSorted = true
	return &idx, nil
}

func readIndices(r io.Reader) ([]RefIndex, error) {
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
		idx[i].Bins, idx[i].Stats, err = readBins(r)
		if err != nil {
			return nil, err
		}
		idx[i].Intervals, err = readIntervals(r)
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}

func readBins(r io.Reader) ([]Bin, *IndexStats, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, nil
	}
	var idxStats *IndexStats
	bins := make([]Bin, n)
	for i := 0; i < len(bins); i++ {
		err = binary.Read(r, binary.LittleEndian, &bins[i].Bin)
		if err != nil {
			return nil, nil, fmt.Errorf("bam: failed to read bin number: %v", err)
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, nil, fmt.Errorf("bam: failed to read bin count: %v", err)
		}
		if bins[i].Bin == statsDummyBin {
			if n != 2 {
				return nil, nil, errors.New("bam: malformed dummy bin header")
			}
			idxStats, err = readStats(r)
			if err != nil {
				return nil, nil, err
			}
			bins = bins[:len(bins)-1]
			i--
			continue
		}
		bins[i].Chunks, err = readChunks(r, n)
		if err != nil {
			return nil, nil, err
		}
	}
	if !sort.IsSorted(byBinNumber(bins)) {
		sort.Sort(byBinNumber(bins))
	}
	return bins, idxStats, nil
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

func readStats(r io.Reader) (*IndexStats, error) {
	var (
		vOff     uint64
		idxStats IndexStats
		err      error
	)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats chunk begin virtual offset: %v", err)
	}
	idxStats.Chunk.Begin = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats chunk end virtual offset: %v", err)
	}
	idxStats.Chunk.End = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &idxStats.Mapped)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats mapped count: %v", err)
	}
	err = binary.Read(r, binary.LittleEndian, &idxStats.Unmapped)
	if err != nil {
		return nil, fmt.Errorf("bam: failed to read index stats unmapped count: %v", err)
	}
	return &idxStats, nil
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
