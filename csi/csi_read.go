// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csi

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sort"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
)

// ReadFrom reads the CSI index from the given io.Reader. Note that
// the csi specification states that the index is stored as BGZF, but
// ReadFrom does not perform decompression.
func ReadFrom(r io.Reader) (*Index, error) {
	var (
		idx   Index
		magic [3]byte
		err   error
	)
	err = binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return nil, err
	}
	if magic != csiMagic {
		return nil, errors.New("csi: magic number mismatch")
	}
	version := []byte{0}
	_, err = io.ReadFull(r, version)
	if err != nil {
		return nil, err
	}
	idx.Version = version[0]
	if idx.Version != 0x1 && idx.Version != 0x2 {
		return nil, fmt.Errorf("csi: unknown version: %d", version[0])
	}
	err = binary.Read(r, binary.LittleEndian, &idx.minShift)
	if err != nil {
		return nil, err
	}
	if int32(idx.minShift) < 0 {
		return nil, errors.New("csi: invalid minimum shift value")
	}
	err = binary.Read(r, binary.LittleEndian, &idx.depth)
	if err != nil {
		return nil, err
	}
	if int32(idx.depth) < 0 {
		return nil, errors.New("csi: invalid index depth value")
	}
	var n int32
	err = binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		idx.Auxilliary = make([]byte, n)
		_, err = io.ReadFull(r, idx.Auxilliary)
		if err != nil {
			return nil, err
		}
	}
	idx.refs, err = readIndices(r, idx.Version)
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

func readIndices(r io.Reader, version byte) ([]refIndex, error) {
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
		idx[i].bins, idx[i].stats, err = readBins(r, version)
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}

func readBins(r io.Reader, version byte) ([]bin, *index.ReferenceStats, error) {
	var n int32
	err := binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, nil, err
	}
	if n == 0 {
		return nil, nil, nil
	}
	var stats *index.ReferenceStats
	bins := make([]bin, n)
	for i := 0; i < len(bins); i++ {
		err = binary.Read(r, binary.LittleEndian, &bins[i].bin)
		if err != nil {
			return nil, nil, fmt.Errorf("csi: failed to read bin number: %v", err)
		}
		var vOff uint64
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, nil, fmt.Errorf("csi: failed to read left virtual offset: %v", err)
		}
		bins[i].left = makeOffset(vOff)
		if version == 0x2 {
			err = binary.Read(r, binary.LittleEndian, &bins[i].records)
			if err != nil {
				return nil, nil, fmt.Errorf("csi: failed to read record count: %v", err)
			}
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, nil, fmt.Errorf("csi: failed to read bin count: %v", err)
		}
		if bins[i].bin == statsDummyBin {
			if n != 2 {
				return nil, nil, errors.New("csi: malformed dummy bin header")
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
			return nil, fmt.Errorf("csi: failed to read chunk begin virtual offset: %v", err)
		}
		chunks[i].Begin = makeOffset(vOff)
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, fmt.Errorf("csi: failed to read chunk end virtual offset: %v", err)
		}
		chunks[i].End = makeOffset(vOff)
	}
	if !sort.IsSorted(byBeginOffset(chunks)) {
		sort.Sort(byBeginOffset(chunks))
	}
	return chunks, nil
}

func readStats(r io.Reader) (*index.ReferenceStats, error) {
	var (
		vOff  uint64
		stats index.ReferenceStats
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
