// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"

	"encoding/binary"
	"errors"
	"io"
	"sort"
)

type Index struct {
	References []RefIndex
	Unmapped   *uint64
}

func (i *Index) Chunks(rid, beg, end int) []Chunk {
	if rid >= len(i.References) {
		return nil
	}
	ref := i.References[rid]

	const tileWidth = 0x4000
	iv := beg / tileWidth
	if iv >= len(ref.Intervals) {
		return nil
	}

	// Collect candidate chunks according to the scheme described in
	// the SAM spec under section 5 Indexing BAM.
	var chunks []Chunk
	for _, bin := range reg2bins(beg, end) {
		b := uint32(bin)
		c := sort.Search(len(ref.Bins), func(i int) bool { return ref.Bins[i].Bin >= b })
		if c < len(ref.Bins) && ref.Bins[c].Bin == b {
			for _, chunk := range ref.Bins[c].Chunks {
				// Here we check all tiles starting from the left end of the
				// query region until we get a non-zero offset. The spec states
				// that we only need to check tiles that contain beg. That is
				// not correct since we may have no alignments at the left end
				// of the query region.
				for j, tile := range ref.Intervals[iv:] {
					tileOffset := vOffset(tile)
					if tileOffset == 0 {
						continue
					}
					tbeg := (j + iv) * tileWidth
					tend := tbeg + tileWidth
					// We allow adjacent alignment since samtools behaviour here
					// has always irritated me and it is cheap to discard these
					// later if they are not wanted.
					if tend >= beg && tbeg <= end && vOffset(chunk.End) > tileOffset {
						chunks = append(chunks, chunk)
						break
					}
				}
			}
		}
	}

	// Sort and merge overlaps.
	if !sort.IsSorted(byBeginOffset(chunks)) {
		sort.Sort(byBeginOffset(chunks))
	}
	for c := 1; c < len(chunks); c++ {
		leftChunk := &chunks[c-1]
		rightChunk := &chunks[c]
		leftEndOffset := vOffset(leftChunk.End)
		if leftEndOffset >= vOffset(rightChunk.Begin) {
			rightChunk.Begin = leftChunk.Begin
			if leftEndOffset > vOffset(rightChunk.End) {
				rightChunk.End = leftChunk.End
			}
			chunks = append(chunks[:c-1], chunks[c:]...)
			c--
		}
	}

	return chunks
}

func vOffset(o bgzf.Offset) int64 {
	return o.File<<16 | int64(o.Block)
}

type RefIndex struct {
	Bins      []Bin
	Stats     *IndexStats
	Intervals []bgzf.Offset
}

type Bin struct {
	Bin    uint32
	Chunks []Chunk
}

type IndexStats struct {
	Chunk    Chunk
	Mapped   uint64
	Unmapped uint64
}

type Chunk struct {
	Begin bgzf.Offset
	End   bgzf.Offset
}

var baiMagic = [4]byte{'B', 'A', 'I', 0x1}

func (b *Index) Unmarshal(r io.Reader) error {
	var (
		nRef  int32
		err   error
		magic [4]byte
	)
	err = binary.Read(r, binary.LittleEndian, &magic)
	if err != nil {
		return err
	}
	if magic != baiMagic {
		return errors.New("bam: magic number mismatch")
	}
	err = binary.Read(r, binary.LittleEndian, &nRef)
	if err != nil {
		return err
	}
	b.References, err = readIndices(r, nRef)
	if err != nil {
		return err
	}
	var nUnmapped uint64
	err = binary.Read(r, binary.LittleEndian, &nUnmapped)
	if err == nil {
		b.Unmapped = &nUnmapped
	} else if err == io.EOF {
		err = nil
	}
	return err
}

func readIndices(r io.Reader, n int32) ([]RefIndex, error) {
	if n == 0 {
		return nil, nil
	}
	var err error
	idx := make([]RefIndex, n)
	for i := range idx {
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, err
		}
		idx[i].Bins, idx[i].Stats, err = readBins(r, n)
		if err != nil {
			return nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, err
		}
		idx[i].Intervals, err = readIntervals(r, n)
		if err != nil {
			return nil, err
		}
	}
	return idx, nil
}

const statsDummyBin = 0x924a

type byBinNumber []Bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].Bin < b[j].Bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

func readBins(r io.Reader, n int32) ([]Bin, *IndexStats, error) {
	if n == 0 {
		return nil, nil, nil
	}
	var (
		idxStats *IndexStats
		err      error
	)
	bins := make([]Bin, n)
	for i := 0; i < len(bins); i++ {
		err = binary.Read(r, binary.LittleEndian, &bins[i].Bin)
		if err != nil {
			return nil, nil, err
		}
		err = binary.Read(r, binary.LittleEndian, &n)
		if err != nil {
			return nil, nil, err
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

func makeOffset(vOff uint64) bgzf.Offset {
	return bgzf.Offset{
		File:  int64(vOff >> 16),
		Block: uint16(vOff),
	}
}

type byBeginOffset []Chunk

func (c byBeginOffset) Len() int           { return len(c) }
func (c byBeginOffset) Less(i, j int) bool { return vOffset(c[i].Begin) < vOffset(c[j].Begin) }
func (c byBeginOffset) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

func readChunks(r io.Reader, n int32) ([]Chunk, error) {
	if n == 0 {
		return nil, nil
	}
	var (
		vOff uint64
		err  error
	)
	chunks := make([]Chunk, n)
	for i := range chunks {
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, err
		}
		chunks[i].Begin = makeOffset(vOff)
		err = binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, err
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
		return nil, err
	}
	idxStats.Chunk.Begin = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &vOff)
	if err != nil {
		return nil, err
	}
	idxStats.Chunk.End = makeOffset(vOff)
	err = binary.Read(r, binary.LittleEndian, &idxStats.Mapped)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.LittleEndian, &idxStats.Unmapped)
	if err != nil {
		return nil, err
	}
	return &idxStats, nil
}

type byVirtOffset []bgzf.Offset

func (o byVirtOffset) Len() int           { return len(o) }
func (o byVirtOffset) Less(i, j int) bool { return vOffset(o[i]) < vOffset(o[j]) }
func (o byVirtOffset) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

func readIntervals(r io.Reader, n int32) ([]bgzf.Offset, error) {
	if n == 0 {
		return nil, nil
	}
	var vOff uint64
	offsets := make([]bgzf.Offset, n)
	for i := range offsets {
		err := binary.Read(r, binary.LittleEndian, &vOff)
		if err != nil {
			return nil, err
		}
		offsets[i] = makeOffset(vOff)
	}
	if !sort.IsSorted(byVirtOffset(offsets)) {
		sort.Sort(byVirtOffset(offsets))
	}
	return offsets, nil
}

const (
	maxInt16  = int(int16(^uint16(0) >> 1))
	minInt16  = -int(maxInt16) - 1
	maxUint16 = int(^uint16(0))
	maxInt32  = int(int32(^uint32(0) >> 1))
	minInt32  = -int(maxInt32) - 1
	maxUint32 = int64(^uint32(0))
)

const wordBits = 29

func validInt16(i int) bool    { return minInt16 <= i && i <= maxInt16 }
func validInt32(i int) bool    { return minInt32 <= i && i <= maxInt32 }
func validUint16(i int) bool   { return 0 <= i && i <= maxUint16 }
func validUint32(i int) bool   { return 0 <= i && int64(i) <= maxUint32 }
func validPos(i int) bool      { return 0 <= i && i <= 1<<wordBits-1 }
func validTmpltLen(i int) bool { return -(1<<wordBits) <= i && i <= 1<<wordBits-1 }
func validLen(i int) bool      { return 1 <= i && i <= 1<<wordBits-1 }

const nextBinShift = 3

const (
	level0 = uint16(((1 << (iota * nextBinShift)) - 1) / 7)
	level1
	level2
	level3
	level4
	level5
)

const (
	level0Shift = wordBits - (iota * nextBinShift)
	level1Shift
	level2Shift
	level3Shift
	level4Shift
	level5Shift
)

// calculate bin given an alignment covering [beg,end) (zero-based, half-close-half-open)
func reg2bin(beg, end int) uint16 {
	end--
	switch {
	case beg>>level5Shift == end>>level5Shift:
		return level5 + uint16(beg>>level5Shift)
	case beg>>level4Shift == end>>level4Shift:
		return level4 + uint16(beg>>level4Shift)
	case beg>>level3Shift == end>>level3Shift:
		return level3 + uint16(beg>>level3Shift)
	case beg>>level2Shift == end>>level2Shift:
		return level2 + uint16(beg>>level2Shift)
	case beg>>level1Shift == end>>level1Shift:
		return level1 + uint16(beg>>level1Shift)
	}
	return level0
}

// calculate the list of bins that may overlap with region [beg,end) (zero-based)
func reg2bins(beg, end int) []uint16 {
	end--
	list := []uint16{level0}
	for _, r := range []struct {
		offset, shift uint16
	}{
		{level1, level1Shift},
		{level2, level2Shift},
		{level3, level3Shift},
		{level4, level4Shift},
		{level5, level5Shift},
	} {
		for k := r.offset + uint16(beg>>r.shift); k <= r.offset+uint16(end>>r.shift); k++ {
			list = append(list, k)
		}
	}
	return list
}
