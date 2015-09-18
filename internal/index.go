// Copyright ©2014 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package internal provides shared code for BAI and tabix index implementations.
package internal

import (
	"errors"
	"sort"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
)

const (
	// TileWidth is the length of the interval tiling used
	// in BAI and tabix indexes.
	TileWidth = 0x4000

	// StatsDummyBin is the bin number of the reference
	// statistics bin used in BAI and tabix indexes.
	StatsDummyBin = 0x924a
)

// Index is a coordinate based index.
type Index struct {
	Refs       []RefIndex
	Unmapped   *uint64
	IsSorted   bool
	LastRecord int
}

// RefIndex is the index of a single reference.
type RefIndex struct {
	Bins      []Bin
	Stats     *ReferenceStats
	Intervals []bgzf.Offset
}

// Bin is an index bin.
type Bin struct {
	Bin    uint32
	Chunks []bgzf.Chunk
}

// ReferenceStats holds mapping statistics for a genomic reference.
type ReferenceStats struct {
	// Chunk is the span of the indexed BGZF
	// holding alignments to the reference.
	Chunk bgzf.Chunk

	// Mapped is the count of mapped reads.
	Mapped uint64

	// Unmapped is the count of unmapped reads.
	Unmapped uint64
}

// Record wraps types that may be indexed by an Index.
type Record interface {
	RefID() int
	Start() int
	End() int
}

// Add records the SAM record as having being located at the given chunk.
func (i *Index) Add(r Record, bin uint32, c bgzf.Chunk, placed, mapped bool) error {
	if !IsValidIndexPos(r.Start()) || !IsValidIndexPos(r.End()) {
		return errors.New("index: attempt to add record outside indexable range")
	}

	if i.Unmapped == nil {
		i.Unmapped = new(uint64)
	}
	if !placed {
		*i.Unmapped++
		return nil
	}

	rid := r.RefID()
	if rid < len(i.Refs)-1 {
		return errors.New("index: attempt to add record out of reference ID sort order")
	}
	if rid == len(i.Refs) {
		i.Refs = append(i.Refs, RefIndex{})
		i.LastRecord = 0
	} else if rid > len(i.Refs) {
		Refs := make([]RefIndex, rid+1)
		copy(Refs, i.Refs)
		i.Refs = Refs
		i.LastRecord = 0
	}
	ref := &i.Refs[rid]

	// Record bin information.
	for i, b := range ref.Bins {
		if b.Bin == bin {
			for j, chunk := range ref.Bins[i].Chunks {
				if vOffset(chunk.End) > vOffset(c.Begin) {
					ref.Bins[i].Chunks[j].End = c.End
					goto found
				}
			}
			ref.Bins[i].Chunks = append(ref.Bins[i].Chunks, c)
			goto found
		}
	}
	i.IsSorted = false // TODO(kortschak) Consider making use of this more effectively for bin search.
	ref.Bins = append(ref.Bins, Bin{
		Bin:    bin,
		Chunks: []bgzf.Chunk{c},
	})
found:

	// Record interval tile information.
	biv := r.Start() / TileWidth
	if r.Start() < i.LastRecord {
		return errors.New("index: attempt to add record out of position sort order")
	}
	i.LastRecord = r.Start()
	eiv := r.End() / TileWidth
	if eiv == len(ref.Intervals) {
		if eiv > biv {
			panic("index: unexpected alignment length")
		}
		ref.Intervals = append(ref.Intervals, c.Begin)
	} else if eiv > len(ref.Intervals) {
		intvs := make([]bgzf.Offset, eiv)
		if len(ref.Intervals) > biv {
			biv = len(ref.Intervals)
		}
		for iv, offset := range intvs[biv:eiv] {
			if !isZero(offset) {
				panic("index: unexpected non-zero offset")
			}
			intvs[iv+biv] = c.Begin
		}
		copy(intvs, ref.Intervals)
		ref.Intervals = intvs
	}

	// Record index stats.
	if ref.Stats == nil {
		ref.Stats = &ReferenceStats{
			Chunk: c,
		}
	} else {
		ref.Stats.Chunk.End = c.End
	}
	if mapped {
		ref.Stats.Mapped++
	} else {
		ref.Stats.Unmapped++
	}

	return nil
}

// Chunks returns a []bgzf.Chunk that corresponds to the given genomic interval.
func (i *Index) Chunks(rid, beg, end int) ([]bgzf.Chunk, error) {
	if rid < 0 || rid >= len(i.Refs) {
		return nil, index.ErrNoReference
	}
	i.sort()
	ref := i.Refs[rid]

	iv := beg / TileWidth
	if iv >= len(ref.Intervals) {
		return nil, index.ErrInvalid
	}

	// Collect candidate chunks according to the scheme described in
	// the SAM spec under section 5 Indexing BAM.
	var chunks []bgzf.Chunk
	for _, b := range OverlappingBinsFor(beg, end) {
		c := sort.Search(len(ref.Bins), func(i int) bool { return ref.Bins[i].Bin >= b })
		if c < len(ref.Bins) && ref.Bins[c].Bin == b {
			for _, chunk := range ref.Bins[c].Chunks {
				// Here we check all tiles starting from the left end of the
				// query region until we get a non-zero offset. The spec states
				// that we only need to check tiles that contain beg. That is
				// not correct since we may have no alignments at the left end
				// of the query region.
				chunkEndOffset := vOffset(chunk.End)
				haveNonZero := false
				for j, tile := range ref.Intervals[iv:] {
					// If we have found a non-zero tile, all subsequent active
					// tiles must also be non-zero, so skip zero tiles.
					if haveNonZero && isZero(tile) {
						continue
					}
					haveNonZero = true
					tbeg := (j + iv) * TileWidth
					tend := tbeg + TileWidth
					// We allow adjacent alignment since samtools behaviour here
					// has always irritated me and it is cheap to discard these
					// later if they are not wanted.
					if tend >= beg && tbeg <= end && chunkEndOffset > vOffset(tile) {
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

	return chunks, nil
}

func (i *Index) sort() {
	if !i.IsSorted {
		for _, ref := range i.Refs {
			sort.Sort(byBinNumber(ref.Bins))
			for _, bin := range ref.Bins {
				sort.Sort(byBeginOffset(bin.Chunks))
			}
			sort.Sort(byVirtOffset(ref.Intervals))
		}
		i.IsSorted = true
	}
}

// MergeChunks applies the given MergeStrategy to all bins in the Index.
func (i *Index) MergeChunks(s func([]bgzf.Chunk) []bgzf.Chunk) {
	if s == nil {
		return
	}
	for _, ref := range i.Refs {
		for b, bin := range ref.Bins {
			if !sort.IsSorted(byBeginOffset(bin.Chunks)) {
				sort.Sort(byBeginOffset(bin.Chunks))
			}
			ref.Bins[b].Chunks = s(bin.Chunks)
			if !sort.IsSorted(byBeginOffset(bin.Chunks)) {
				sort.Sort(byBeginOffset(bin.Chunks))
			}
		}
	}
}

const (
	indexWordBits = 29
	nextBinShift  = 3
)

// IsValidIndexPos returns a boolean indicating whether
// the given position is in the valid range for BAM/SAM.
func IsValidIndexPos(i int) bool { return -1 <= i && i <= (1<<indexWordBits-1)-1 } // 0-based.

const (
	level0 = uint32(((1 << (iota * nextBinShift)) - 1) / 7)
	level1
	level2
	level3
	level4
	level5
)

const (
	level0Shift = indexWordBits - (iota * nextBinShift)
	level1Shift
	level2Shift
	level3Shift
	level4Shift
	level5Shift
)

// BinFor returns the bin number for given an interval covering
// [beg,end) (zero-based, half-close-half-open).
func BinFor(beg, end int) uint32 {
	end--
	switch {
	case beg>>level5Shift == end>>level5Shift:
		return level5 + uint32(beg>>level5Shift)
	case beg>>level4Shift == end>>level4Shift:
		return level4 + uint32(beg>>level4Shift)
	case beg>>level3Shift == end>>level3Shift:
		return level3 + uint32(beg>>level3Shift)
	case beg>>level2Shift == end>>level2Shift:
		return level2 + uint32(beg>>level2Shift)
	case beg>>level1Shift == end>>level1Shift:
		return level1 + uint32(beg>>level1Shift)
	}
	return level0
}

// OverlappingBinsFor returns the bin numbers for all bins overlapping
// an interval covering [beg,end) (zero-based, half-close-half-open).
func OverlappingBinsFor(beg, end int) []uint32 {
	end--
	list := []uint32{level0}
	for _, r := range []struct {
		offset, shift uint32
	}{
		{level1, level1Shift},
		{level2, level2Shift},
		{level3, level3Shift},
		{level4, level4Shift},
		{level5, level5Shift},
	} {
		for k := r.offset + uint32(beg>>r.shift); k <= r.offset+uint32(end>>r.shift); k++ {
			list = append(list, k)
		}
	}
	return list
}

func makeOffset(vOff uint64) bgzf.Offset {
	return bgzf.Offset{
		File:  int64(vOff >> 16),
		Block: uint16(vOff),
	}
}

func isZero(o bgzf.Offset) bool {
	return o == bgzf.Offset{}
}

func vOffset(o bgzf.Offset) int64 {
	return o.File<<16 | int64(o.Block)
}

type byBinNumber []Bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].Bin < b[j].Bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type byBeginOffset []bgzf.Chunk

func (c byBeginOffset) Len() int           { return len(c) }
func (c byBeginOffset) Less(i, j int) bool { return vOffset(c[i].Begin) < vOffset(c[j].Begin) }
func (c byBeginOffset) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type byVirtOffset []bgzf.Offset

func (o byVirtOffset) Len() int           { return len(o) }
func (o byVirtOffset) Less(i, j int) bool { return vOffset(o[i]) < vOffset(o[j]) }
func (o byVirtOffset) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }
