// Copyright ©2014 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"
	"code.google.com/p/biogo.bam/sam"

	"errors"
	"sort"
)

var baiMagic = [4]byte{'B', 'A', 'I', 0x1}

const (
	tileWidth     = 0x4000
	statsDummyBin = 0x924a
)

// Index is a BAI index.
type Index struct {
	refs       []refIndex
	unmapped   *uint64
	isSorted   bool
	lastRecord int
}

type refIndex struct {
	bins      []bin
	stats     *ReferenceStats
	intervals []bgzf.Offset
}

type bin struct {
	bin    uint32
	chunks []bgzf.Chunk
}

// ReferenceStats holds mapping statistics for a BAM reference
type ReferenceStats struct {
	// Chunk is the span of the BAM holding alignments
	// to the reference.
	Chunk bgzf.Chunk

	// Mapped is the count of mapped reads.
	Mapped uint64

	// Unmapped is the count of unmapped reads.
	Unmapped uint64
}

// NumRefs returns the number of references in the index.
func (i *Index) NumRefs() int {
	return len(i.refs)
}

// ReferenceStats returns the index statistics for the given reference and true
// if the statistics are valid.
func (i *Index) ReferenceStats(id int) (stats ReferenceStats, ok bool) {
	s := i.refs[id].stats
	if s == nil {
		return ReferenceStats{}, false
	}
	return *s, true
}

// Unmapped returns the number of unmapped reads and true if the count is valid.
func (i *Index) Unmapped() (n uint64, ok bool) {
	if i.unmapped == nil {
		return 0, false
	}
	return *i.unmapped, true
}

// Add records the SAM record as having being located at the given chunk.
func (i *Index) Add(r *sam.Record, c bgzf.Chunk) error {
	if !validIndexPos(r.Start()) || !validIndexPos(r.End()) {
		return errors.New("bam: attempt to add record outside indexable range")
	}

	if i.unmapped == nil {
		i.unmapped = new(uint64)
	}
	if !isPlaced(r) {
		*i.unmapped++
		return nil
	}

	rid := r.Ref.ID()
	if rid < len(i.refs)-1 {
		return errors.New("bam: attempt to add record out of reference ID sort order")
	}
	if rid == len(i.refs) {
		i.refs = append(i.refs, refIndex{})
	} else {
		refs := make([]refIndex, rid+1)
		copy(refs, i.refs)
		i.refs = refs
	}
	ref := &i.refs[rid]

	// Record bin information.
	b := uint32(r.Bin())
	for i, bin := range ref.bins {
		if bin.bin == b {
			for j, chunk := range ref.bins[i].chunks {
				if vOffset(chunk.End) > vOffset(c.Begin) {
					ref.bins[i].chunks[j].End = c.End
					goto found
				}
			}
			ref.bins[i].chunks = append(ref.bins[i].chunks, c)
			goto found
		}
	}
	i.isSorted = false // TODO(kortschak) Consider making use of this more effectively for bin search.
	ref.bins = append(ref.bins, bin{
		bin:    b,
		chunks: []bgzf.Chunk{c},
	})
found:

	// Record interval tile information.
	biv := r.Start() / tileWidth
	if r.Start() < i.lastRecord {
		return errors.New("bam: attempt to add record out of position sort order")
	}
	i.lastRecord = r.Start()
	eiv := r.End() / tileWidth
	if eiv == len(ref.intervals) {
		if eiv > biv {
			panic("bam: unexpected alignment length")
		}
		ref.intervals = append(ref.intervals, c.Begin)
	} else if eiv > len(ref.intervals) {
		intvs := make([]bgzf.Offset, eiv)
		if len(ref.intervals) > biv {
			biv = len(ref.intervals)
		}
		for iv, offset := range intvs[biv:eiv] {
			if !isZero(offset) {
				panic("bam: unexpected non-zero offset")
			}
			intvs[iv+biv] = c.Begin
		}
		copy(intvs, ref.intervals)
		ref.intervals = intvs
	}

	// Record index stats.
	if ref.stats == nil {
		ref.stats = &ReferenceStats{
			Chunk: c,
		}
	} else {
		ref.stats.Chunk.End = c.End
	}
	if r.Flags&sam.Unmapped == 0 {
		ref.stats.Mapped++
	} else {
		ref.stats.Unmapped++
	}

	return nil
}

// Chunks returns a []bgzf.Chunk that correspond to the given genomic interval.
func (i *Index) Chunks(r *sam.Reference, beg, end int) []bgzf.Chunk {
	rid := r.ID()
	if rid < 0 || rid >= len(i.refs) {
		return nil
	}
	i.sort()
	ref := i.refs[rid]

	iv := beg / tileWidth
	if iv >= len(ref.intervals) {
		return nil
	}

	// Collect candidate chunks according to the scheme described in
	// the SAM spec under section 5 Indexing BAM.
	var chunks []bgzf.Chunk
	for _, bin := range reg2bins(beg, end) {
		b := uint32(bin)
		c := sort.Search(len(ref.bins), func(i int) bool { return ref.bins[i].bin >= b })
		if c < len(ref.bins) && ref.bins[c].bin == b {
			for _, chunk := range ref.bins[c].chunks {
				// Here we check all tiles starting from the left end of the
				// query region until we get a non-zero offset. The spec states
				// that we only need to check tiles that contain beg. That is
				// not correct since we may have no alignments at the left end
				// of the query region.
				for j, tile := range ref.intervals[iv:] {
					if isZero(tile) {
						continue
					}
					tbeg := (j + iv) * tileWidth
					tend := tbeg + tileWidth
					// We allow adjacent alignment since samtools behaviour here
					// has always irritated me and it is cheap to discard these
					// later if they are not wanted.
					if tend >= beg && tbeg <= end && vOffset(chunk.End) > vOffset(tile) {
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

	return adjacent(chunks)
}

func (i *Index) sort() {
	if !i.isSorted {
		for _, ref := range i.refs {
			sort.Sort(byBinNumber(ref.bins))
			for _, bin := range ref.bins {
				sort.Sort(byBeginOffset(bin.chunks))
			}
			sort.Sort(byVirtOffset(ref.intervals))
		}
		i.isSorted = true
	}
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

func isPlaced(r *sam.Record) bool {
	return r.Ref != nil && r.Pos != -1
}

type byBinNumber []bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].bin < b[j].bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type byBeginOffset []bgzf.Chunk

func (c byBeginOffset) Len() int           { return len(c) }
func (c byBeginOffset) Less(i, j int) bool { return vOffset(c[i].Begin) < vOffset(c[j].Begin) }
func (c byBeginOffset) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type byVirtOffset []bgzf.Offset

func (o byVirtOffset) Len() int           { return len(o) }
func (o byVirtOffset) Less(i, j int) bool { return vOffset(o[i]) < vOffset(o[j]) }
func (o byVirtOffset) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

// Strategy represents a chunk compression strategy.
type Strategy func([]bgzf.Chunk) []bgzf.Chunk

var (
	// Identity leaves the []bgzf.Chunk unaltered.
	Identity Strategy = identity

	// Adjacent merges contiguous bgzf.Chunks.
	Adjacent Strategy = adjacent

	// Squash merges all bgzf.Chunks into a single bgzf.Chunk.
	Squash Strategy = squash
)

// CompressorStrategy returns a Strategy that will merge bgzf.Chunks
// that have a distance between BGZF block starts less than or equal
// to near.
func CompressorStrategy(near int64) Strategy {
	return func(chunks []bgzf.Chunk) []bgzf.Chunk {
		if len(chunks) == 0 {
			return nil
		}
		for c := 1; c < len(chunks); c++ {
			leftChunk := chunks[c-1]
			rightChunk := &chunks[c]
			if leftChunk.End.File+near >= rightChunk.Begin.File {
				rightChunk.Begin = leftChunk.Begin
				if vOffset(leftChunk.End) > vOffset(rightChunk.End) {
					rightChunk.End = leftChunk.End
				}
				chunks = append(chunks[:c-1], chunks[c:]...)
				c--
			}
		}
		return chunks
	}
}

func identity(chunks []bgzf.Chunk) []bgzf.Chunk { return chunks }

func adjacent(chunks []bgzf.Chunk) []bgzf.Chunk {
	if len(chunks) == 0 {
		return nil
	}
	for c := 1; c < len(chunks); c++ {
		leftChunk := chunks[c-1]
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

func squash(chunks []bgzf.Chunk) []bgzf.Chunk {
	if len(chunks) == 0 {
		return nil
	}
	left := chunks[0].Begin
	right := chunks[0].End
	for _, c := range chunks[1:] {
		if vOffset(c.End) > vOffset(right) {
			right = c.End
		}
	}
	return []bgzf.Chunk{{Begin: left, End: right}}
}

// MergeChunks applies the given Strategy to all bins in the Index.
func (i *Index) MergeChunks(s Strategy) {
	if s == nil {
		return
	}
	for _, ref := range i.refs {
		for b, bin := range ref.bins {
			if !sort.IsSorted(byBeginOffset(bin.chunks)) {
				sort.Sort(byBeginOffset(bin.chunks))
			}
			ref.bins[b].chunks = s(bin.chunks)
			if !sort.IsSorted(byBeginOffset(bin.chunks)) {
				sort.Sort(byBeginOffset(bin.chunks))
			}
		}
	}
}
