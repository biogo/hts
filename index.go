// Copyright ©2014 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"

	"errors"
	"sort"
)

var baiMagic = [4]byte{'B', 'A', 'I', 0x1}

const (
	tileWidth     = 0x4000
	statsDummyBin = 0x924a
)

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

func isPlaced(r *Record) bool {
	return r.Ref != nil && r.Pos != -1
}

type byBinNumber []Bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].Bin < b[j].Bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type byBeginOffset []Chunk

func (c byBeginOffset) Len() int           { return len(c) }
func (c byBeginOffset) Less(i, j int) bool { return vOffset(c[i].Begin) < vOffset(c[j].Begin) }
func (c byBeginOffset) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

type byVirtOffset []bgzf.Offset

func (o byVirtOffset) Len() int           { return len(o) }
func (o byVirtOffset) Less(i, j int) bool { return vOffset(o[i]) < vOffset(o[j]) }
func (o byVirtOffset) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

type Index struct {
	References []RefIndex
	Unmapped   *uint64
	isSorted   bool
	lastRecord int
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

func (i *Index) Sort() {
	if !i.isSorted {
		for _, ref := range i.References {
			sort.Sort(byBinNumber(ref.Bins))
			for _, bin := range ref.Bins {
				sort.Sort(byBeginOffset(bin.Chunks))
			}
			sort.Sort(byVirtOffset(ref.Intervals))
		}
		i.isSorted = true
	}
}

func (i *Index) Add(r *Record, c Chunk) error {
	if i.Unmapped == nil {
		i.Unmapped = new(uint64)
	}
	if !isPlaced(r) {
		*i.Unmapped++
		return nil
	}

	rid := r.Reference().ID()
	if rid < len(i.References)-1 {
		return errors.New("bam: attempt to add record out of reference ID sort order")
	}
	if rid == len(i.References) {
		i.References = append(i.References, RefIndex{})
	} else {
		refs := make([]RefIndex, rid+1)
		copy(refs, i.References)
		i.References = refs
	}
	ref := &i.References[rid]

	// Record bin information.
	b := uint32(r.Bin())
	for i, bin := range ref.Bins {
		if bin.Bin == b {
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
	i.isSorted = false // TODO(kortschak) Consider making use of this more effectively for bin search.
	ref.Bins = append(ref.Bins, Bin{
		Bin:    b,
		Chunks: []Chunk{c},
	})
found:

	// Record interval tile information.
	biv := r.Start() / tileWidth
	if r.Start() < i.lastRecord {
		return errors.New("bam: attempt to add record out of position sort order")
	}
	i.lastRecord = r.Start()
	eiv := r.End() / tileWidth
	if eiv == len(ref.Intervals) {
		if eiv > biv {
			panic("bam: unexpected alignment length")
		}
		ref.Intervals = append(ref.Intervals, c.Begin)
	} else if eiv > len(ref.Intervals) {
		intvs := make([]bgzf.Offset, eiv)
		if len(ref.Intervals) > biv {
			biv = len(ref.Intervals)
		}
		for iv, offset := range intvs[biv:eiv] {
			if !isZero(offset) {
				panic("bam: unexpected non-zero offset")
			}
			intvs[iv+biv] = c.Begin
		}
		copy(intvs, ref.Intervals)
		ref.Intervals = intvs
	}

	// Record index stats.
	if ref.Stats == nil {
		ref.Stats = &IndexStats{
			Chunk: c,
		}
	} else {
		ref.Stats.Chunk.End = c.End
	}
	if r.Flags&Unmapped == 0 {
		ref.Stats.Mapped++
	} else {
		ref.Stats.Unmapped++
	}

	return nil
}

func (i *Index) Chunks(rid, beg, end int) []Chunk {
	if rid >= len(i.References) {
		return nil
	}
	i.Sort()
	ref := i.References[rid]

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
