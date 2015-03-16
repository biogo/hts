// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package csi implements CSIv1 and CSIv2 coordinate sorted indexing.
package csi

import (
	"errors"
	"sort"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
)

var csiMagic = [3]byte{'C', 'S', 'I'}

const (
	// DefaultShift is the default minimum shift setting for a CSI.
	DefaultShift = 14

	// DefaultDepth is the default index depth for a CSI.
	DefaultDepth = 5
)

const (
	nextBinShift  = 3
	statsDummyBin = 0x924a
)

// MinimumShiftFor returns the lowest minimum shift value that can be used to index
// the given maximum position with the given index depth.
func MinimumShiftFor(max int64, depth uint32) (uint32, bool) {
	for shift := uint32(0); shift < 32; shift++ {
		if validIndexPos(int(max), shift, depth) {
			return shift, true
		}
	}
	return 0, false
}

// MinimumDepthFor returns the lowest depth value that can be used to index
// the given maximum position with the given index minimum shift.
func MinimumDepthFor(max int64, shift uint32) (uint32, bool) {
	for depth := uint32(0); depth < 32; depth++ {
		if validIndexPos(int(max), shift, depth) {
			return depth, true
		}
	}
	return 0, false
}

func validIndexPos(i int, minShift, depth uint32) bool { // 0-based.
	return -1 <= i && i <= (1<<(minShift+depth*nextBinShift)-1)-1
}

// New returns a CSI index with the given minimum shift and depth.
// The returned index defaults to CSI version 2.
func New(minShift, depth int) *Index {
	if minShift == 0 {
		minShift = DefaultShift
	}
	if depth == 0 {
		depth = DefaultDepth
	}
	return &Index{Version: 0x2, minShift: uint32(minShift), depth: uint32(depth)}
}

// Index implements coordinate sorted indexing.
type Index struct {
	Auxilliary []byte
	Version    byte

	refs     []refIndex
	unmapped *uint64

	minShift uint32
	depth    uint32

	isSorted   bool
	lastRecord int
}

type refIndex struct {
	bins  []bin
	stats *index.ReferenceStats
}

type bin struct {
	bin     uint32
	left    bgzf.Offset
	records uint64
	chunks  []bgzf.Chunk
}

// NumRefs returns the number of references in the index.
func (i *Index) NumRefs() int {
	return len(i.refs)
}

// ReferenceStats returns the index statistics for the given reference and true
// if the statistics are valid.
func (i *Index) ReferenceStats(id int) (stats index.ReferenceStats, ok bool) {
	s := i.refs[id].stats
	if s == nil {
		return index.ReferenceStats{}, false
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

// Record wraps types that may be indexed by an Index.
type Record interface {
	RefID() int
	Start() int
	End() int
}

// Add records the Record as having being located at the given chunk with the given
// mapping and placement status.
func (i *Index) Add(r Record, c bgzf.Chunk, mapped, placed bool) error {
	if !validIndexPos(r.Start(), i.minShift, i.depth) || !validIndexPos(r.End(), i.minShift, i.depth) {
		return errors.New("csi: attempt to add record outside indexable range")
	}

	if i.unmapped == nil {
		i.unmapped = new(uint64)
	}
	if !placed {
		*i.unmapped++
		return nil
	}

	rid := r.RefID()
	if rid < len(i.refs)-1 {
		return errors.New("csi: attempt to add record out of reference ID sort order")
	}
	if rid == len(i.refs) {
		i.refs = append(i.refs, refIndex{})
		i.lastRecord = 0
	} else if rid > len(i.refs) {
		refs := make([]refIndex, rid+1)
		copy(refs, i.refs)
		i.refs = refs
		i.lastRecord = 0
	}
	ref := &i.refs[rid]

	// Record bin information.
	b := reg2bin(int64(r.Start()), int64(r.End()), i.minShift, i.depth)
	for i, bin := range ref.bins {
		if bin.bin == b {
			for j, chunk := range ref.bins[i].chunks {
				if vOffset(chunk.End) > vOffset(c.Begin) {
					ref.bins[i].chunks[j].End = c.End
					ref.bins[i].records++
					goto found
				}
			}
			ref.bins[i].records++
			ref.bins[i].chunks = append(ref.bins[i].chunks, c)
			goto found
		}
	}
	i.isSorted = false // TODO(kortschak) Consider making use of this more effectively for bin search.
	ref.bins = append(ref.bins, bin{
		bin:     b,
		left:    c.Begin,
		records: 1,
		chunks:  []bgzf.Chunk{c},
	})
found:

	if r.Start() < i.lastRecord {
		return errors.New("csi: attempt to add record out of position sort order")
	}
	i.lastRecord = r.Start()

	// Record index stats.
	if ref.stats == nil {
		ref.stats = &index.ReferenceStats{
			Chunk: c,
		}
	} else {
		ref.stats.Chunk.End = c.End
	}
	if mapped {
		ref.stats.Mapped++
	} else {
		ref.stats.Unmapped++
	}

	return nil
}

// Chunks returns a []bgzf.Chunk that corresponds to the given interval.
func (i *Index) Chunks(rid int, beg, end int) []bgzf.Chunk {
	if rid < 0 || rid >= len(i.refs) {
		return nil
	}
	i.sort()
	ref := i.refs[rid]

	// Collect candidate chunks according to a scheme modified
	// from the one described in the SAM spec under section 5
	// Indexing BAM.
	var chunks []bgzf.Chunk
	for _, bin := range reg2bins(int64(beg), int64(end), i.minShift, i.depth) {
		b := uint32(bin)
		c := sort.Search(len(ref.bins), func(i int) bool { return ref.bins[i].bin >= b })
		if c < len(ref.bins) && ref.bins[c].bin == b {
			left := vOffset(ref.bins[c].left)
			for _, chunk := range ref.bins[c].chunks {
				if vOffset(chunk.End) > left {
					chunks = append(chunks, chunk)
					break
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

var adjacent = index.Adjacent

func (i *Index) sort() {
	if !i.isSorted {
		for _, ref := range i.refs {
			sort.Sort(byBinNumber(ref.bins))
			for _, bin := range ref.bins {
				sort.Sort(byBeginOffset(bin.chunks))
			}
		}
		i.isSorted = true
	}
}

// MergeChunks applies the given MergeStrategy to all bins in the Index.
func (i *Index) MergeChunks(s index.MergeStrategy) {
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

type byBinNumber []bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].bin < b[j].bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

type byBeginOffset []bgzf.Chunk

func (c byBeginOffset) Len() int           { return len(c) }
func (c byBeginOffset) Less(i, j int) bool { return vOffset(c[i].Begin) < vOffset(c[j].Begin) }
func (c byBeginOffset) Swap(i, j int)      { c[i], c[j] = c[j], c[i] }

// calculate bin given an alignment covering [beg,end) (zero-based, half-close-half-open)
func reg2bin(beg, end int64, minShift, depth uint32) uint32 {
	end--
	s := minShift
	t := uint32(((1 << (depth * nextBinShift)) - 1) / 7)
	for level := depth; level > 0; level-- {
		offset := beg >> s
		if offset == end>>s {
			return t + uint32(offset)
		}
		s += nextBinShift
		t -= 1 << (level * nextBinShift)
	}
	return 0
}

// calculate the list of bins that may overlap with region [beg,end) (zero-based)
func reg2bins(beg, end int64, minShift, depth uint32) []uint32 {
	end--
	var list []uint32
	s := minShift + depth*nextBinShift
	for level, t := uint32(0), uint32(0); level <= depth; level++ {
		b := t + uint32(beg>>s)
		e := t + uint32(end>>s)
		for i := b; i <= e; i++ {
			list = append(list, i)
		}
		s -= nextBinShift
		t += 1 << (level * nextBinShift)
	}
	return list
}
