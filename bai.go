// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
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

type Index struct {
	References []RefIndex
	Unmapped   *uint64
	isSorted   bool
	lastRecord int
}

var baiMagic = [4]byte{'B', 'A', 'I', 0x1}

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

const tileWidth = 0x4000

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

func isPlaced(r *Record) bool {
	return r.Ref != nil && r.Pos != -1
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

func isZero(o bgzf.Offset) bool {
	return o == bgzf.Offset{}
}

func vOffset(o bgzf.Offset) int64 {
	return o.File<<16 | int64(o.Block)
}

type RefIndex struct {
	Bins      []Bin
	Stats     *IndexStats
	Intervals []bgzf.Offset
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

const statsDummyBin = 0x924a

type byBinNumber []Bin

func (b byBinNumber) Len() int           { return len(b) }
func (b byBinNumber) Less(i, j int) bool { return b[i].Bin < b[j].Bin }
func (b byBinNumber) Swap(i, j int)      { b[i], b[j] = b[j], b[i] }

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

type byVirtOffset []bgzf.Offset

func (o byVirtOffset) Len() int           { return len(o) }
func (o byVirtOffset) Less(i, j int) bool { return vOffset(o[i]) < vOffset(o[j]) }
func (o byVirtOffset) Swap(i, j int)      { o[i], o[j] = o[j], o[i] }

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

func WriteIndex(w io.Writer, idx *Index) error {
	idx.Sort()
	err := binary.Write(w, binary.LittleEndian, baiMagic)
	if err != nil {
		return err
	}
	err = writeIndices(w, idx.References)
	if err != nil {
		return err
	}
	if idx.Unmapped != nil {
		err = binary.Write(w, binary.LittleEndian, idx.Unmapped)
	}
	return err
}

func writeIndices(w io.Writer, idx []RefIndex) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(idx)))
	if err != nil {
		return err
	}
	for i := range idx {
		err = writeBins(w, idx[i].Bins, idx[i].Stats)
		if err != nil {
			return err
		}
		err = writeIntervals(w, idx[i].Intervals)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBins(w io.Writer, bins []Bin, idxStats *IndexStats) error {
	n := int32(len(bins))
	if idxStats != nil {
		n++
	}
	err := binary.Write(w, binary.LittleEndian, &n)
	if err != nil {
		return err
	}
	for _, b := range bins {
		err = binary.Write(w, binary.LittleEndian, b.Bin)
		if err != nil {
			return fmt.Errorf("bam: failed to write bin number: %v", err)
		}
		err = writeChunks(w, b.Chunks)
		if err != nil {
			return err
		}
	}
	if idxStats != nil {
		return writeStats(w, idxStats)
	}
	return nil
}

func writeChunks(w io.Writer, chunks []Chunk) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(chunks)))
	if err != nil {
		return fmt.Errorf("bam: failed to write bin count: %v", err)
	}
	for _, c := range chunks {
		err = binary.Write(w, binary.LittleEndian, vOffset(c.Begin))
		if err != nil {
			return fmt.Errorf("bam: failed to write chunk begin virtual offset: %v", err)
		}
		err = binary.Write(w, binary.LittleEndian, vOffset(c.End))
		if err != nil {
			return fmt.Errorf("bam: failed to write chunk end virtual offset: %v", err)
		}
	}
	return nil
}

func writeStats(w io.Writer, idxStats *IndexStats) error {
	var err error
	err = binary.Write(w, binary.LittleEndian, [2]uint32{statsDummyBin, 2})
	if err != nil {
		return fmt.Errorf("bam: failed to write stats bin header: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(idxStats.Chunk.Begin))
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats chunk begin virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(idxStats.Chunk.End))
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats chunk end virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, idxStats.Mapped)
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats mapped count: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, idxStats.Unmapped)
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats unmapped count: %v", err)
	}
	return nil
}

func writeIntervals(w io.Writer, offsets []bgzf.Offset) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(offsets)))
	if err != nil {
		return err
	}
	for _, o := range offsets {
		err := binary.Write(w, binary.LittleEndian, vOffset(o))
		if err != nil {
			return fmt.Errorf("bam: failed to write tile interval virtual offset: %v", err)
		}
	}
	return nil
}
