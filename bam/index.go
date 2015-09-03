// Copyright ©2014 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"encoding/binary"
	"errors"
	"io"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
	"github.com/biogo/hts/internal"
	"github.com/biogo/hts/sam"
)

// Index is a BAI index.
type Index struct {
	idx internal.Index
}

// NumRefs returns the number of references in the index.
func (i *Index) NumRefs() int {
	return len(i.idx.Refs)
}

// ReferenceStats returns the index statistics for the given reference and true
// if the statistics are valid.
func (i *Index) ReferenceStats(id int) (stats index.ReferenceStats, ok bool) {
	s := i.idx.Refs[id].Stats
	if s == nil {
		return index.ReferenceStats{}, false
	}
	return index.ReferenceStats(*s), true
}

// Unmapped returns the number of unmapped reads and true if the count is valid.
func (i *Index) Unmapped() (n uint64, ok bool) {
	if i.idx.Unmapped == nil {
		return 0, false
	}
	return *i.idx.Unmapped, true
}

// Add records the SAM record as having being located at the given chunk.
func (i *Index) Add(r *sam.Record, c bgzf.Chunk) error {
	return i.idx.Add(r, uint32(r.Bin()), c, isPlaced(r), isMapped(r))
}

func isPlaced(r *sam.Record) bool {
	return r.Ref != nil && r.Pos != -1
}

func isMapped(r *sam.Record) bool {
	return r.Flags&sam.Unmapped == 0
}

// Chunks returns a []bgzf.Chunk that corresponds to the given genomic interval.
func (i *Index) Chunks(r *sam.Reference, beg, end int) ([]bgzf.Chunk, error) {
	chunks, err := i.idx.Chunks(r.ID(), beg, end)
	if err != nil {
		return nil, err
	}
	return index.Adjacent(chunks), nil
}

// MergeChunks applies the given MergeStrategy to all bins in the Index.
func (i *Index) MergeChunks(s index.MergeStrategy) {
	i.idx.MergeChunks(s)
}

var baiMagic = [4]byte{'B', 'A', 'I', 0x1}

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

	var n int32
	err = binary.Read(r, binary.LittleEndian, &n)
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	idx.idx, err = internal.ReadIndex(r, n, "bam")
	if err != nil {
		return nil, err
	}
	return &idx, nil
}

// WriteIndex writes the Index to the given io.Writer.
func WriteIndex(w io.Writer, idx *Index) error {
	err := binary.Write(w, binary.LittleEndian, baiMagic)
	if err != nil {
		return err
	}

	err = binary.Write(w, binary.LittleEndian, int32(len(idx.idx.Refs)))
	if err != nil {
		return err
	}
	return internal.WriteIndex(w, &idx.idx, "bam")
}
