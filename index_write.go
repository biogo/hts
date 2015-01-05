// Copyright ©2014 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"

	"encoding/binary"
	"fmt"
	"io"
)

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

func writeChunks(w io.Writer, chunks []bgzf.Chunk) error {
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
