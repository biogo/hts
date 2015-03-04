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

// WriteIndex writes the Index to the given io.Writer.
func WriteIndex(w io.Writer, idx *Index) error {
	idx.sort()
	err := binary.Write(w, binary.LittleEndian, baiMagic)
	if err != nil {
		return err
	}
	err = writeIndices(w, idx.refs)
	if err != nil {
		return err
	}
	if idx.unmapped != nil {
		err = binary.Write(w, binary.LittleEndian, idx.unmapped)
	}
	return err
}

func writeIndices(w io.Writer, idx []refIndex) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(idx)))
	if err != nil {
		return err
	}
	for i := range idx {
		err = writeBins(w, idx[i].bins, idx[i].stats)
		if err != nil {
			return err
		}
		err = writeIntervals(w, idx[i].intervals)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBins(w io.Writer, bins []bin, stats *ReferenceStats) error {
	n := int32(len(bins))
	if stats != nil {
		n++
	}
	err := binary.Write(w, binary.LittleEndian, &n)
	if err != nil {
		return err
	}
	for _, b := range bins {
		err = binary.Write(w, binary.LittleEndian, b.bin)
		if err != nil {
			return fmt.Errorf("bam: failed to write bin number: %v", err)
		}
		err = writeChunks(w, b.chunks)
		if err != nil {
			return err
		}
	}
	if stats != nil {
		return writeStats(w, stats)
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

func writeStats(w io.Writer, stats *ReferenceStats) error {
	var err error
	err = binary.Write(w, binary.LittleEndian, [2]uint32{statsDummyBin, 2})
	if err != nil {
		return fmt.Errorf("bam: failed to write stats bin header: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.Begin))
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats chunk begin virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.End))
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats chunk end virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Mapped)
	if err != nil {
		return fmt.Errorf("bam: failed to write index stats mapped count: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Unmapped)
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
