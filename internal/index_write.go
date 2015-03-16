// Copyright ©2014 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package internal

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/biogo/hts/bgzf"
)

// WriteIndex writes the Index to the given io.Writer.
func WriteIndex(w io.Writer, idx *Index, typ string) error {
	idx.sort()
	err := writeIndices(w, idx.Refs, typ)
	if err != nil {
		return err
	}
	if idx.Unmapped != nil {
		err = binary.Write(w, binary.LittleEndian, *idx.Unmapped)
	}
	return err
}

func writeIndices(w io.Writer, idx []RefIndex, typ string) error {
	for i := range idx {
		err := writeBins(w, idx[i].Bins, idx[i].Stats, typ)
		if err != nil {
			return err
		}
		err = writeIntervals(w, idx[i].Intervals, typ)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBins(w io.Writer, bins []Bin, stats *ReferenceStats, typ string) error {
	n := int32(len(bins))
	if stats != nil {
		n++
	}
	err := binary.Write(w, binary.LittleEndian, &n)
	if err != nil {
		return err
	}
	for _, b := range bins {
		err = binary.Write(w, binary.LittleEndian, b.Bin)
		if err != nil {
			return fmt.Errorf("%s: failed to write bin number: %v", typ, err)
		}
		err = writeChunks(w, b.Chunks, typ)
		if err != nil {
			return err
		}
	}
	if stats != nil {
		return writeStats(w, stats, typ)
	}
	return nil
}

func writeChunks(w io.Writer, chunks []bgzf.Chunk, typ string) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(chunks)))
	if err != nil {
		return fmt.Errorf("%s: failed to write bin count: %v", typ, err)
	}
	for _, c := range chunks {
		err = binary.Write(w, binary.LittleEndian, vOffset(c.Begin))
		if err != nil {
			return fmt.Errorf("%s: failed to write chunk begin virtual offset: %v", typ, err)
		}
		err = binary.Write(w, binary.LittleEndian, vOffset(c.End))
		if err != nil {
			return fmt.Errorf("%s: failed to write chunk end virtual offset: %v", typ, err)
		}
	}
	return nil
}

func writeStats(w io.Writer, stats *ReferenceStats, typ string) error {
	var err error
	err = binary.Write(w, binary.LittleEndian, [2]uint32{StatsDummyBin, 2})
	if err != nil {
		return fmt.Errorf("%s: failed to write stats bin header: %v", typ, err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.Begin))
	if err != nil {
		return fmt.Errorf("%s: failed to write index stats chunk begin virtual offset: %v", typ, err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.End))
	if err != nil {
		return fmt.Errorf("%s: failed to write index stats chunk end virtual offset: %v", typ, err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Mapped)
	if err != nil {
		return fmt.Errorf("%s: failed to write index stats mapped count: %v", typ, err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Unmapped)
	if err != nil {
		return fmt.Errorf("%s: failed to write index stats unmapped count: %v", typ, err)
	}
	return nil
}

func writeIntervals(w io.Writer, offsets []bgzf.Offset, typ string) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(offsets)))
	if err != nil {
		return err
	}
	for _, o := range offsets {
		err := binary.Write(w, binary.LittleEndian, vOffset(o))
		if err != nil {
			return fmt.Errorf("%s: failed to write tile interval virtual offset: %v", typ, err)
		}
	}
	return nil
}
