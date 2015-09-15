// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csi

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"
)

// WriteTo writes the CSI index to the given io.Writer. Note that
// the csi specification states that the index is stored as BGZF, but
// WriteTo does not perform compression.
func WriteTo(w io.Writer, idx *Index) error {
	idx.sort()
	err := binary.Write(w, binary.LittleEndian, csiMagic)
	if err != nil {
		return err
	}
	_, err = w.Write([]byte{idx.Version})
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, int32(idx.minShift))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, int32(idx.depth))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.LittleEndian, int32(len(idx.Auxilliary)))
	if err != nil {
		return err
	}
	_, err = w.Write(idx.Auxilliary)
	if err != nil {
		return err
	}
	err = writeIndices(w, idx.Version, idx.refs)
	if err != nil {
		return err
	}
	if idx.unmapped != nil {
		err = binary.Write(w, binary.LittleEndian, idx.unmapped)
	}
	return err
}

func writeIndices(w io.Writer, version byte, idx []refIndex) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(idx)))
	if err != nil {
		return err
	}
	for i := range idx {
		err = writeBins(w, version, idx[i].bins, idx[i].stats)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBins(w io.Writer, version byte, bins []bin, stats *index.ReferenceStats) error {
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
			return fmt.Errorf("csi: failed to write bin number: %v", err)
		}
		err = binary.Write(w, binary.LittleEndian, vOffset(b.left))
		if err != nil {
			return fmt.Errorf("csi: failed to write left virtual offset: %v", err)
		}
		if version == 0x2 {
			err = binary.Write(w, binary.LittleEndian, b.records)
			if err != nil {
				return fmt.Errorf("csi: failed to write record count: %v", err)
			}
		}
		err = writeChunks(w, b.chunks)
		if err != nil {
			return err
		}
	}
	if stats != nil {
		return writeStats(w, version, stats)
	}
	return nil
}

func writeChunks(w io.Writer, chunks []bgzf.Chunk) error {
	err := binary.Write(w, binary.LittleEndian, int32(len(chunks)))
	if err != nil {
		return fmt.Errorf("csi: failed to write bin count: %v", err)
	}
	for _, c := range chunks {
		err = binary.Write(w, binary.LittleEndian, vOffset(c.Begin))
		if err != nil {
			return fmt.Errorf("csi: failed to write chunk begin virtual offset: %v", err)
		}
		err = binary.Write(w, binary.LittleEndian, vOffset(c.End))
		if err != nil {
			return fmt.Errorf("csi: failed to write chunk end virtual offset: %v", err)
		}
	}
	return nil
}

func writeStats(w io.Writer, version byte, stats *index.ReferenceStats) error {
	var err error
	switch version {
	case 0x1:
		err = binary.Write(w, binary.LittleEndian, [4]uint32{statsDummyBin, 0, 0, 2})
	case 0x2:
		err = binary.Write(w, binary.LittleEndian, [6]uint32{statsDummyBin, 0, 0, 0, 0, 2})
	}
	if err != nil {
		return fmt.Errorf("csi: failed to write stats bin header: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.Begin))
	if err != nil {
		return fmt.Errorf("csi: failed to write index stats chunk begin virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, vOffset(stats.Chunk.End))
	if err != nil {
		return fmt.Errorf("csi: failed to write index stats chunk end virtual offset: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Mapped)
	if err != nil {
		return fmt.Errorf("csi: failed to write index stats mapped count: %v", err)
	}
	err = binary.Write(w, binary.LittleEndian, stats.Unmapped)
	if err != nil {
		return fmt.Errorf("csi: failed to write index stats unmapped count: %v", err)
	}
	return nil
}
