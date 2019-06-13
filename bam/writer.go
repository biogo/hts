// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/sam"
)

// Writer implements BAM data writing.
type Writer struct {
	h *sam.Header

	bg  *bgzf.Writer
	buf bytes.Buffer
}

// NewWriter returns a new Writer using the given SAM header. Write
// concurrency is set to wc.
func NewWriter(w io.Writer, h *sam.Header, wc int) (*Writer, error) {
	return NewWriterLevel(w, h, gzip.DefaultCompression, wc)
}

func makeWriter(w io.Writer, level, wc int) (*bgzf.Writer, error) {
	if bw, ok := w.(*bgzf.Writer); ok {
		return bw, nil
	}
	return bgzf.NewWriterLevel(w, level, wc)
}

// NewWriterLevel returns a new Writer using the given SAM header. Write
// concurrency is set to wc and compression level is set to level. Valid
// values for level are described in the compress/gzip documentation.
func NewWriterLevel(w io.Writer, h *sam.Header, level, wc int) (*Writer, error) {
	bg, err := makeWriter(w, level, wc)
	if err != nil {
		return nil, err
	}
	bw := &Writer{
		bg: bg,
		h:  h,
	}

	err = bw.writeHeader(h)
	if err != nil {
		return nil, err
	}
	bw.bg.Flush()
	err = bw.bg.Wait()
	if err != nil {
		return nil, err
	}
	return bw, nil
}

func (bw *Writer) writeHeader(h *sam.Header) error {
	bw.buf.Reset()
	err := h.EncodeBinary(&bw.buf)
	if err != nil {
		return err
	}

	_, err = bw.bg.Write(bw.buf.Bytes())
	return err
}

// Write writes r to the BAM stream.
func (bw *Writer) Write(r *sam.Record) error {
	if len(r.Name) == 0 || len(r.Name) > 254 {
		return errors.New("bam: name absent or too long")
	}
	if r.Qual != nil && len(r.Qual) != r.Seq.Length {
		return errors.New("bam: sequence/quality length mismatch")
	}
	tags := buildAux(r.AuxFields)
	recLen := bamFixedRemainder +
		len(r.Name) + 1 + // Null terminated.
		len(r.Cigar)<<2 + // CigarOps are 4 bytes.
		len(r.Seq.Seq) +
		r.Seq.Length +
		len(tags)

	bw.buf.Reset()
	bin := binaryWriter{w: &bw.buf}

	// Write record header data.
	bin.writeInt32(int32(recLen))
	bin.writeInt32(int32(r.Ref.ID()))
	bin.writeInt32(int32(r.Pos))
	bin.writeUint8(byte(len(r.Name) + 1))
	bin.writeUint8(r.MapQ)
	bin.writeUint16(uint16(r.Bin())) //r.bin
	bin.writeUint16(uint16(len(r.Cigar)))
	bin.writeUint16(uint16(r.Flags))
	bin.writeInt32(int32(r.Seq.Length))
	bin.writeInt32(int32(r.MateRef.ID()))
	bin.writeInt32(int32(r.MatePos))
	bin.writeInt32(int32(r.TempLen))

	// Write variable length data.
	bw.buf.WriteString(r.Name)
	bw.buf.WriteByte(0)
	writeCigarOps(&bin, r.Cigar)
	bw.buf.Write(doublets(r.Seq.Seq).Bytes())
	if r.Qual != nil {
		bw.buf.Write(r.Qual)
	} else {
		for i := 0; i < r.Seq.Length; i++ {
			bw.buf.WriteByte(0xff)
		}
	}
	bw.buf.Write(tags)
	_, err := bw.bg.Write(bw.buf.Bytes())
	return err
}

func writeCigarOps(bin *binaryWriter, co []sam.CigarOp) {
	for _, o := range co {
		bin.writeUint32(uint32(o))
	}
}

// Close closes the writer.
func (bw *Writer) Close() error {
	return bw.bg.Close()
}

type binaryWriter struct {
	w   *bytes.Buffer
	buf [4]byte
}

func (w *binaryWriter) writeUint8(v uint8) {
	w.buf[0] = v
	w.w.Write(w.buf[:1])
}

func (w *binaryWriter) writeUint16(v uint16) {
	binary.LittleEndian.PutUint16(w.buf[:2], v)
	w.w.Write(w.buf[:2])
}

func (w *binaryWriter) writeInt32(v int32) {
	binary.LittleEndian.PutUint32(w.buf[:4], uint32(v))
	w.w.Write(w.buf[:4])
}

func (w *binaryWriter) writeUint32(v uint32) {
	binary.LittleEndian.PutUint32(w.buf[:4], v)
	w.w.Write(w.buf[:4])
}
