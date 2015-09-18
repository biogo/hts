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
		len(r.Qual) +
		len(tags)

	bw.buf.Reset()
	wb := errWriter{w: &bw.buf}
	bin := binaryWriter{w: &wb}

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
	wb.Write(append([]byte(r.Name), 0))
	writeCigarOps(&bin, r.Cigar)
	wb.Write(doublets(r.Seq.Seq).Bytes())
	if r.Qual != nil {
		wb.Write(r.Qual)
	} else {
		for i := 0; i < r.Seq.Length; i++ {
			wb.WriteByte(0xff)
		}
	}
	wb.Write(tags)
	if wb.err != nil {
		return wb.err
	}

	_, err := bw.bg.Write(bw.buf.Bytes())
	return err
}

func writeCigarOps(bin *binaryWriter, co []sam.CigarOp) {
	for _, o := range co {
		bin.writeUint32(uint32(o))
		if bin.w.err != nil {
			return
		}
	}
	return
}

// Close closes the writer.
func (bw *Writer) Close() error {
	return bw.bg.Close()
}

type errWriter struct {
	w   *bytes.Buffer
	err error
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	var n int
	n, w.err = w.w.Write(p)
	return n, w.err
}

func (w *errWriter) WriteByte(b byte) error {
	if w.err != nil {
		return w.err
	}
	w.err = w.w.WriteByte(b)
	return w.err
}

type binaryWriter struct {
	w   *errWriter
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
