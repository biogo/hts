// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"code.google.com/p/biogo.bam/bgzf"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
)

type Writer struct {
	h *Header

	bg  *bgzf.Writer
	buf bytes.Buffer
}

func NewWriter(w io.Writer, h *Header, wc int) (*Writer, error) {
	return NewWriterLevel(w, h, gzip.DefaultCompression, wc)
}

func makeWriter(w io.Writer, level, wc int) *bgzf.Writer {
	if bw, ok := w.(*bgzf.Writer); ok {
		return bw
	}
	return bgzf.NewWriterLevel(w, level, wc)
}

func NewWriterLevel(w io.Writer, h *Header, level, wc int) (*Writer, error) {
	bw := &Writer{
		bg: makeWriter(w, level, wc),
		h:  h,
	}

	err := bw.writeHeader(h)
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

func (bw *Writer) writeHeader(h *Header) error {
	// bw.buf.Reset() must be called before this if writeHeader is called from anywhere
	// other than NewWriterLevel.
	wb := &errWriter{w: &bw.buf}

	binary.Write(wb, Endian, bamMagic)
	text := h.Bytes()
	binary.Write(wb, Endian, int32(len(text)))
	wb.Write(text)
	binary.Write(wb, Endian, int32(len(h.refs)))

	if !validInt32(len(h.refs)) {
		return errors.New("bam: value out of range")
	}
	var name []byte
	for _, r := range h.refs {
		name = append(name, []byte(r.name)...)
		name = append(name, 0)
		binary.Write(wb, Endian, int32(len(name)))
		wb.Write(name)
		name = name[:0]
		binary.Write(wb, Endian, r.lRef)
	}
	if wb.err != nil {
		return wb.err
	}

	_, err := bw.bg.Write(bw.buf.Bytes())
	return err
}

func (bw *Writer) Write(r *Record) error {
	tags := buildAux(r.AuxTags)
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
	bin.writeUint16(reg2bin(r.Pos, r.End())) //r.bin
	bin.writeUint16(uint16(len(r.Cigar)))
	bin.writeUint16(uint16(r.Flags))
	bin.writeInt32(int32(len(r.Qual)))
	bin.writeInt32(int32(r.MateRef.ID()))
	bin.writeInt32(int32(r.MatePos))
	bin.writeInt32(int32(r.TempLen))

	// Write variable length data.
	wb.Write(append([]byte(r.Name), 0))
	writeCigarOps(&bin, r.Cigar)
	wb.Write(nybblePairs(r.Seq.Seq).Bytes())
	wb.Write(r.Qual)
	wb.Write(tags)
	if wb.err != nil {
		return wb.err
	}

	_, err := bw.bg.Write(bw.buf.Bytes())
	return err
}

func writeCigarOps(bin *binaryWriter, co []CigarOp) {
	for _, o := range co {
		bin.writeUint32(uint32(o))
		if bin.w.err != nil {
			return
		}
	}
	return
}

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

type binaryWriter struct {
	w   *errWriter
	buf [4]byte
}

func (w *binaryWriter) writeUint8(v uint8) {
	w.buf[0] = v
	w.w.Write(w.buf[:1])
}

func (w *binaryWriter) writeUint16(v uint16) {
	Endian.PutUint16(w.buf[:2], v)
	w.w.Write(w.buf[:2])
}

func (w *binaryWriter) writeInt32(v int32) {
	Endian.PutUint32(w.buf[:4], uint32(v))
	w.w.Write(w.buf[:4])
}

func (w *binaryWriter) writeUint32(v uint32) {
	Endian.PutUint32(w.buf[:4], v)
	w.w.Write(w.buf[:4])
}
