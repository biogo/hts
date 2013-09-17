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
	"unsafe"
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

	bw.bg.Write(bw.buf.Bytes())
	return bw.bg.Error()
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

	binary.Write(&wb, Endian, bamRecordFixed{
		BlockSize: int32(recLen),
		RefID:     int32(r.Ref.ID()),
		Pos:       int32(r.Pos),
		NLen:      byte(len(r.Name) + 1),
		MapQ:      r.MapQ,
		Bin:       reg2bin(r.Pos, r.End()), //r.bin,
		NCigar:    uint16(len(r.Cigar)),
		Flags:     r.Flags,
		LSeq:      int32(len(r.Qual)),
		NextRefID: int32(r.MateRef.ID()),
		NextPos:   int32(r.MatePos),
		TLen:      int32(r.TempLen),
	})
	wb.Write(append([]byte(r.Name), 0))
	writeCigarOps(&wb, r.Cigar)
	wb.Write(*(*[]byte)(unsafe.Pointer(&r.Seq.Seq)))
	wb.Write(r.Qual)
	wb.Write(tags)
	if wb.err != nil {
		return wb.err
	}

	bw.bg.Write(bw.buf.Bytes())
	return bw.bg.Error()
}

func writeCigarOps(w io.Writer, co []CigarOp) {
	var (
		back [4]byte
		buf  = back[:]
	)
	for _, o := range co {
		Endian.PutUint32(buf, uint32(o))
		_, err := w.Write(buf)
		if err != nil {
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
