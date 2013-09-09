// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"
	"compress/gzip"
	"io"
)

type Writer struct {
	bg  *bgzf.Writer
	h   *Header
	rec bamRecord
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

	err := h.writeTo(bw.bg)
	if err != nil {
		return nil, err
	}
	err = bw.bg.Flush()

	return bw, err
}

func (bw *Writer) Write(r *Record) error {
	_ = r.marshal(&bw.rec)
	bw.rec.writeTo(bw.bg)
	return nil
}

func (bw *Writer) Close() error {
	return bw.bg.Close()
}
