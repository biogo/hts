// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
	"io/ioutil"
)

type Reader struct {
	gzip.Header
	r  io.Reader
	cr *countReader
	gz *gzip.Reader

	chunk Chunk

	err error
}

func makeReader(r io.Reader) *countReader {
	switch r := r.(type) {
	case *countReader:
		panic("bgzf: illegal use of internal type")
	case flate.Reader:
		return &countReader{r: r}
	default:
		return &countReader{r: bufio.NewReader(r)}
	}
}

type countReader struct {
	r flate.Reader
	n int64
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *countReader) ReadByte() (byte, error) {
	b, err := r.r.ReadByte()
	r.n++
	return b, err
}

func NewReader(r io.Reader) (*Reader, error) {
	cr := makeReader(r)
	gz, err := gzip.NewReader(cr)
	if err != nil {
		return nil, err
	}
	gz.Multistream(false)
	if ExpectedBlockSize(gz.Header) < 0 {
		return nil, ErrNoBlockSize
	}
	bg := &Reader{
		Header: gz.Header,
		r:      r,
		cr:     cr,
		gz:     gz,
	}
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
}

type Chunk struct {
	Begin Offset
	End   Offset
}

type reseter interface {
	Reset(io.Reader)
}

func (bg *Reader) Seek(off Offset, whence int) error {
	rs, ok := bg.r.(io.ReadSeeker)
	if !ok {
		return ErrNotASeeker
	}
	_, bg.err = rs.Seek(off.File, whence)
	if bg.err != nil {
		return bg.err
	}
	if r, ok := bg.cr.r.(reseter); ok {
		r.Reset(bg.r)
	} else {
		bg.cr = makeReader(bg.r)
	}
	bg.cr.n = off.File
	bg.err = bg.gz.Reset(bg.cr)
	if bg.err != nil {
		return bg.err
	}
	bg.gz.Multistream(false)
	bg.Header = bg.gz.Header
	bg.chunk.Begin = Offset{File: off.File, Block: 0}
	if off.Block > 0 {
		var n int64
		n, bg.err = io.CopyN(ioutil.Discard, bg.gz, int64(off.Block))
		bg.chunk.Begin.Block = uint16(n)
	}
	bg.chunk.End = bg.chunk.Begin
	return bg.err
}

func (bg *Reader) LastChunk() Chunk { return bg.chunk }

func (bg *Reader) Close() error {
	return bg.gz.Close()
}

func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}

	bg.chunk.Begin = bg.chunk.End

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = bg.gz.Read(p[n:])
		n += _n
		bg.chunk.Begin.Block = bg.chunk.End.Block
		bg.chunk.End.Block += uint16(_n)
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}
			if bs := ExpectedBlockSize(bg.Header); bs < 0 || bg.chunk.End.File < 0 {
				bg.chunk.End.File = -1
			} else {
				bg.chunk.End.File += int64(bs)
				bg.chunk.Begin.File = bg.cr.n
				if bg.chunk.End.File != bg.cr.n {
					bg.err = ErrBlockSizeMismatch
					break
				}
			}
			bg.chunk.End.Block = 0
			bg.err = bg.gz.Reset(bg.cr)
			bg.Header = bg.gz.Header
			bg.gz.Multistream(false)
		}
	}

	return n, bg.err
}

func ExpectedBlockSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}
