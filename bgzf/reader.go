// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bufio"
	"bytes"
	"code.google.com/p/biogo.bam/bgzf/flate"
	"code.google.com/p/biogo.bam/bgzf/gzip"
	"io"
	"io/ioutil"
)

type Reader struct {
	Header
	r  io.Reader
	cr *countReader
	gz *gzip.Reader

	offset Offset

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
	h := Header(gz.Header)
	if h.BlockSize() < 0 {
		return nil, ErrNoBlockSize
	}
	bg := &Reader{
		Header: h,
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
	bg.Header = Header(bg.gz.Header)
	bg.offset = Offset{File: off.File, Block: 0}
	if off.Block > 0 {
		var n int64
		n, bg.err = io.CopyN(ioutil.Discard, bg.gz, int64(off.Block))
		bg.offset.Block = uint16(n)
	}
	return bg.err
}

func (bg *Reader) Offset() Offset { return bg.offset }

func (bg *Reader) Close() error {
	return bg.gz.Close()
}

func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = bg.gz.Read(p[n:])
		n += _n
		bg.offset.Block += uint16(_n)
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}
			if bs := bg.Header.BlockSize(); bs < 0 || bg.offset.File < 0 {
				bg.offset.File = -1
			} else {
				bg.offset.File += int64(bs)
				if bg.offset.File != bg.cr.n {
					bg.err = ErrBlockSizeMismatch
					break
				}
			}
			bg.offset.Block = 0
			bg.err = bg.gz.Reset(bg.cr)
			bg.Header = Header(bg.gz.Header)
			bg.gz.Multistream(false)
		}
	}

	return n, bg.err
}

type Header gzip.Header

func (h Header) BlockSize() int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}
