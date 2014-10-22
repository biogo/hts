// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bytes"
	"code.google.com/p/biogo.bam/bgzf/gzip"
	"io"
	"io/ioutil"
)

type Reader struct {
	Header
	r  io.Reader
	gz *gzip.Reader

	offset Offset

	err error
}

func NewReader(r io.Reader) (*Reader, error) {
	gz, err := gzip.NewReader(r)
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
		gz:     gz,
	}
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
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
	bg.err = bg.gz.Reset(rs)
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
		// FIXME(kortschak) Reading through members fails after a seek.
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}
			bs := bg.Header.BlockSize()
			bg.err = bg.gz.Reset(bg.r)
			if bs < 0 || bg.err != nil {
				bg.offset.File = -1
			} else {
				bg.offset.File += int64(bs)
			}
			bg.offset.Block = 0
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
