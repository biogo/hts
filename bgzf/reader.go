// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
)

type Reader struct {
	Header
	rs  io.ReadSeeker
	gz  *gzip.Reader
	err error
}

func readSeeker(r io.Reader) io.ReadSeeker {
	rs, _ := r.(io.ReadSeeker)
	return rs
}

func NewReader(r io.Reader) (*Reader, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	h := Header(gz.Header)
	if h.BlockSize() < 0 {
		return nil, ErrNoBlockSize
	}
	bg := &Reader{
		Header: h,
		rs:     readSeeker(r),
		gz:     gz,
	}
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
}

func (bg *Reader) Seek(off Offset, whence int) error {
	if bg.rs == nil {
		return ErrNotASeeker
	}
	_, bg.err = bg.rs.Seek(off.File, whence)
	if bg.err != nil {
		return bg.err
	}
	bg.err = bg.gz.Reset(bg.rs)
	if bg.err != nil {
		return bg.err
	}
	bg.Header = Header(bg.gz.Header)
	if off.Block > 0 {
		_, bg.err = io.CopyN(ioutil.Discard, bg.gz, int64(off.Block))
	}
	return bg.err
}

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
