// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"code.google.com/p/biogo.bam/bgzf/egzip"

	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
)

type Reader struct {
	gzip.Header
	gz  *egzip.Reader
	err error
}

func NewReader(r io.Reader, limited bool) (*Reader, error) {
	bg := &Reader{}
	gz, err := egzip.NewReader(r, &bg.Header)
	if err != nil {
		return nil, err
	}
	gz.BlockLimited = limited
	bg.gz = gz
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
}

func (bg *Reader) Seek(off Offset, whence int) error {
	bg.err = bg.gz.Seek(off.File, whence)
	if bg.err != nil {
		return bg.err
	}
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
	n, err := bg.gz.Read(p)
	if n < len(p) && err == nil {
		var pn int
		pn, err = bg.Read(p[n:])
		n += pn
	}
	if n > 0 && err == io.EOF {
		err = nil
	}
	return n, err
}

func (bg *Reader) CurrBlockSize() (int, error) {
	if bg.err != nil {
		return -1, bg.err
	}
	i := bytes.Index(bg.Extra, bgzfExtraPrefix)
	if i+5 >= len(bg.Extra) {
		return -1, gzip.ErrHeader
	}
	return (int(bg.Extra[i+4]) | int(bg.Extra[i+5])<<8) + 1, nil
}
