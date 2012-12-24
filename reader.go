// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"code.google.com/p/biogo.bam/bgzf"
	"io"
)

type Reader struct {
	r   *bgzf.Reader
	h   *Header
	rec bamRecord
}

func NewReader(r io.Reader, limited bool) (*Reader, error) {
	bg, err := bgzf.NewReader(r, limited)
	if err != nil {
		return nil, err
	}
	br := &Reader{
		r: bg,
		h: &Header{
			seenRefs:   set{},
			seenGroups: set{},
			seenProgs:  set{},
		},
	}
	err = br.h.read(br.r)
	if err != nil {
		return nil, err
	}
	return br, nil
}

func (br *Reader) Header() *Header {
	return br.h
}

func (br *Reader) Read() (*Record, error) {
	err := (&br.rec).readFrom(br.r)
	if err != nil {
		return nil, err
	}
	return br.rec.unmarshal(br.h), nil
}
