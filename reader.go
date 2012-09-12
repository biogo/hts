// Copyright ©2012 Dan Kortschak <dan.kortschak@adelaide.edu.au>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
