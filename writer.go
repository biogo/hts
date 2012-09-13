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

type Writer struct {
	w   io.Writer
	bg  *bgzf.Writer
	h   *Header
	rec bamRecord
}

func makeWriter(w io.Writer, level int) *bgzf.Writer {
	if bw, ok := w.(*bgzf.Writer); ok {
		return bw
	}
	return bgzf.NewWriterLevel(w, level)
}

func NewWriter(w io.Writer, h *Header, level int) (*Writer, error) {
	bw := &Writer{
		w:  w,
		bg: makeWriter(w, level),
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
	err := bw.bg.Close()
	if err != nil {
		return err
	}
	_, err = bw.w.Write([]byte{ // Magic BAM block‽
		0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff, 0x06, 0x00, 0x42, 0x43, 0x02, 0x00,
		0x1b, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	})
	if err != nil {
		return err
	}
	return err
}
