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
	"fmt"
	"io"
)

type Writer struct {
	w   *bgzf.Writer
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
		w: makeWriter(w, level),
		h: h,
	}

	err := h.writeTo(bw.w)
	if err != nil {
		return nil, err
	}

	return bw, nil
}

func (bw *Writer) Write(r *Record) error {
	l := r.marshal(&bw.rec)
	if l+bw.w.Next() > bgzf.MaxBlockSize { // This could be more intellgent.
		err := bw.w.Flush()
		fmt.Println("ERROR:", err)
		if err != nil {
			return err
		}
	}
	bw.rec.writeTo(bw.w)
	return nil
}

func (bw *Writer) Close() error {
	if bw.w.Next() != 0 {
		err := bw.w.Flush()
		fmt.Println("ERROR:", err)
		if err != nil {
			return err
		}
	}
	_, err := bw.w.Write(nil)
	if err != nil {
		return err
	}
	return bw.w.Close()
}
