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

package bgzf

import (
	"bytes"
	"code.google.com/p/biogo.bam/bgzf/egzip"
	"compress/gzip"
	"io"
	"io/ioutil"
)

const MaxBlockSize = 0x10000

var ErrNewBlock = egzip.ErrNewBlock

type Reader struct {
	gzip.Header
	gz  *egzip.Reader
	err error
}

func NewReader(r io.Reader, limited bool) (*Reader, error) {
	bg := &Reader{}
	gz, err := egzip.NewReader(r, &bg.Header)
	gz.BlockLimited = limited
	if err != nil {
		return nil, err
	}
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
	return bg.gz.Read(p)
}

func (bg *Reader) CurrBlockSize() (int, error) {
	if bg.err != nil {
		return -1, bg.err
	}
	i := bytes.Index(bg.Extra, []byte("BC\x02\x00"))
	if i+4 >= len(bg.Extra) {
		return -1, gzip.ErrHeader
	}
	return int(bg.Extra[i+4] | bg.Extra[i+5]<<8), nil
}

type Writer struct {
	gzip.Header
	level   int
	w       io.Writer
	next    uint
	err     error
	written bool
	closed  bool
	buf     [MaxBlockSize]byte
}

func NewWriter(w io.Writer) *Writer {
	return NewWriterLevel(w, gzip.DefaultCompression)
}

func NewWriterLevel(w io.Writer, level int) *Writer {
	return &Writer{
		w:     w,
		level: level,
	}
}

func (bg *Writer) Next() int {
	return int(bg.next)
}

func (bg *Writer) Flush() error {
	if bg.err != nil {
		return bg.err
	}
	if bg.written && bg.next == 0 {
		return nil
	}
	bg.written = true
	var gz *egzip.Writer
	gz, bg.err = egzip.NewWriterLevel(bg.w, bg.level)
	if bg.err != nil {
		return bg.err
	}
	gz.Header = gzip.Header{
		Comment: bg.Comment,
		Extra:   append([]byte{'B', 'C', 0x2, 0x0, byte(bg.next), byte(bg.next >> 8)}, bg.Extra...),
		ModTime: bg.ModTime,
		Name:    bg.Name,
		OS:      bg.OS,
	}
	_, bg.err = gz.Write(bg.buf[:bg.next])
	if bg.err != nil {
		return bg.err
	}
	bg.next = 0
	return gz.Close()
}

func (bg *Writer) Close() error {
	if bg.err != nil {
		return bg.err
	}
	if bg.closed {
		return nil
	}
	bg.closed = true
	return bg.Flush()
}

func (bg *Writer) Write(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}
	if bg.closed {
		return len(p), nil
	}

	bg.written = false
	var n int
	for len(p) > 0 {
		c := copy(bg.buf[bg.next:], p)
		n += c
		p = p[c:]
		bg.next += uint(c)
		if bg.next == MaxBlockSize {
			return n, bg.Flush()
		}
	}

	return n, bg.err
}
