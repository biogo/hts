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

type Reader struct {
	gzip.Header
	gz  *egzip.Reader
	err error
}

func NewReader(r io.Reader) (*Reader, error) {
	bg := &Reader{}
	gz, err := egzip.NewReader(r, &bg.Header)
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

func (self *Reader) Seek(off Offset, whence int) error {
	self.err = self.gz.Seek(off.File, whence)
	if self.err != nil {
		return self.err
	}
	if off.Block > 0 {
		_, self.err = io.CopyN(ioutil.Discard, self.gz, int64(off.Block))
	}
	return self.err
}

func (self *Reader) Close() error {
	return self.gz.Close()
}

func (self *Reader) Read(p []byte) (int, error) {
	if self.err != nil {
		return 0, self.err
	}
	return self.gz.Read(p)
}

func (self *Reader) CurrBlockSize() (int, error) {
	if self.err != nil {
		return -1, self.err
	}
	i := bytes.Index(self.Extra, []byte("BC\x02"))
	if i+4 >= len(self.Extra) {
		return -1, gzip.ErrHeader
	}
	return int(self.Extra[i+3] | self.Extra[i+4]<<8), nil
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

func (self *Writer) Next() int {
	return int(self.next)
}

func (self *Writer) Flush() error {
	if self.err != nil {
		return self.err
	}
	if self.written && self.next == 0 {
		return nil
	}
	self.written = true
	var gz *gzip.Writer
	gz, self.err = gzip.NewWriterLevel(self.w, self.level)
	if self.err != nil {
		return self.err
	}
	gz.Header = gzip.Header{
		Comment: self.Comment,
		Extra:   append([]byte{'B', 'C', 0x2, byte(self.next), byte(self.next >> 8)}, self.Extra...),
		ModTime: self.ModTime,
		Name:    self.Name,
		OS:      self.OS,
	}
	_, self.err = gz.Write(self.buf[:self.next])
	if self.err != nil {
		return self.err
	}
	self.next = 0
	return gz.Close()
}

func (self *Writer) Close() error {
	if self.err != nil {
		return self.err
	}
	if self.closed {
		return nil
	}
	self.closed = true
	return self.Flush()
}

func (self *Writer) Write(p []byte) (int, error) {
	if self.err != nil {
		return 0, self.err
	}
	if self.closed {
		return len(p), nil
	}
	var n int
	for len(p) > 0 {
		c := copy(self.buf[self.next:], p)
		n += c
		p = p[c:]
		self.next += uint(c)
		if self.next == MaxBlockSize {
			return n, self.Flush()
		}
	}

	return n, self.err
}
