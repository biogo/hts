// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bytes"
	"code.google.com/p/biogo.bam/bgzf/egzip"
	"compress/gzip"
	"errors"
	"io"
	"io/ioutil"
	"os"
)

const (
	BlockSize    = 0x0ff00 // Size of input data block.
	MaxBlockSize = 0x10000 // Maximum size of output block.
)

const (
	bgzfExtra = "BC\x02\x00\x00\x00"
	minFrame  = 20 + len(bgzfExtra) // Minimum bgzf header+footer length.

	// Magic EOF block.
	magicBlock = "\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

var bgzfExtraPrefix = []byte(bgzfExtra[:4])

func compressBound(srcLen int) int {
	return srcLen + srcLen>>12 + srcLen>>14 + srcLen>>25 + 13 + minFrame
}

func init() {
	if compressBound(BlockSize) > MaxBlockSize {
		panic("bam: BlockSize too large")
	}
}

var (
	NewBlock         = egzip.NewBlock
	ErrClosed        = errors.New("bgzf: write to closed writer")
	ErrBlockOverflow = errors.New("bgzf: block overflow")
	ErrWrongFileType = errors.New("bgzf: file is a directory")
)

type Reader struct {
	gzip.Header
	gz  *egzip.Reader
	err error
}

func CheckEOF(f *os.File) (bool, error) {
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		return false, ErrWrongFileType
	}

	b := make([]byte, len(magicBlock))
	_, err = f.ReadAt(b, fi.Size()-int64(len(magicBlock)))
	if err != nil {
		return false, err
	}
	for i := range b {
		if b[i] != magicBlock[i] {
			return false, nil
		}
	}
	return true, nil
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

type Writer struct {
	gzip.Header
	level   int
	w       io.Writer
	gz      *egzip.Writer
	next    uint
	err     error
	written bool
	closed  bool
	block   [BlockSize]byte
	buf     *bytes.Buffer
}

func NewWriter(w io.Writer) *Writer {
	return NewWriterLevel(w, gzip.DefaultCompression)
}

func NewWriterLevel(w io.Writer, level int) *Writer {
	return &Writer{
		Header: gzip.Header{OS: 0xff},
		w:      w,
		level:  level,
		buf:    &bytes.Buffer{},
	}
}

func (bg *Writer) Next() int {
	return int(bg.next)
}

func (bg *Writer) Flush() error {
	if bg.err != nil {
		return bg.err
	}
	if bg.closed {
		return nil
	}
	if bg.written && bg.next == 0 {
		return nil
	}
	bg.written = true
	return bg.writeBlock()
}

func (bg *Writer) writeBlock() error {
	if bg.gz == nil {
		bg.gz, bg.err = egzip.NewWriterLevel(bg.buf, bg.level)
		if bg.err != nil {
			return bg.err
		}
	} else {
		bg.gz.Reset(bg.buf)
	}
	bg.gz.Header = gzip.Header{
		Comment: bg.Comment,
		Extra:   append([]byte(bgzfExtra), bg.Extra...),
		ModTime: bg.ModTime,
		Name:    bg.Name,
		OS:      bg.OS,
	}

	_, bg.err = bg.gz.Write(bg.block[:bg.next])
	if bg.err != nil {
		return bg.err
	}
	bg.err = bg.gz.Close()
	if bg.err != nil {
		return bg.err
	}
	bg.next = 0

	b := bg.buf.Bytes()
	i := bytes.Index(b, bgzfExtraPrefix)
	if i < 0 {
		return gzip.ErrHeader
	}
	size := len(b) - 1
	if size >= MaxBlockSize {
		bg.err = ErrBlockOverflow
		return bg.err
	}
	b[i+4], b[i+5] = byte(size), byte(size>>8)

	_, bg.err = io.Copy(bg.w, bg.buf)
	if bg.err != nil {
		return bg.err
	}
	bg.buf.Reset()

	return nil
}

func (bg *Writer) Close() error {
	if bg.err != nil {
		return bg.err
	}
	if bg.closed {
		return nil
	}
	bg.err = bg.writeBlock()
	if bg.err != nil {
		return bg.err
	}
	_, bg.err = bg.w.Write([]byte(magicBlock))
	if bg.err == nil {
		bg.closed = true
	}
	return bg.err
}

func (bg *Writer) Write(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}
	if bg.closed {
		return 0, ErrClosed
	}

	bg.written = false
	var n int
	for len(p) > 0 {
		if bg.next+uint(len(p)) > BlockSize {
			bg.err = bg.Flush()
			if bg.err != nil {
				return 0, bg.err
			}
		}
		c := copy(bg.block[bg.next:], p)
		n += c
		p = p[c:]
		bg.next += uint(c)
		if bg.next == BlockSize {
			bg.err = bg.Flush()
			if bg.err != nil {
				return n, bg.err
			}
		}
	}

	return n, bg.err
}
