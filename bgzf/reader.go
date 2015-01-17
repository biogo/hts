// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bufio"
	"bytes"
	"compress/flate"
	"compress/gzip"
	"io"
)

type Reader struct {
	gzip.Header
	r io.Reader

	chunk Chunk

	block *blockReader

	err error
}

type blockReader struct {
	cr *countReader
	gz *gzip.Reader

	decompressed Block
}

func newBlockReader(r io.Reader) (*blockReader, error) {
	cr := makeReader(r)
	gz, err := gzip.NewReader(cr)
	if err != nil {
		return nil, err
	}
	if expectedBlockSize(gz.Header) < 0 {
		return nil, ErrNoBlockSize
	}
	return &blockReader{cr: cr, gz: gz}, nil
}

func (b *blockReader) header() gzip.Header {
	return b.gz.Header
}

func (b *blockReader) reset(r io.Reader, off int64) (gzip.Header, error) {
	isNewBlock := b.decompressed == nil
	if isNewBlock {
		b.decompressed = &block{}
	}

	if r != nil {
		switch cr := b.cr.r.(type) {
		case reseter:
			cr.Reset(r)
		default:
			b.cr = makeReader(r)
		}
		b.cr.n = off
		b.decompressed.setBase(off)
	}

	if isNewBlock {
		b.decompressed.setHeader(b.gz.Header)
		return b.gz.Header, b.fill()
	}

	b.decompressed.setBase(b.cr.n)

	err := b.gz.Reset(b.cr)
	if err == nil && expectedBlockSize(b.gz.Header) < 0 {
		err = ErrNoBlockSize
	}
	if err != nil {
		return b.gz.Header, err
	}

	b.decompressed.setHeader(b.gz.Header)
	return b.gz.Header, b.fill()
}

func (b *blockReader) fill() error {
	b.gz.Multistream(false)
	_, err := b.decompressed.readFrom(b.gz)
	return err
}

type Block interface {
	// Base returns the file offset of the start of
	// the gzip member from which the Block data was
	// decompressed.
	Base() int64

	io.Reader

	// header returns the gzip.Header of the gzip member
	// from which the Block data was decompressed.
	header() gzip.Header

	// isValid protects the Reader from a cache that provides
	// a Block that has not been filled with data.
	isValid() bool

	// The following are unexported equivalents
	// of the io interfaces.
	seek(offset int64, whence int) (int64, error)
	readFrom(io.Reader) (int64, error)

	// len returns the number of remaining
	// bytes that can be read from the Block.
	len() int

	// setBase sets the file offset of the start
	// and of the gzip member that the Block data
	// was decompressed from.
	setBase(int64)

	// setHeader sets the file header of of the gzip
	// member that the Block data was decompressed from.
	setHeader(gzip.Header)

	// beginTx marks the chunk beginning for a set
	// of reads.
	beginTx()

	// endTx returns the Chunk describing the chunk
	// the block read by a set of reads.
	endTx() Chunk
}

type block struct {
	base  int64
	h     gzip.Header
	valid bool

	chunk Chunk

	buf  *bytes.Reader
	data [MaxBlockSize]byte
}

func (b *block) Base() int64 { return b.base }

func (b *block) Read(p []byte) (int, error) {
	n, err := b.buf.Read(p)
	b.chunk.End.Block += uint16(n)
	return n, err
}

func (b *block) readFrom(r io.Reader) (int64, error) {
	b.valid = false
	buf := bytes.NewBuffer(b.data[:0])
	n, err := io.Copy(buf, r)
	if err != nil {
		return n, err
	}
	b.buf = bytes.NewReader(buf.Bytes())
	b.valid = true
	return n, nil
}

func (b *block) seek(offset int64, whence int) (int64, error) {
	var err error
	b.base, err = b.buf.Seek(offset, whence)
	if err == nil {
		b.chunk.Begin.Block = uint16(offset)
		b.chunk.End.Block = uint16(offset)
	}
	return b.base, err
}

func (b *block) len() int {
	if b.buf == nil {
		return 0
	}
	return b.buf.Len()
}

func (b *block) setBase(n int64) {
	b.base = n
	b.chunk = Chunk{Begin: Offset{File: n}, End: Offset{File: n}}
}

func (b *block) setHeader(h gzip.Header) { b.h = h }

func (b *block) header() gzip.Header { return b.h }

func (b *block) isValid() bool { return b.valid }

func (b *block) beginTx() { b.chunk.Begin = b.chunk.End }

func (b *block) endTx() Chunk { return b.chunk }

func makeReader(r io.Reader) *countReader {
	switch r := r.(type) {
	case *countReader:
		panic("bgzf: illegal use of internal type")
	case flate.Reader:
		return &countReader{r: r}
	default:
		return &countReader{r: bufio.NewReader(r)}
	}
}

type countReader struct {
	r flate.Reader
	n int64
}

func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *countReader) ReadByte() (byte, error) {
	b, err := r.r.ReadByte()
	r.n++
	return b, err
}

func NewReader(r io.Reader) (*Reader, error) {
	b, err := newBlockReader(r)
	if err != nil {
		return nil, err
	}
	bg := &Reader{
		Header: b.header(),
		r:      r,
		block:  b,
	}
	return bg, nil
}

type Offset struct {
	File  int64
	Block uint16
}

type Chunk struct {
	Begin Offset
	End   Offset
}

type reseter interface {
	Reset(io.Reader)
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
	var h gzip.Header
	h, bg.err = bg.block.reset(bg.r, off.File)
	if bg.err != nil {
		return bg.err
	}
	bg.Header = h

	if off.Block > 0 {
		_, bg.err = bg.block.decompressed.seek(int64(off.Block), 0)
	}

	return bg.err
}

func (bg *Reader) LastChunk() Chunk { return bg.chunk }

func (bg *Reader) Close() error {
	return bg.block.gz.Close()
}

func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}
	var h gzip.Header

	if bg.block.decompressed != nil {
		bg.block.decompressed.beginTx()
	}

	if bg.block.decompressed == nil || bg.block.decompressed.len() == 0 {
		h, bg.err = bg.block.reset(nil, 0)
		if bg.err != nil {
			return 0, bg.err
		}
		bg.Header = h
	}

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = bg.block.decompressed.Read(p[n:])
		if _n > 0 {
			bg.chunk = bg.block.decompressed.endTx()
		}
		n += _n
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}

			h, bg.err = bg.block.reset(nil, 0)
			if bg.err != nil {
				break
			}
			bg.Header = h
		}
	}

	return n, bg.err
}

func expectedBlockSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}
