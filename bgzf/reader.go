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

// Reader implements BGZF reading as described in the SAM specification.
// The specification is available at https://github.com/samtools/hts-specs.
type Reader struct {
	gzip.Header
	r io.Reader

	// lastChunk is the virtual file offset
	// interval of the last successful read
	// or seek operation.
	lastChunk Chunk

	// nextBase is the file offset of the
	// block following the current block.
	nextBase int64

	block *blockReader

	Cache Cache

	err error
}

type blockReader struct {
	owner *Reader

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

// lazyBlock conditionally creates a ready to use Block and returns whether
// the Block subsequently held by the blockReader needs to be reset before
// being filled.
func (b *blockReader) lazyBlock() bool {
	if b.decompressed == nil {
		if w, ok := b.owner.Cache.(Wrapper); ok {
			b.decompressed = w.Wrap(&block{owner: b.owner})
		} else {
			b.decompressed = &block{owner: b.owner}
		}
		return false
	}
	if !b.decompressed.ownedBy(b.owner) {
		b.decompressed.setOwner(b.owner)
	}
	return true
}

func (b *blockReader) reset() (gzip.Header, error) {
	needReset := b.lazyBlock()

	if b.gotBlockFor(b.owner.nextBase) {
		return b.decompressed.header(), nil
	}

	if needReset && b.cr.n != b.owner.nextBase {
		// It should not be possible for the expected next block base
		// to be out of register with the count reader unless Seek
		// has been called, so we know the base reader must be an
		// io.ReadSeeker.
		err := b.seek(b.owner.r.(io.ReadSeeker), b.owner.nextBase)
		if err != nil {
			return b.decompressed.header(), err
		}
	}

	return b.fill(needReset)
}

func (b *blockReader) seekRead(r io.ReadSeeker, off int64) (gzip.Header, error) {
	b.lazyBlock()

	if off == b.decompressed.Base() && b.decompressed.hasData() {
		return b.decompressed.header(), nil
	}

	if b.gotBlockFor(off) {
		return b.decompressed.header(), nil
	}

	err := b.seek(r, off)
	if err != nil {
		return b.decompressed.header(), err
	}

	return b.fill(true)
}

func (b *blockReader) seek(r io.ReadSeeker, off int64) error {
	_, err := r.Seek(off, 0)
	if err != nil {
		return err
	}

	type reseter interface {
		Reset(io.Reader)
	}
	switch cr := b.cr.r.(type) {
	case reseter:
		cr.Reset(r)
	default:
		b.cr = makeReader(r)
	}
	b.cr.n = off

	return nil
}

// gotBlockFor returns true if the blockReader has access to a cache
// and that cache holds the block with given base and the correct
// owner, otherwise it returns false.
// gotBlockFor has side effects of recovering the block and putting
// the currently active block into the cache. If the cache returns
// a block owned by another reader, it is discarded.
func (b *blockReader) gotBlockFor(base int64) bool {
	if b.owner.Cache != nil {
		dec := b.decompressed
		if blk := b.owner.Cache.Get(base); blk != nil && blk.ownedBy(b.owner) {
			if dec != nil && dec.hasData() {
				// We have just retrieved a block from the cache
				// so we know that it must be below capacity and
				// a put will not cause an eviction.
				b.owner.Cache.Put(dec)
			}
			if blk.seek(0) == nil {
				b.decompressed = blk
				b.owner.nextBase = blk.nextBase()
				return true
			}
		}
		if dec != nil && dec.hasData() {
			dec, retained := b.owner.Cache.Put(dec)
			if retained {
				b.decompressed = dec
				b.lazyBlock()
			}
		}
	}

	return false
}

func (b *blockReader) fill(reset bool) (gzip.Header, error) {
	dec := b.decompressed

	if reset {
		dec.setBase(b.cr.n)

		err := b.gz.Reset(b.cr)
		if err == nil && expectedBlockSize(b.gz.Header) < 0 {
			err = ErrNoBlockSize
		}
		if err != nil {
			return b.gz.Header, err
		}
	}

	dec.setHeader(b.gz.Header)
	b.gz.Multistream(false)
	_, err := dec.readFrom(b.gz)
	b.owner.nextBase = dec.nextBase()
	return b.gz.Header, err
}

func expectedBlockSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}

// If a Cache is a Wrapper, its Wrap method is called on newly created blocks.
type Cache interface {
	// Get returns the Block in the Cache with the specified
	// base or a nil Block if it does not exist. The returned
	// Block must be removed from the Cache.
	Get(base int64) Block

	// Put inserts a Block into the Cache, returning the Block
	// that was evicted or nil if no eviction was necessary and
	// a boolean indicating whether the put Block was retained
	// by the Cache.
	Put(Block) (evicted Block, retained bool)
}

// Wrapper defines Cache types that need to modify a Block at its creation.
type Wrapper interface {
	Wrap(Block) Block
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

	// ownedBy returns whether the Block is owned by
	// the given Reader.
	ownedBy(*Reader) bool

	// setOwner changes the owner to the given Reader,
	// reseting other data to its zero state.
	setOwner(*Reader)

	// hasData returns whether the Block has read data.
	hasData() bool

	// The following are unexported equivalents
	// of the io interfaces. seek is limited to
	// the file origin offset case and does not
	// return the new offset.
	seek(offset int64) error
	readFrom(io.Reader) (int64, error)

	// len returns the number of remaining
	// bytes that can be read from the Block.
	len() int

	// setBase sets the file offset of the start
	// and of the gzip member that the Block data
	// was decompressed from.
	setBase(int64)

	// nextBase returns the expected position of the next
	// BGZF block. It returns -1 if the block is not valid.
	nextBase() int64

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
	owner *Reader

	base int64
	h    gzip.Header

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
	o := b.owner
	b.owner = nil
	buf := bytes.NewBuffer(b.data[:0])
	n, err := io.Copy(buf, r)
	if err != nil {
		return n, err
	}
	b.buf = bytes.NewReader(buf.Bytes())
	b.owner = o
	return n, nil
}

func (b *block) seek(offset int64) error {
	_, err := b.buf.Seek(offset, 0)
	if err == nil {
		b.chunk.Begin.Block = uint16(offset)
		b.chunk.End.Block = uint16(offset)
	}
	return err
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

func (b *block) nextBase() int64 {
	size := int64(expectedBlockSize(b.h))
	if size == -1 {
		return -1
	}
	return b.base + size
}

func (b *block) setHeader(h gzip.Header) { b.h = h }

func (b *block) header() gzip.Header { return b.h }

func (b *block) setOwner(r *Reader) {
	b.owner = r
	b.base = -1
	b.h = gzip.Header{}
	b.chunk = Chunk{}
	b.buf = nil
}

func (b *block) ownedBy(r *Reader) bool { return b.owner == r }

func (b *block) hasData() bool { return b.buf != nil }

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

// NewReader returns a new BGZF reader.
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
	b.owner = bg
	return bg, nil
}

// Offset is a BGZF virtual offset.
type Offset struct {
	File  int64
	Block uint16
}

// Chunk is a region of a BGZF file.
type Chunk struct {
	Begin Offset
	End   Offset
}

// Seek performs a seek operation to the given virtual offset.
func (bg *Reader) Seek(off Offset) error {
	rs, ok := bg.r.(io.ReadSeeker)
	if !ok {
		return ErrNotASeeker
	}

	var h gzip.Header
	h, bg.err = bg.block.seekRead(rs, off.File)
	if bg.err != nil {
		return bg.err
	}
	bg.Header = h

	bg.err = bg.block.decompressed.seek(int64(off.Block))
	if bg.err == nil {
		bg.lastChunk = Chunk{Begin: off, End: off}
	}

	return bg.err
}

// LastChunk returns the region of the BGZF file read by the last read
// operation or the resulting virtual offset of the last successful
// seek operation.
func (bg *Reader) LastChunk() Chunk { return bg.lastChunk }

// Close closes the reader and releases resources.
func (bg *Reader) Close() error {
	bg.Cache = nil
	return bg.block.gz.Close()
}

// Read implements the io.Reader interface.
func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}
	var h gzip.Header

	dec := bg.block.decompressed
	if dec != nil {
		dec.beginTx()
	}

	if dec == nil || dec.len() == 0 {
		h, bg.err = bg.block.reset()
		if bg.err != nil {
			return 0, bg.err
		}
		bg.Header = h
		dec = bg.block.decompressed
	}

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = dec.Read(p[n:])
		if _n > 0 {
			bg.lastChunk = dec.endTx()
		}
		n += _n
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}

			h, bg.err = bg.block.reset()
			if bg.err != nil {
				break
			}
			bg.Header = h
			dec = bg.block.decompressed
		}
	}

	return n, bg.err
}
