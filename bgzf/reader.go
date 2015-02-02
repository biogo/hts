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

// Reader implements BGZF blocked gzip decompression.
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

	active *decompressor

	// Cache is the Reader block cache. If Cache is not nil,
	// the cache is queried for blocks before an attempt to
	// read from the underlying io.Reader.
	Cache Cache

	err error
}

type decompressor struct {
	owner *Reader

	gz *gzip.Reader

	// Underlying Reader.
	r flate.Reader

	// Positions within underlying data stream
	offset int64 // Current offset in stream - possibly virtual.
	mark   int64 // Offset at start of useUnderlying.

	// Buffered compressed data from read ahead.
	i   int // Current position in buffered data.
	n   int // Total size of buffered data.
	buf [MaxBlockSize]byte

	// Decompressed data.
	decompressed Block

	err error
}

func makeReader(r io.Reader) flate.Reader {
	switch r := r.(type) {
	case *decompressor:
		panic("bgzf: illegal use of internal type")
	case flate.Reader:
		return r
	default:
		return bufio.NewReader(r)
	}
}

func newDecompressor(r io.Reader) (*decompressor, error) {
	d := &decompressor{r: makeReader(r)}
	gz, err := gzip.NewReader(d)
	if err != nil {
		return nil, err
	}
	bs := expectedBlockSize(gz.Header)
	if bs < 0 {
		return nil, ErrNoBlockSize
	}
	d.gz = gz
	return d, nil
}

// lazyBlock conditionally creates a ready to use Block and returns whether
// the Block subsequently held by the decompressor needs to be reset before
// being filled.
func (d *decompressor) lazyBlock() bool {
	if d.decompressed == nil {
		if w, ok := d.owner.Cache.(Wrapper); ok {
			d.decompressed = w.Wrap(&block{owner: d.owner})
		} else {
			d.decompressed = &block{owner: d.owner}
		}
		return false
	}
	if !d.decompressed.ownedBy(d.owner) {
		d.decompressed.setOwner(d.owner)
	}
	return true
}

func (d *decompressor) header() gzip.Header {
	return d.gz.Header
}

func (d *decompressor) isLimited() bool { return d.n != 0 }

// Read provides the Read method for the decompressor's gzip.Reader.
func (d *decompressor) Read(p []byte) (int, error) {
	var (
		n   int
		err error
	)
	if d.isLimited() {
		if d.i >= d.n {
			return 0, io.EOF
		}
		if n := d.n - d.i; len(p) > n {
			p = p[:n]
		}
		n = copy(p, d.buf[d.i:])
		d.i += n
	} else {
		n, err = d.r.Read(p)
	}
	d.offset += int64(n)
	return n, err
}

// ReadByte provides the ReadByte method for the decompressor's gzip.Reader.
func (d *decompressor) ReadByte() (byte, error) {
	var (
		b   byte
		err error
	)
	if d.isLimited() {
		if d.i == d.n {
			return 0, io.EOF
		}
		b = d.buf[d.i]
		d.i++
	} else {
		b, err = d.r.ReadByte()
	}
	if err == nil {
		d.offset++
	}
	return b, err
}

func (d *decompressor) reset() {
	needReset := d.lazyBlock()

	if d.gotBlockFor(d.owner.nextBase) {
		return
	}

	if needReset && d.offset != d.owner.nextBase {
		// It should not be possible for the expected next block base
		// to be out of register with the count reader unless Seek
		// has been called, so we know the base reader must be an
		// io.ReadSeeker.
		d.err = d.seek(d.owner.r.(io.ReadSeeker), d.owner.nextBase)
		if d.err != nil {
			return
		}
	}

	d.err = d.fill(needReset)
}

func (d *decompressor) seekRead(r io.ReadSeeker, off int64) {
	d.lazyBlock()

	if off == d.decompressed.Base() && d.decompressed.hasData() {
		return
	}

	if d.gotBlockFor(off) {
		return
	}

	d.err = d.seek(r, off)
	if d.err != nil {
		return
	}

	d.err = d.fill(true)
}

func (d *decompressor) seek(r io.ReadSeeker, off int64) error {
	_, err := r.Seek(off, 0)
	if err != nil {
		return err
	}

	type reseter interface {
		Reset(io.Reader)
	}
	switch cr := d.r.(type) {
	case reseter:
		cr.Reset(r)
	default:
		d.r = makeReader(r)
	}
	d.offset = off

	return nil
}

// gotBlockFor returns true if the decompressor has access to a cache
// and that cache holds the block with given base and the correct
// owner, otherwise it returns false.
// gotBlockFor has side effects of recovering the block and putting
// the currently active block into the cache. If the cache returns
// a block owned by another reader, it is discarded.
func (d *decompressor) gotBlockFor(base int64) bool {
	if d.owner.Cache != nil {
		dec := d.decompressed
		if blk := d.owner.Cache.Get(base); blk != nil && blk.ownedBy(d.owner) {
			if dec != nil && dec.hasData() {
				// TODO(kortschak): Under some conditions, e.g. FIFO
				// cache we will be discarding a non-nil evicted Block.
				// Consider retaining these in a sync.Pool.
				d.owner.Cache.Put(dec)
			}
			if d.err = blk.seek(0); d.err == nil {
				d.decompressed = blk
				d.owner.nextBase = blk.nextBase()
				return true
			}
		}
		if dec != nil && dec.hasData() {
			dec, retained := d.owner.Cache.Put(dec)
			if retained {
				d.decompressed = dec
				d.lazyBlock()
			}
		}
	}

	return false
}

func (d *decompressor) useUnderlying() { d.n = 0; d.mark = d.offset }

func (d *decompressor) readAhead(n int) error {
	d.i, d.n = 0, n
	var err error
	lr := io.LimitedReader{R: d.r, N: int64(n)}
	for i, _n := 0, 0; i < n && err == nil; i += _n {
		_n, err = lr.Read(d.buf[i:])
		if err != nil {
			break
		}
	}
	return err
}

func (d *decompressor) deltaOffset() int64 { return d.offset - d.mark }

func (d *decompressor) fill(reset bool) error {
	dec := d.decompressed

	if reset {
		dec.setBase(d.offset)

		d.useUnderlying()
		err := d.gz.Reset(d)
		bs := expectedBlockSize(d.gz.Header)
		if err == nil && bs < 0 {
			err = ErrNoBlockSize
		}
		if err != nil {
			return err
		}
		err = d.readAhead(bs - int(d.deltaOffset()))
		if err != nil {
			return err
		}
	}

	dec.setHeader(d.gz.Header)
	d.gz.Multistream(false)
	_, err := dec.readFrom(d.gz)
	return err
}

func expectedBlockSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}

// Cache is a Block caching type. Basic cache implementations are provided
// in the cache package.
//
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

// Block wraps interaction with decompressed BGZF data blocks.
type Block interface {
	// Base returns the file offset of the start of
	// the gzip member from which the Block data was
	// decompressed.
	Base() int64

	io.Reader

	// Used returns whether one or more bytes have
	// been read from the Block.
	Used() bool

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
	used  bool

	base int64
	h    gzip.Header

	chunk Chunk

	buf  *bytes.Reader
	data [MaxBlockSize]byte
}

func (b *block) Base() int64 { return b.base }

func (b *block) Used() bool { return b.used }

func (b *block) Read(p []byte) (int, error) {
	n, err := b.buf.Read(p)
	b.chunk.End.Block += uint16(n)
	if n > 0 {
		b.used = true
	}
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
	b.used = false
	b.base = -1
	b.h = gzip.Header{}
	b.chunk = Chunk{}
	b.buf = nil
}

func (b *block) ownedBy(r *Reader) bool { return b.owner == r }

func (b *block) hasData() bool { return b.buf != nil }

func (b *block) beginTx() { b.chunk.Begin = b.chunk.End }

func (b *block) endTx() Chunk { return b.chunk }

// NewReader returns a new BGZF reader.
//
// The number of concurrent read decompressors is specified by
// rd (currently ignored).
func NewReader(r io.Reader, rd int) (*Reader, error) {
	d, err := newDecompressor(r)
	if err != nil {
		return nil, err
	}
	bg := &Reader{
		Header: d.header(),
		r:      r,
		active: d,
	}
	d.owner = bg
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

	bg.active.seekRead(rs, off.File)
	bg.err = bg.active.err
	if bg.err != nil {
		return bg.err
	}
	bg.Header = bg.active.header()
	bg.nextBase = bg.active.decompressed.nextBase()

	bg.err = bg.active.decompressed.seek(int64(off.Block))
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
	return bg.active.gz.Close()
}

// Read implements the io.Reader interface.
func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}

	dec := bg.active.decompressed
	if dec != nil {
		dec.beginTx()
	} else {
		bs := expectedBlockSize(bg.Header)
		bg.err = bg.active.readAhead(bs - int(bg.active.deltaOffset()))
		if bg.err != nil {
			return 0, bg.err
		}
	}

	if dec == nil || dec.len() == 0 {
		dec, bg.err = bg.resetDecompressor()
		if bg.err != nil {
			return 0, bg.err
		}
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

			dec, bg.err = bg.resetDecompressor()
			if bg.err != nil {
				break
			}
		}
	}

	return n, bg.err
}

func (bg *Reader) resetDecompressor() (Block, error) {
	bg.active.reset()
	if bg.active.err != nil {
		return nil, bg.active.err
	}
	bg.Header = bg.active.header()
	bg.nextBase = bg.active.decompressed.nextBase()
	return bg.active.decompressed, nil
}
