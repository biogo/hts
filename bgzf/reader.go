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

	// head serialises access to the underlying
	// io.Reader.
	head chan *countReader

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

	gz gzip.Reader

	cr   *countReader
	mark int64 // Offset at start of useUnderlying.

	// Current block size.
	blockSize int

	// Buffered compressed data from read ahead.
	i   int // Current position in buffered data.
	n   int // Total size of buffered data.
	buf [MaxBlockSize]byte

	// Decompressed data.
	decompressed Block

	err error
}

// countReader wraps flate.Reader, adding support for querying current offset.
type countReader struct {
	// Underlying Reader.
	fr flate.Reader

	// Offset within the underlying reader.
	off int64
}

// newCountReader returns a new countReader.
func newCountReader(r io.Reader) *countReader {
	switch r := r.(type) {
	case *countReader:
		panic("bgzf: illegal use of internal type")
	case flate.Reader:
		return &countReader{fr: r}
	default:
		return &countReader{fr: bufio.NewReader(r)}
	}
}

// Read is required to satisfy flate.Reader.
func (r *countReader) Read(p []byte) (int, error) {
	n, err := r.fr.Read(p)
	r.off += int64(n)
	return n, err
}

// ReadByte is required to satisfy flate.Reader.
func (r *countReader) ReadByte() (byte, error) {
	b, err := r.fr.ReadByte()
	if err == nil {
		r.off++
	}
	return b, err
}

// offset returns the current offset in the underlying reader.
func (r *countReader) offset() int64 { return r.off }

// seek moves the countReader to the specified offset using rs as the
// underlying reader.
func (r *countReader) seek(rs io.ReadSeeker, off int64) error {
	_, err := rs.Seek(off, 0)
	if err != nil {
		return err
	}

	type reseter interface {
		Reset(io.Reader)
	}
	switch cr := r.fr.(type) {
	case reseter:
		cr.Reset(rs)
	default:
		r.fr = newCountReader(rs)
	}
	r.off = off

	return nil
}

// init initialises a decompressor to use the provided flate.Reader.
func (d *decompressor) init(cr *countReader) error {
	defer d.releaseHead()

	d.cr = cr
	d.useUnderlying()
	err := d.gz.Reset(d)
	d.owner.Header = d.gz.Header
	if err != nil {
		d.blockSize = -1
		return err
	}
	d.blockSize = expectedBlockSize(d.gz.Header)
	if d.blockSize < 0 {
		return ErrNoBlockSize
	}

	return d.readAhead()
}

// readMember reads the block starting the current decompressor offset.
func (d *decompressor) readMember() error {
	return d.init(d.cr)
}

// acquireHead gains the read head from the decompressor's owner.
func (d *decompressor) acquireHead() {
	d.cr = <-d.owner.head
}

// releaseHead releases the read head back to the decompressor's owner.
func (d *decompressor) releaseHead() {
	d.owner.head <- d.cr
	d.cr = nil // Defensively zero the reader.
}

// lazyBlock conditionally creates a ready to use Block.
func (d *decompressor) lazyBlock() {
	if d.decompressed == nil {
		if w, ok := d.owner.Cache.(Wrapper); ok {
			d.decompressed = w.Wrap(&block{owner: d.owner})
		} else {
			d.decompressed = &block{owner: d.owner}
		}
		return
	}
	if !d.decompressed.ownedBy(d.owner) {
		d.decompressed.setOwner(d.owner)
	}
}

// isBuffered returns whether the decompressor has buffered compressed data.
func (d *decompressor) isBuffered() bool { return d.n != 0 }

// Read provides the Read method for the decompressor's gzip.Reader.
func (d *decompressor) Read(p []byte) (int, error) {
	var (
		n   int
		err error
	)
	if d.isBuffered() {
		if d.i >= d.n {
			return 0, io.EOF
		}
		if n := d.n - d.i; len(p) > n {
			p = p[:n]
		}
		n = copy(p, d.buf[d.i:])
		d.i += n
	} else {
		n, err = d.cr.Read(p)
	}
	return n, err
}

// ReadByte provides the ReadByte method for the decompressor's gzip.Reader.
func (d *decompressor) ReadByte() (byte, error) {
	var (
		b   byte
		err error
	)
	if d.isBuffered() {
		if d.i == d.n {
			return 0, io.EOF
		}
		b = d.buf[d.i]
		d.i++
	} else {
		b, err = d.cr.ReadByte()
	}
	return b, err
}

// reset makes the decompressor ready for reading decompressed data
// from its Block. It checks if there is a cached Block for the nextBase,
// otherwise it seeks to the correct location if decompressor is not
// correctly positioned, and then reads the compressed data and fills
// the decompressed Block.
func (d *decompressor) reset() {
	d.lazyBlock()

	if d.gotBlockFor(d.owner.nextBase) {
		return
	}

	d.acquireHead()
	if d.cr.offset() != d.owner.nextBase {
		// It should not be possible for the expected next block base
		// to be out of register with the count reader unless Seek
		// has been called, so we know the base reader must be an
		// io.ReadSeeker.
		d.err = d.cr.seek(d.owner.r.(io.ReadSeeker), d.owner.nextBase)
		if d.err != nil {
			d.releaseHead()
			return
		}
	}

	d.err = d.fill()
}

// seekRead is the seeking equivalent of reset. It checks if the seek
// is within the current Block and if not whether the seeked Block is
// cached, returning successfully if either is true. Otherwise it seeks
// to the offset and fills the decompressed Block.
func (d *decompressor) seekRead(r io.ReadSeeker, off int64) {
	d.lazyBlock()

	if off == d.decompressed.Base() && d.decompressed.hasData() {
		return
	}

	if d.gotBlockFor(off) {
		return
	}

	d.acquireHead()
	d.err = d.cr.seek(r, off)
	if d.err != nil {
		d.releaseHead()
		return
	}

	d.err = d.fill()
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

// useUnderlying set the decompressor to Read from the underlying flate.Reader.
// It marks the offset at from where the underlying reader has been used.
func (d *decompressor) useUnderlying() { d.n = 0; d.mark = d.cr.offset() }

// readAhead reads compressed data into the decompressor buffer. It reads until
// the underlying flate.Reader is positioned at the end of the gzip member in
// which the readAhead call was made. readAhead should not be called unless the
// decompressor has had readMember or init called successfully.
func (d *decompressor) readAhead() error {
	d.i = 0
	var err error
	d.n, err = io.ReadFull(d.cr, d.buf[:d.blockSize-d.deltaOffset()])
	return err
}

// deltaOffset returns the number of bytes read since the last call to
// useUnderlying.
func (d *decompressor) deltaOffset() int { return int(d.cr.offset() - d.mark) }

// fill decompresses data into the decompressor's Block.
func (d *decompressor) fill() error {
	dec := d.decompressed

	dec.setBase(d.cr.offset())
	err := d.readMember()
	if err != nil {
		return err
	}

	dec.setHeader(d.gz.Header)
	return dec.readFrom(&d.gz)
}

// expectedBlock size returns the size of the BGZF conformant gzip member.
// It returns -1 if no BGZF block size field is found.
func expectedBlockSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}

// NewReader returns a new BGZF reader.
//
// The number of concurrent read decompressors is specified by
// rd (currently ignored).
func NewReader(r io.Reader, rd int) (*Reader, error) {
	bg := &Reader{
		r:      r,
		active: &decompressor{},
		head:   make(chan *countReader, 1),
	}
	bg.active.owner = bg
	err := bg.active.init(newCountReader(r))
	if err != nil {
		return nil, err
	}

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

	if dec == nil {
		bg.active.lazyBlock()
		dec = bg.active.decompressed
		bg.err = dec.readFrom(&bg.active.gz)
		if bg.err != nil {
			return 0, bg.err
		}
		dec.setHeader(bg.active.gz.Header)
		bg.nextBase = dec.nextBase()
	}

	for dec.len() == 0 {
		dec, bg.err = bg.resetDecompressor()
		if bg.err != nil {
			return 0, bg.err
		}
	}

	bg.lastChunk.Begin = dec.txOffset()

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = dec.Read(p[n:])
		if _n > 0 {
			bg.lastChunk.End = dec.txOffset()
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
	bg.nextBase = bg.active.decompressed.nextBase()
	return bg.active.decompressed, nil
}
