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
	"sync"
)

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

// buffer is a flate.Reader used by a decompressor to store read-ahead data.
type buffer struct {
	// Buffered compressed data from read ahead.
	off  int // Current position in buffered data.
	size int // Total size of buffered data.
	data [MaxBlockSize]byte
}

// Read provides the flate.Decompressor Read method.
func (r *buffer) Read(b []byte) (int, error) {
	if r.off >= r.size {
		return 0, io.EOF
	}
	if n := r.size - r.off; len(b) > n {
		b = b[:n]
	}
	n := copy(b, r.data[r.off:])
	r.off += n
	return n, nil
}

// ReadByte provides the flate.Decompressor ReadByte method.
func (r *buffer) ReadByte() (byte, error) {
	if r.off == r.size {
		return 0, io.EOF
	}
	b := r.data[r.off]
	r.off++
	return b, nil
}

// reset makes the buffer available to store data.
func (r *buffer) reset() { r.size = 0 }

// hasData returns whether the buffer has any data buffered.
func (r *buffer) hasData() bool { return r.size != 0 }

// readLimited reads n bytes into the buffer from the given source.
func (r *buffer) readLimited(n int, src *countReader) error {
	if r.hasData() {
		panic("bgzf: read into non-empty buffer")
	}
	r.off = 0
	var err error
	r.size, err = io.ReadFull(src, r.data[:n])
	return err
}

// equals returns a boolean indicating the equality between
// the buffered data and the given byte slice.
func (r *buffer) equals(b []byte) bool { return bytes.Equal(r.data[:r.size], b) }

// decompressor is a gzip member decompressor worker.
type decompressor struct {
	owner *Reader

	gz gzip.Reader

	cr *countReader

	// Current block size.
	blockSize int

	// Buffered compressed data from read ahead.
	buf buffer

	// Decompressed data.
	wg  sync.WaitGroup
	blk Block

	err error
}

// Read provides the Read method for the decompressor's gzip.Reader.
func (d *decompressor) Read(b []byte) (int, error) {
	if d.buf.hasData() {
		return d.buf.Read(b)
	}
	return d.cr.Read(b)
}

// ReadByte provides the ReadByte method for the decompressor's gzip.Reader.
func (d *decompressor) ReadByte() (byte, error) {
	if d.buf.hasData() {
		return d.buf.ReadByte()
	}
	return d.cr.ReadByte()
}

// lazyBlock conditionally creates a ready to use Block.
func (d *decompressor) lazyBlock() {
	if d.blk == nil {
		if w, ok := d.owner.Cache.(Wrapper); ok {
			d.blk = w.Wrap(&block{owner: d.owner})
		} else {
			d.blk = &block{owner: d.owner}
		}
		return
	}
	if !d.blk.ownedBy(d.owner) {
		d.blk.setOwner(d.owner)
	}
}

// acquireHead gains the read head from the decompressor's owner.
func (d *decompressor) acquireHead() {
	d.wg.Add(1)
	d.cr = <-d.owner.head
}

// releaseHead releases the read head back to the decompressor's owner.
func (d *decompressor) releaseHead() {
	d.owner.head <- d.cr
	d.cr = nil // Defensively zero the reader.
}

// wait waits for the current member to be decompressed or fail, and returns
// the resulting error state.
func (d *decompressor) wait() (Block, error) {
	d.wg.Wait()
	blk := d.blk
	d.blk = nil
	return blk, d.err
}

func (d *decompressor) using(b Block) *decompressor { d.blk = b; return d }

// nextBlockAt makes the decompressor ready for reading decompressed data
// from its Block. It checks if there is a cached Block for the nextBase,
// otherwise it seeks to the correct location if decompressor is not
// correctly positioned, and then reads the compressed data and fills
// the decompressed Block.
// After nextBlockAt returns without error, the decompressor's Block
// holds a valid gzip.Header and base offset.
func (d *decompressor) nextBlockAt(off int64, rs io.ReadSeeker) *decompressor {
	d.err = nil
	for {
		exists, next := d.owner.cacheHasBlockFor(off)
		if !exists {
			break
		}
		off = next
	}

	d.lazyBlock()

	d.acquireHead()
	defer d.releaseHead()

	if d.cr.offset() != off {
		if rs == nil {
			// It should not be possible for the expected next block base
			// to be out of register with the count reader unless Seek
			// has been called, so we know the base reader must be an
			// io.ReadSeeker.
			var ok bool
			rs, ok = d.owner.r.(io.ReadSeeker)
			if !ok {
				panic("bgzf: unexpected offset without seek")
			}
		}
		d.err = d.cr.seek(rs, off)
		if d.err != nil {
			d.wg.Done()
			return d
		}
	}

	d.blk.setBase(d.cr.offset())
	var skipped int
	skipped, d.err = d.readMember()
	if d.err != nil {
		d.wg.Done()
		return d
	}
	if skipped < len(magicBlock) && d.buf.equals([]byte(magicBlock)[skipped:]) {
		// Special case for a magic block. This is done to preserve
		// gzip header contents if a client has used ioutil.ReadAll.
		// We need to copy over the extra data though to ensure that
		// Block.nextBase returns the correct value.
		h := d.blk.header()
		h.Extra = d.gz.Header.Extra
		d.blk.setHeader(h)
	} else {
		d.blk.setHeader(d.gz.Header)
	}

	// Decompress data into the decompressor's Block.
	go func() {
		d.err = d.blk.readFrom(&d.gz)
		d.wg.Done()
	}()

	return d
}

// expectedMemberSize returns the size of the BGZF conformant gzip member.
// It returns -1 if no BGZF block size field is found.
func expectedMemberSize(h gzip.Header) int {
	i := bytes.Index(h.Extra, bgzfExtraPrefix)
	if i < 0 || i+5 >= len(h.Extra) {
		return -1
	}
	return (int(h.Extra[i+4]) | int(h.Extra[i+5])<<8) + 1
}

// readMember buffers the gzip member starting the current decompressor offset,
// it returns the number of prefix bytes not read into the decompressor's buffer.
func (d *decompressor) readMember() (skipped int, err error) {
	// Set the decompressor to Read from the underlying flate.Reader
	// and mark the starting offset from which the underlying reader
	// was used.
	d.buf.reset()
	mark := d.cr.offset()

	err = d.gz.Reset(d)
	if err != nil {
		d.blockSize = -1
		return 0, err
	}

	d.blockSize = expectedMemberSize(d.gz.Header)
	if d.blockSize < 0 {
		return 0, ErrNoBlockSize
	}
	skipped = int(d.cr.offset() - mark)

	// Read compressed data into the decompressor buffer until the
	// underlying flate.Reader is positioned at the end of the gzip
	// member in which the readMember call was made.
	return skipped, d.buf.readLimited(d.blockSize-skipped, d.cr)
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

	dec *decompressor

	current Block

	cacheLock sync.RWMutex
	// Cache is the Reader block cache. If Cache is not nil,
	// the cache is queried for blocks before an attempt to
	// read from the underlying io.Reader.
	Cache Cache

	err error
}

// NewReader returns a new BGZF reader.
//
// The number of concurrent read decompressors is specified by
// rd (currently ignored).
func NewReader(r io.Reader, rd int) (*Reader, error) {
	bg := &Reader{
		r: r,

		head: make(chan *countReader, 1),
	}
	bg.head <- newCountReader(r)

	// Read the first block now so we can fail before
	// the first Read call if there is a problem.
	bg.dec = &decompressor{owner: bg}
	blk, err := bg.dec.nextBlockAt(0, nil).wait()
	if err != nil {
		return nil, err
	}
	bg.current = blk
	bg.Header = bg.current.header()

	return bg, nil
}

// Seek performs a seek operation to the given virtual offset.
func (bg *Reader) Seek(off Offset) error {
	rs, ok := bg.r.(io.ReadSeeker)
	if !ok {
		return ErrNotASeeker
	}

	if off.File != bg.current.Base() || !bg.current.hasData() {
		ok := bg.cacheSwap(off.File)
		if !ok {
			bg.current, bg.err = bg.dec.
				using(bg.current).
				nextBlockAt(off.File, rs).
				wait()
			bg.Header = bg.current.header()
			if bg.err != nil {
				return bg.err
			}
		}
	}

	bg.err = bg.current.seek(int64(off.Block))
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
	if bg.err == io.EOF {
		return nil
	}
	return bg.err
}

// Read implements the io.Reader interface.
func (bg *Reader) Read(p []byte) (int, error) {
	if bg.err != nil {
		return 0, bg.err
	}

	// Discard leading empty blocks. This is an indexing
	// optimisation to avoid retaining useless members
	// in a BAI/CSI.
	for bg.current.len() == 0 {
		bg.err = bg.nextBlock()
		bg.Header = bg.current.header()
		if bg.err != nil {
			return 0, bg.err
		}
	}

	bg.lastChunk.Begin = bg.current.txOffset()

	var n int
	for n < len(p) && bg.err == nil {
		var _n int
		_n, bg.err = bg.current.Read(p[n:])
		if _n > 0 {
			bg.lastChunk.End = bg.current.txOffset()
		}
		n += _n
		if bg.err == io.EOF {
			if n == len(p) {
				bg.err = nil
				break
			}

			bg.err = bg.nextBlock()
			bg.Header = bg.current.header()
			if bg.err != nil {
				break
			}
		}
	}

	return n, bg.err
}

func (bg *Reader) nextBlock() error {
	base := bg.current.NextBase()
	ok := bg.cacheSwap(base)
	if ok {
		return nil
	}
	var err error
	bg.current, err = bg.dec.
		using(bg.current).
		nextBlockAt(base, nil).
		wait()
	return err
}

// cacheSwap attempts to swap the current Block for a cached Block
// for the given base offset. It returns true if successful.
func (bg *Reader) cacheSwap(base int64) bool {
	if bg.Cache == nil {
		return false
	}
	bg.cacheLock.Lock()
	defer bg.cacheLock.Unlock()

	blk, err := bg.cachedBlockFor(base)
	if err != nil {
		return false
	}
	if blk != nil {
		// TODO(kortschak): Under some conditions, e.g. FIFO
		// cache we will be discarding a non-nil evicted Block.
		// Consider retaining these in a sync.Pool.
		bg.cachePut(bg.current)
		bg.current = blk
		return true
	}
	var retained bool
	bg.current, retained = bg.cachePut(bg.current)
	if retained {
		bg.current = nil
	}
	return false
}

// cacheHasBlockFor returns whether the Reader's cache has a block
// for the given base offset. If the requested Block exists, the base
// offset of the following Block is returned.
func (bg *Reader) cacheHasBlockFor(base int64) (exists bool, next int64) {
	if bg.Cache == nil {
		return false, -1
	}
	bg.cacheLock.RLock()
	exists, next = bg.Cache.Peek(base)
	bg.cacheLock.RUnlock()
	return exists, next
}

// cachedBlockFor returns a non-nil Block if the Reader has access to a
// cache and the cache holds the block with the given base and the
// correct owner, otherwise it returns nil. If the Block's owner is not
// correct, or the Block cannot seek to the start of its data, a non-nil
// error is returned.
func (bg *Reader) cachedBlockFor(base int64) (Block, error) {
	blk := bg.Cache.Get(base)
	if blk != nil {
		if !blk.ownedBy(bg) {
			return nil, ErrContaminatedCache
		}
		err := blk.seek(0)
		if err != nil {
			return nil, err
		}
	}
	return blk, nil
}

// cachePut puts the given Block into the cache if it exists, it returns
// the Block that was evicted or b if it was not retained, and whether
// the Block was retained by the cache.
func (bg *Reader) cachePut(b Block) (evicted Block, retained bool) {
	if b == nil || !b.hasData() {
		return b, false
	}
	return bg.Cache.Put(b)
}
