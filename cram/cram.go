// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cram is a WIP CRAM reader implementation.
//
// Currently the package implements container, block and slice retrieval, and
// SAM header values can be retrieved from blocks.
//
// See https://samtools.github.io/hts-specs/CRAMv3.pdf for the CRAM
// specification.
package cram

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"reflect"

	"github.com/ulikunitz/xz/lzma"

	"github.com/biogo/hts/cram/encoding/itf8"
	"github.com/biogo/hts/cram/encoding/ltf8"
	"github.com/biogo/hts/sam"
)

// cramEOFmarker is the CRAM end of file marker.
//
// See CRAM spec section 9.
var cramEOFmarker = []byte{
	0x0f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |........|
	0x0f, 0xe0, 0x45, 0x4f, 0x46, 0x00, 0x00, 0x00, // |..EOF...|
	0x00, 0x01, 0x00, 0x05, 0xbd, 0xd9, 0x4f, 0x00, // |......O.|
	0x01, 0x00, 0x06, 0x06, 0x01, 0x00, 0x01, 0x00, // |........|
	0x01, 0x00, 0xee, 0x63, 0x01, 0x4b, /*       */ // |...c.K|
}

// ErrNoEnd is returned when a stream cannot seek to a CRAM EOF block.
var ErrNoEnd = errors.New("cram: cannot determine offset from end")

// HasEOF checks for the presence of a CRAM magic EOF block.
// The magic block is defined in the CRAM specification. A magic block
// is written by a Writer on calling Close. The ReaderAt must provide
// some method for determining valid ReadAt offsets.
func HasEOF(r io.ReaderAt) (bool, error) {
	type sizer interface {
		Size() int64
	}
	type stater interface {
		Stat() (os.FileInfo, error)
	}
	type lenSeeker interface {
		io.Seeker
		Len() int
	}
	var size int64
	switch r := r.(type) {
	case sizer:
		size = r.Size()
	case stater:
		fi, err := r.Stat()
		if err != nil {
			return false, err
		}
		size = fi.Size()
	case lenSeeker:
		var err error
		size, err = r.Seek(0, 1)
		if err != nil {
			return false, err
		}
		size += int64(r.Len())
	default:
		return false, ErrNoEnd
	}

	b := make([]byte, len(cramEOFmarker))
	_, err := r.ReadAt(b, size-int64(len(cramEOFmarker)))
	if err != nil {
		return false, err
	}
	return bytes.Equal(b, cramEOFmarker), nil
}

// Reader is a CRAM format reader.
type Reader struct {
	r io.Reader

	d definition
	c *Container

	err error
}

// NewReader returns a new Reader.
func NewReader(r io.Reader) (*Reader, error) {
	cr := Reader{r: r}
	err := cr.d.readFrom(r)
	if err != nil {
		return nil, err
	}
	return &cr, nil
}

// Next advances the Reader to the next CRAM container. It returns false
// when the stream ends, either by reaching the end of the stream or
// encountering an error.
func (r *Reader) Next() bool {
	if r.err != nil {
		return false
	}
	if r.c != nil {
		io.Copy(io.Discard, r.c.blockData)
	}
	var c Container
	r.err = c.readFrom(r.r)
	r.c = &c
	return r.err == nil
}

// Container returns the current CRAM container. The returned Container
// is only valid after a previous call to Next has returned true.
func (r *Reader) Container() *Container {
	return r.c
}

// Err returns the most recent error.
func (r *Reader) Err() error {
	if errors.Is(r.err, io.EOF) {
		return nil
	}
	return r.err
}

// definition is a CRAM file definition.
//
// See CRAM spec section 6.
type definition struct {
	Magic   [4]byte `is:"CRAM"`
	Version [2]byte
	ID      [20]byte
}

// readFrom populates a definition from the given io.Reader. If the magic
// number of the file is not "CRAM" readFrom returns an error.
func (d *definition) readFrom(r io.Reader) error {
	err := binary.Read(r, binary.LittleEndian, d)
	if err != nil {
		return err
	}
	magic := reflect.TypeOf(*d).Field(0).Tag.Get("is")
	if !bytes.Equal(d.Magic[:], []byte(magic)) {
		return fmt.Errorf("cram: not a cram file: magic bytes %q", d.Magic)
	}
	return nil
}

// Container is a CRAM container.
//
// See CRAM spec section 7.
type Container struct {
	blockLen  int32
	refID     int32
	start     int32
	span      int32
	nRec      int32
	recCount  int64
	bases     int64
	blocks    int32
	landmarks []int32
	crc32     uint32
	blockData io.Reader

	block *Block
	err   error
}

// readFrom populates a Container from the given io.Reader checking that the
// CRC32 for the container header is correct.
func (c *Container) readFrom(r io.Reader) error {
	crc := crc32.NewIEEE()
	er := errorReader{r: io.TeeReader(r, crc)}
	var buf [4]byte
	io.ReadFull(&er, buf[:])
	c.blockLen = int32(binary.LittleEndian.Uint32(buf[:]))
	c.refID = er.itf8()
	c.start = er.itf8()
	c.span = er.itf8()
	c.nRec = er.itf8()
	c.recCount = er.ltf8()
	c.bases = er.ltf8()
	c.blocks = er.itf8()
	c.landmarks = er.itf8slice()
	sum := crc.Sum32()
	_, err := io.ReadFull(&er, buf[:])
	if err != nil {
		return err
	}
	c.crc32 = binary.LittleEndian.Uint32(buf[:])
	if c.crc32 != sum {
		return fmt.Errorf("cram: container crc32 mismatch got:0x%08x want:0x%08x", sum, c.crc32)
	}
	if er.err != nil {
		return er.err
	}
	// The spec says T[] is {itf8, element...}.
	// This is not true for byte[] according to
	// the EOF block.
	c.blockData = &io.LimitedReader{R: r, N: int64(c.blockLen)}
	return nil
}

// Next advances the Container to the next CRAM block. It returns false
// when the data ends, either by reaching the end of the container or
// encountering an error.
func (c *Container) Next() bool {
	if c.err != nil {
		return false
	}
	var b Block
	c.err = b.readFrom(c.blockData)
	if c.err == nil {
		c.block = &b
		return true
	}
	return false
}

// Block returns the current CRAM block. The returned Blcok is only
// valid after a previous call to Next has returned true.
func (c *Container) Block() *Block {
	return c.block
}

// Err returns the most recent error.
func (c *Container) Err() error {
	if errors.Is(c.err, io.EOF) {
		return nil
	}
	return c.err
}

// Block is a CRAM block structure.
//
// See CRAM spec section 8.
type Block struct {
	method         byte
	typ            byte
	contentID      int32
	compressedSize int32
	rawSize        int32
	blockData      []byte
	crc32          uint32
}

const (
	rawMethod = iota
	gzipMethod
	bzip2Method
	lzmaMethod
	ransMethod
)

const (
	fileHeader = iota
	compressionHeader
	mappedSliceHeader
	_ // reserved
	externalData
	coreData
)

// readFrom fills a Block from the given io.Reader checking that the
// CRC32 for the block is correct.
func (b *Block) readFrom(r io.Reader) error {
	crc := crc32.NewIEEE()
	er := errorReader{r: io.TeeReader(r, crc)}
	var buf [4]byte
	io.ReadFull(&er, buf[:2])
	b.method = buf[0]
	b.typ = buf[1]
	b.contentID = er.itf8()
	b.compressedSize = er.itf8()
	b.rawSize = er.itf8()
	if b.method == rawMethod && b.compressedSize != b.rawSize {
		return fmt.Errorf("cram: compressed (%d) != raw (%d) size for raw method", b.compressedSize, b.rawSize)
	}
	// The spec says T[] is {itf8, element...}.
	// This is not true for byte[] according to
	// the EOF block.
	b.blockData = make([]byte, b.compressedSize)
	_, err := io.ReadFull(&er, b.blockData)
	if err != nil {
		return err
	}
	sum := crc.Sum32()
	_, err = io.ReadFull(&er, buf[:])
	if err != nil {
		return err
	}
	b.crc32 = binary.LittleEndian.Uint32(buf[:])
	if b.crc32 != sum {
		return fmt.Errorf("cram: block crc32 mismatch got:0x%08x want:0x%08x", sum, b.crc32)
	}
	return nil
}

// Value returns the value of the Block.
//
// Note that rANS decompression is not implemented.
func (b *Block) Value() (interface{}, error) {
	switch b.typ {
	case fileHeader:
		var h sam.Header
		blockData, err := b.expandBlockdata()
		if err != nil {
			return nil, err
		}
		end := binary.LittleEndian.Uint32(blockData[:4])
		err = h.UnmarshalText(blockData[4 : 4+end])
		if err != nil {
			return nil, err
		}
		return &h, nil
	case mappedSliceHeader:
		var s Slice
		s.readFrom(bytes.NewReader(b.blockData))
		return &s, nil
	default:
		// Experimental.
		switch b.method {
		case gzipMethod, bzip2Method, lzmaMethod:
			var err error
			b.blockData, err = b.expandBlockdata()
			if err != nil {
				return nil, err
			}
			b.method |= 0x80
		default:
			// Do nothing.
		}
		return b, nil
	}
}

// expandBlockdata decompresses the block's compressed data.
func (b *Block) expandBlockdata() ([]byte, error) {
	switch b.method {
	default:
		panic(fmt.Sprintf("cram: unknown method: %v", b.method))
	case rawMethod:
		return b.blockData, nil
	case gzipMethod:
		gz, err := gzip.NewReader(bytes.NewReader(b.blockData))
		if err != nil {
			return nil, err
		}
		return io.ReadAll(gz)
	case bzip2Method:
		return io.ReadAll(bzip2.NewReader(bytes.NewReader(b.blockData)))
	case lzmaMethod:
		lz, err := lzma.NewReader(bytes.NewReader(b.blockData))
		if err != nil {
			return nil, err
		}
		return io.ReadAll(lz)
	case ransMethod:
		// Unimplemented.
		// BUG(kortschak): The rANS method is not implemented.
		// Data blocks compressed with rANS will be returned
		// compressed and an "unimplemented" error will be
		// returned.
		return b.blockData, errors.New("unimplemented")
	}
}

// Slice is a CRAM slice header block.
//
// See CRAM spec section 8.5.
type Slice struct {
	refID         int32
	start         int32
	span          int32
	nRec          int32
	recCount      int64
	blocks        int32
	blockIDs      []int32
	embeddedRefID int32
	md5sum        [16]byte
	tags          []byte
}

// readFrom populates a Slice from the given io.Reader.
func (s *Slice) readFrom(r io.Reader) error {
	er := errorReader{r: r}
	s.refID = er.itf8()
	s.start = er.itf8()
	s.span = er.itf8()
	s.nRec = er.itf8()
	s.recCount = er.ltf8()
	s.blocks = er.itf8()
	s.blockIDs = er.itf8slice()
	s.embeddedRefID = er.itf8()
	_, err := io.ReadFull(&er, s.md5sum[:])
	if err != nil {
		return err
	}
	s.tags, err = io.ReadAll(&er)
	return err
}

// errorReader is a sticky error io.Reader.
type errorReader struct {
	r   io.Reader
	err error
}

// Read implements the io.Reader interface.
func (r *errorReader) Read(b []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	var n int
	n, r.err = r.r.Read(b)
	return n, r.err
}

// itf8 returns the ITF-8 encoded number at the current reader position.
func (r *errorReader) itf8() int32 {
	var buf [5]byte
	_, r.err = io.ReadFull(r, buf[:1])
	if r.err != nil {
		return 0
	}
	i, n, ok := itf8.Decode(buf[:1])
	if ok {
		return i
	}
	_, r.err = io.ReadFull(r, buf[1:n])
	if r.err != nil {
		return 0
	}
	i, _, ok = itf8.Decode(buf[:n])
	if !ok {
		r.err = fmt.Errorf("cram: failed to decode itf-8 stream %#v", buf[:n])
	}
	return i
}

// itf8slice returns the n[ITF-8] encoded numbers at the current reader position
// where n is an ITF-8 encoded number.
func (r *errorReader) itf8slice() []int32 {
	n := r.itf8()
	if r.err != nil {
		return nil
	}
	if n == 0 {
		return nil
	}
	s := make([]int32, n)
	for i := range s {
		s[i] = r.itf8()
		if r.err != nil {
			return s[:i]
		}
	}
	return s
}

// itf8 returns the LTF-8 encoded number at the current reader position.
func (r *errorReader) ltf8() int64 {
	var buf [9]byte
	_, r.err = io.ReadFull(r, buf[:1])
	if r.err != nil {
		return 0
	}
	i, n, ok := ltf8.Decode(buf[:1])
	if ok {
		return i
	}
	_, r.err = io.ReadFull(r, buf[1:n])
	if r.err != nil {
		return 0
	}
	i, _, ok = ltf8.Decode(buf[:n])
	if !ok {
		r.err = fmt.Errorf("cram: failed to decode ltf-8 stream %#v", buf[:n])
	}
	return i
}
