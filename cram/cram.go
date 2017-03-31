// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cram

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"os"
	"reflect"

	"github.com/biogo/hts/cram/encoding/itf8"
	"github.com/biogo/hts/cram/encoding/ltf8"
	"github.com/biogo/hts/sam"
)

var cramEOFmarker = []byte{
	0x0f, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |........|
	0x0f, 0xe0, 0x45, 0x4f, 0x46, 0x00, 0x00, 0x00, // |..EOF...|
	0x00, 0x01, 0x00, 0x05, 0xbd, 0xd9, 0x4f, 0x00, // |......O.|
	0x01, 0x00, 0x06, 0x06, 0x01, 0x00, 0x01, 0x00, // |........|
	0x01, 0x00, 0xee, 0x63, 0x01, 0x4b, /*       */ // |...c.K|
}

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

// CRAM spec section 6.
type definition struct {
	Magic   [4]byte `is:"CRAM"`
	Version [2]byte
	ID      [20]byte
}

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

// CRAM spec section 7.
type container struct {
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
}

func (c *container) readFrom(r io.Reader) error {
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

// CRAM spec section 8.
type block struct {
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

func (b *block) readFrom(r io.Reader) error {
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

func (b *block) value() interface{} {
	switch b.typ {
	case fileHeader:
		var h sam.Header
		end := binary.LittleEndian.Uint32(b.blockData[:4])
		err := h.UnmarshalText(b.blockData[4 : 4+end])
		if err != nil {
			return err
		}
		return &h
	case mappedSliceHeader:
		var s slice
		s.readFrom(bytes.NewReader(b.blockData))
		return &s
	default:
		return b
	}
}

// CRAM spec section 8.5.
type slice struct {
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

func (s *slice) readFrom(r io.Reader) error {
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
	s.tags, err = ioutil.ReadAll(&er)
	return err
}

type errorReader struct {
	r   io.Reader
	err error
}

func (r *errorReader) Read(b []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	var n int
	n, r.err = r.r.Read(b)
	return n, r.err
}

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
