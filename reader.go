// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"encoding/binary"
	"errors"
	"io"

	"code.google.com/p/biogo.bam/bgzf"
)

type Reader struct {
	r *bgzf.Reader
	h *Header
	c *bgzf.Chunk

	lastChunk bgzf.Chunk
}

func NewReader(r io.Reader, rd int) (*Reader, error) {
	bg, err := bgzf.NewReader(r, rd)
	if err != nil {
		return nil, err
	}
	br := &Reader{
		r: bg,
		h: &Header{
			seenRefs:   set{},
			seenGroups: set{},
			seenProgs:  set{},
		},
	}
	err = br.h.read(br.r)
	if err != nil {
		return nil, err
	}
	br.lastChunk.End = br.r.LastChunk().End

	return br, nil
}

func (br *Reader) Header() *Header {
	return br.h
}

// BAM record layout.
type bamRecordFixed struct {
	blockSize int32
	refID     int32
	pos       int32
	nLen      uint8
	mapQ      uint8
	bin       uint16
	nCigar    uint16
	flags     Flags
	lSeq      int32
	nextRefID int32
	nextPos   int32
	tLen      int32
}

var (
	lenFieldSize      = binary.Size(bamRecordFixed{}.blockSize)
	bamFixedRemainder = binary.Size(bamRecordFixed{}) - lenFieldSize
)

func (br *Reader) Read() (*Record, error) {
	if br.c != nil && vOffset(br.r.LastChunk().End) >= vOffset(br.c.End) {
		return nil, io.EOF
	}

	r := errReader{r: br.r}
	bin := binaryReader{r: &r}

	// Read record header data.
	blockSize := int(bin.readInt32())
	r.n = 0 // The blocksize field is not included in the blocksize.

	// br.r.Chunk() is only valid after the call the Read(), so this
	// must come after the first read in the record.
	br.lastChunk.Begin = br.r.LastChunk().Begin
	defer func() {
		br.lastChunk.End = br.r.LastChunk().End
	}()

	var rec Record

	refID := bin.readInt32()
	rec.Pos = int(bin.readInt32())
	nLen := bin.readUint8()
	rec.MapQ = bin.readUint8()
	_ = bin.readUint16()
	nCigar := bin.readUint16()
	rec.Flags = Flags(bin.readUint16())
	lSeq := bin.readInt32()
	nextRefID := bin.readInt32()
	rec.MatePos = int(bin.readInt32())
	rec.TempLen = int(bin.readInt32())
	if r.err != nil {
		return nil, r.err
	}

	// Read variable length data.
	name := make([]byte, nLen)
	if nf, _ := r.Read(name); nf != int(nLen) {
		return nil, errors.New("bam: truncated record name")
	}
	rec.Name = string(name[:len(name)-1]) // The BAM spec indicates name is null terminated.

	rec.Cigar = readCigarOps(&bin, nCigar)
	if r.err != nil {
		return nil, r.err
	}

	seq := make(nybblePairs, (lSeq+1)>>1)
	if nf, _ := r.Read(seq.Bytes()); nf != int((lSeq+1)>>1) {
		return nil, errors.New("bam: truncated sequence")
	}
	rec.Seq = NybbleSeq{Length: int(lSeq), Seq: seq}

	rec.Qual = make([]byte, lSeq)
	if nf, _ := r.Read(rec.Qual); nf != int(lSeq) {
		return nil, errors.New("bam: truncated quality")
	}

	auxTags := make([]byte, blockSize-r.n)
	r.Read(auxTags)
	if r.n != blockSize {
		return nil, errors.New("bam: truncated auxilliary data")
	}
	rec.AuxTags = parseAux(auxTags)

	if r.err != nil {
		return nil, r.err
	}

	refs := int32(len(br.h.Refs()))
	if refID != -1 {
		if refID < -1 || refID >= refs {
			return nil, errors.New("bam: reference id out of range")
		}
		rec.Ref = br.h.Refs()[refID]
	}
	if nextRefID != -1 {
		if nextRefID < -1 || nextRefID >= refs {
			return nil, errors.New("bam: mate reference id out of range")
		}
		rec.MateRef = br.h.Refs()[nextRefID]
	}

	return &rec, nil
}

func (r *Reader) SetChunk(c *bgzf.Chunk) {
	if c != nil {
		r.r.Seek(c.Begin)
	}
	r.c = c
}

func (r *Reader) LastChunk() bgzf.Chunk {
	return r.lastChunk
}

func readCigarOps(br *binaryReader, n uint16) []CigarOp {
	co := make([]CigarOp, n)
	for i := range co {
		co[i] = CigarOp(br.readUint32())
		if br.r.err != nil {
			return nil
		}
	}
	return co
}

type errReader struct {
	r   *bgzf.Reader
	n   int
	err error
}

func (r *errReader) Read(p []byte) (int, error) {
	if r.err != nil {
		return 0, r.err
	}
	var n int
	n, r.err = r.r.Read(p)
	r.n += n
	return n, r.err
}

type binaryReader struct {
	r   *errReader
	buf [4]byte
}

func (r *binaryReader) readUint8() uint8 {
	r.r.Read(r.buf[:1])
	return r.buf[0]
}

func (r *binaryReader) readUint16() uint16 {
	r.r.Read(r.buf[:2])
	return binary.LittleEndian.Uint16(r.buf[:2])
}

func (r *binaryReader) readInt32() int32 {
	r.r.Read(r.buf[:4])
	return int32(binary.LittleEndian.Uint32(r.buf[:4]))
}

func (r *binaryReader) readUint32() uint32 {
	r.r.Read(r.buf[:4])
	return binary.LittleEndian.Uint32(r.buf[:4])
}
