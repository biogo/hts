// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"

	"code.google.com/p/biogo.bam/bgzf"
	"code.google.com/p/biogo.bam/sam"
)

type Reader struct {
	r *bgzf.Reader
	h *sam.Header
	c *bgzf.Chunk

	lastChunk bgzf.Chunk
}

func NewReader(r io.Reader, rd int) (*Reader, error) {
	bg, err := bgzf.NewReader(r, rd)
	if err != nil {
		return nil, err
	}
	h, _ := sam.NewHeader(nil, nil)
	br := &Reader{
		r: bg,
		h: h,
	}
	err = br.h.DecodeBinary(br.r)
	if err != nil {
		return nil, err
	}
	br.lastChunk.End = br.r.LastChunk().End

	return br, nil
}

func (br *Reader) Header() *sam.Header {
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
	flags     sam.Flags
	lSeq      int32
	nextRefID int32
	nextPos   int32
	tLen      int32
}

var (
	lenFieldSize      = binary.Size(bamRecordFixed{}.blockSize)
	bamFixedRemainder = binary.Size(bamRecordFixed{}) - lenFieldSize
)

func (br *Reader) Read() (*sam.Record, error) {
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

	var rec sam.Record

	refID := bin.readInt32()
	rec.Pos = int(bin.readInt32())
	nLen := bin.readUint8()
	rec.MapQ = bin.readUint8()
	_ = bin.readUint16()
	nCigar := bin.readUint16()
	rec.Flags = sam.Flags(bin.readUint16())
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

	seq := make(doublets, (lSeq+1)>>1)
	if nf, _ := r.Read(seq.Bytes()); nf != int((lSeq+1)>>1) {
		return nil, errors.New("bam: truncated sequence")
	}
	rec.Seq = sam.Seq{Length: int(lSeq), Seq: seq}

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

func readCigarOps(br *binaryReader, n uint16) []sam.CigarOp {
	co := make([]sam.CigarOp, n)
	for i := range co {
		co[i] = sam.CigarOp(br.readUint32())
		if br.r.err != nil {
			return nil
		}
	}
	return co
}

var jumps = [256]int{
	'A': 1,
	'c': 1, 'C': 1,
	's': 2, 'S': 2,
	'i': 4, 'I': 4,
	'f': 4,
	'Z': -1,
	'H': -1,
	'B': -1,
}

// parseAux examines the data of a SAM record's OPT fields,
// returning a slice of sam.Aux that are backed by the original data.
func parseAux(aux []byte) (aa []sam.Aux) {
	for i := 0; i+2 < len(aux); {
		t := aux[i+2]
		switch j := jumps[t]; {
		case j > 0:
			j += 3
			aa = append(aa, sam.Aux(aux[i:i+j]))
			i += j
		case j < 0:
			switch t {
			case 'Z', 'H':
				var (
					j int
					v byte
				)
				for j, v = range aux[i:] {
					if v == 0 { // C string termination
						break // Truncate terminal zero.
					}
				}
				aa = append(aa, sam.Aux(aux[i:i+j]))
				i += j + 1
			case 'B':
				var length int32
				err := binary.Read(bytes.NewBuffer([]byte(aux[i+4:i+8])), binary.LittleEndian, &length)
				if err != nil {
					panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
				}
				j = int(length)*jumps[aux[i+3]] + int(unsafe.Sizeof(length)) + 4
				aa = append(aa, sam.Aux(aux[i:i+j]))
				i += j
			}
		default:
			panic(fmt.Sprintf("bam: unrecognised optional field type: %q", t))
		}
	}
	return
}

// buildAux constructs a single byte slice that represents a slice of sam.Aux.
func buildAux(aa []sam.Aux) (aux []byte) {
	for _, a := range aa {
		// TODO: validate each 'a'
		aux = append(aux, []byte(a)...)
		switch a.Type() {
		case 'Z', 'H':
			aux = append(aux, 0)
		}
	}
	return
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

type doublets []sam.Doublet

func (np doublets) Bytes() []byte { return *(*[]byte)(unsafe.Pointer(&np)) }
