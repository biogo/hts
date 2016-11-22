// Copyright ©2012 The bíogo Authors. All rights reserved.
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

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/sam"
)

// Reader implements BAM data reading.
type Reader struct {
	r *bgzf.Reader
	h *sam.Header
	c *bgzf.Chunk

	// omit specifies how much of the
	// record should be omitted during
	// a read of the BAM input.
	omit int

	lastChunk bgzf.Chunk

	// buf is used to read the block size of each record.
	buf [4]byte
}

// NewReader returns a new Reader using the given io.Reader
// and setting the read concurrency to rd. If rd is zero
// concurrency is set to GOMAXPROCS. The returned Reader
// should be closed after use to avoid leaking resources.
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

// Header returns the SAM Header held by the Reader.
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

func vOffset(o bgzf.Offset) int64 {
	return o.File<<16 | int64(o.Block)
}

// Omit specifies what portions of the Record to omit reading.
// When o is None, a full sam.Record is returned by Read, when o
// is AuxTags the auxiliary tag data is omitted and when o is
// AllVariableLengthData, sequence, quality and auxiliary data
// is omitted.
func (br *Reader) Omit(o int) {
	br.omit = o
}

// None, AuxTags and AllVariableLengthData are values taken
// by the Reader Omit method.
const (
	None                  = iota // Omit no field data from the record.
	AuxTags                      // Omit auxiliary tag data.
	AllVariableLengthData        // Omit sequence, quality and auxiliary data.
)

// Read returns the next sam.Record in the BAM stream.
//
// The sam.Record returned will not contain the sequence, quality or
// auxiliary tag data if Omit(AllVariableLengthData) has been called
// prior to the Read call and will not contain the auxiliary tag data
// is Omit(AuxTags) has been called.
func (br *Reader) Read() (*sam.Record, error) {
	if br.c != nil && vOffset(br.r.LastChunk().End) >= vOffset(br.c.End) {
		return nil, io.EOF
	}

	b, err := newBuffer(br)
	if err != nil {
		return nil, err
	}

	var rec sam.Record
	refID := b.readInt32()
	rec.Pos = int(b.readUint32())
	nLen := b.readUint8()
	rec.MapQ = b.readUint8()
	b.discard(2)
	nCigar := b.readUint16()
	rec.Flags = sam.Flags(b.readUint16())
	lSeq := int32(b.readUint32())
	nextRefID := int32(b.readUint32())
	rec.MatePos = int(b.readInt32())
	rec.TempLen = int(b.readInt32())
	// Read variable length data.
	rec.Name = string(b.bytes(int(nLen) - 1))
	b.discard(1)

	rec.Cigar = readCigarOps(b.bytes(int(nCigar) * 4))

	var seq doublets
	var auxTags []byte
	if br.omit >= AllVariableLengthData {
		goto done
	}

	seq = make(doublets, (lSeq+1)>>1)
	*(*[]byte)(unsafe.Pointer(&seq)) = b.bytes(int(lSeq+1) >> 1)

	rec.Seq = sam.Seq{Length: int(lSeq), Seq: seq}
	rec.Qual = b.bytes(int(lSeq))

	if br.omit >= AuxTags {
		goto done
	}
	auxTags = b.bytes(b.len())
	rec.AuxFields = parseAux(auxTags)

done:
	refs := int32(len(br.h.Refs()))
	if refID != -1 {
		if refID < -1 || refID >= refs {
			return nil, errors.New("bam: reference id out of range")
		}
		rec.Ref = br.h.Refs()[refID]
	}
	if nextRefID != -1 {
		if refID == nextRefID {
			rec.MateRef = rec.Ref
			return &rec, nil
		}
		if nextRefID < -1 || nextRefID >= refs {
			return nil, errors.New("bam: mate reference id out of range")
		}
		rec.MateRef = br.h.Refs()[nextRefID]
	}

	return &rec, nil
}

// SetCache sets the cache to be used by the Reader.
func (bg *Reader) SetCache(c bgzf.Cache) {
	bg.r.SetCache(c)
}

// Seek performs a seek to the specified bgzf.Offset.
func (br *Reader) Seek(off bgzf.Offset) error {
	return br.r.Seek(off)
}

// SetChunk sets a limited range of the underlying BGZF file to read, after
// seeking to the start of the given chunk. It may be used to iterate over
// a defined genomic interval.
func (br *Reader) SetChunk(c *bgzf.Chunk) error {
	if c != nil {
		err := br.r.Seek(c.Begin)
		if err != nil {
			return err
		}
	}
	br.c = c
	return nil
}

// LastChunk returns the bgzf.Chunk corresponding to the last Read operation.
// The bgzf.Chunk returned is only valid if the last Read operation returned a
// nil error.
func (br *Reader) LastChunk() bgzf.Chunk {
	return br.lastChunk
}

// Close closes the Reader.
func (br *Reader) Close() error {
	return br.r.Close()
}

// Iterator wraps a Reader to provide a convenient loop interface for reading BAM data.
// Successive calls to the Next method will step through the features of the provided
// Reader. Iteration stops unrecoverably at EOF or the first error.
type Iterator struct {
	r *Reader

	chunks []bgzf.Chunk

	rec *sam.Record
	err error
}

// NewIterator returns a Iterator to read from r, limiting the reads to the provided
// chunks.
//
//  chunks, err := idx.Chunks(ref, beg, end)
//  if err != nil {
//  	return err
//  }
//  i, err := NewIterator(r, chunks)
//  if err != nil {
//  	return err
//  }
//  for i.Next() {
//  	fn(i.Record())
//  }
//  return i.Close()
//
func NewIterator(r *Reader, chunks []bgzf.Chunk) (*Iterator, error) {
	if len(chunks) == 0 {
		return &Iterator{r: r, err: io.EOF}, nil
	}
	err := r.SetChunk(&chunks[0])
	if err != nil {
		return nil, err
	}
	chunks = chunks[1:]
	return &Iterator{r: r, chunks: chunks}, nil
}

// Next advances the Iterator past the next record, which will then be available through
// the Record method. It returns false when the iteration stops, either by reaching the end of the
// input or an error. After Next returns false, the Error method will return any error that
// occurred during iteration, except that if it was io.EOF, Error will return nil.
func (i *Iterator) Next() bool {
	if i.err != nil {
		return false
	}
	i.rec, i.err = i.r.Read()
	if len(i.chunks) != 0 && i.err == io.EOF {
		i.err = i.r.SetChunk(&i.chunks[0])
		i.chunks = i.chunks[1:]
		return i.Next()
	}
	return i.err == nil
}

// Error returns the first non-EOF error that was encountered by the Iterator.
func (i *Iterator) Error() error {
	if i.err == io.EOF {
		return nil
	}
	return i.err
}

// Record returns the most recent record read by a call to Next.
func (i *Iterator) Record() *sam.Record { return i.rec }

// Close releases the underlying Reader.
func (i *Iterator) Close() error {
	i.r.SetChunk(nil)
	return i.Error()
}

// len(cb) must be a multiple of 4.
func readCigarOps(cb []byte) []sam.CigarOp {
	co := make([]sam.CigarOp, len(cb)/4)
	for i := range co {
		co[i] = sam.CigarOp(binary.LittleEndian.Uint32(cb[i*4 : (i+1)*4]))
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
func parseAux(aux []byte) []sam.Aux {
	if len(aux) == 0 {
		return nil
	}
	aa := make([]sam.Aux, 0, 4)
	for i := 0; i+2 < len(aux); {
		t := aux[i+2]
		switch j := jumps[t]; {
		case j > 0:
			j += 3
			aa = append(aa, sam.Aux(aux[i:i+j:i+j]))
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
				aa = append(aa, sam.Aux(aux[i:i+j:i+j]))
				i += j + 1
			case 'B':
				var length int32
				err := binary.Read(bytes.NewBuffer([]byte(aux[i+4:i+8])), binary.LittleEndian, &length)
				if err != nil {
					panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
				}
				j = int(length)*jumps[aux[i+3]] + int(unsafe.Sizeof(length)) + 4
				aa = append(aa, sam.Aux(aux[i:i+j:i+j]))
				i += j
			}
		default:
			panic(fmt.Sprintf("bam: unrecognised optional field type: %q", t))
		}
	}
	return aa
}

// buffer is light-weight read buffer.
type buffer struct {
	off  int
	data []byte
}

func (b *buffer) bytes(n int) []byte {
	s := b.off
	b.off += n
	return b.data[s:b.off]
}

func (b *buffer) len() int {
	return len(b.data) - b.off
}

func (b *buffer) discard(n int) {
	b.off += n
}

func (b *buffer) readUint8() uint8 {
	b.off++
	return b.data[b.off-1]
}

func (b *buffer) readUint16() uint16 {
	return binary.LittleEndian.Uint16(b.bytes(2))
}

func (b *buffer) readInt32() int32 {
	return int32(binary.LittleEndian.Uint32(b.bytes(4)))
}

func (b *buffer) readUint32() uint32 {
	return binary.LittleEndian.Uint32(b.bytes(4))
}

// newBuffer returns a new buffer reading from the Reader's underlying bgzf.Reader and
// updates the Reader's lastChunk field.
func newBuffer(br *Reader) (*buffer, error) {
	n, err := io.ReadFull(br.r, br.buf[:4])
	// br.r.Chunk() is only valid after the call the Read(), so this
	// must come after the first read in the record.
	tx := br.r.Begin()
	defer func() {
		br.lastChunk = tx.End()
	}()
	if err != nil {
		return nil, err
	}
	if n != 4 {
		return nil, errors.New("bam: invalid record: short block size")
	}
	b := &buffer{data: br.buf[:4]}
	size := int(b.readInt32())
	b.off, b.data = 0, make([]byte, size)
	n, err = io.ReadFull(br.r, b.data)
	if err != nil {
		return nil, err
	}
	if n != size {
		return nil, errors.New("bam: truncated record")
	}
	return b, nil
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

type doublets []sam.Doublet

func (np doublets) Bytes() []byte { return *(*[]byte)(unsafe.Pointer(&np)) }
