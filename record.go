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
)

type Record struct {
	Name    string
	Ref     *Reference
	Pos     int
	MapQ    byte
	Cigar   []CigarOp
	Flags   Flags
	MateRef *Reference
	MatePos int
	TempLen int
	Seq     NybbleSeq
	Qual    []byte
	AuxTags []Aux
}

func NewRecord(name string, ref, mRef *Reference, p, mPos, tLen int, mapQ byte, co []CigarOp, seq, qual []byte, aux []Aux) (*Record, error) {
	if !(validPos(p) && validPos(mPos) && validTmpltLen(tLen) && validLen(len(seq)) && validLen(len(qual))) {
		return nil, errors.New("bam: value out of range")
	}
	if len(qual) != len(seq) {

	}
	if ref != nil {
		if ref.id < 0 {
			return nil, errors.New("bam: linking to invalid reference")
		}
	}
	if mRef != nil {
		if mRef.id < 0 {
			return nil, errors.New("bam: linking to invalid mate reference")
		}
	}
	r := &Record{
		Name:    name,
		Ref:     ref,
		Pos:     p,
		MapQ:    mapQ,
		Cigar:   co,
		MateRef: mRef,
		MatePos: mPos,
		TempLen: tLen,
		Seq:     NewNybbleSeq(seq),
		Qual:    qual,
		AuxTags: aux,
	}
	return r, nil
}

func (r *Record) Reference() *Reference {
	return r.Ref
}

// Tag returns an Aux tag whose tag ID matches the first two bytes of tag and true.
// If no tag matches, nil and false are returned.
func (r *Record) Tag(tag []byte) (v Aux, ok bool) {
	for i := range r.AuxTags {
		if bytes.Compare(r.AuxTags[i][:2], tag) == 0 {
			return r.AuxTags[i], true
		}
	}
	return
}

// Start returns the lower-coordinate end of the alignment.
func (r *Record) Start() int {
	return r.Pos
}

// Bin returns the BAM index bin of the record.
func (r *Record) Bin() int {
	return int(reg2bin(r.Pos, r.End()))
}

// Len returns the length of the alignment.
func (r *Record) Len() int {
	return r.End() - r.Start()
}

// End returns the higher-coordinate end of the alignment.
// This is the start plus the sum of CigarMatch lengths.
func (r *Record) End() int {
	end := r.Pos
	for i, co := range r.Cigar {
		if t := co.Type(); t == CigarBack {
			if i == len(r.Cigar)-1 {
				break
			}
			var j, forw, delta int
			back := co.Len()
			for j = i - 1; j >= 0; j-- {
				x := r.Cigar[j]
				tx, lx := x.Type(), x.Len()
				if consume[tx].query {
					if forw+lx >= back {
						if consume[tx].ref {
							delta += back - forw
						}
						break
					} else {
						forw += lx
					}
				}
				if consume[t].ref {
					delta += lx
				}
			}
			if j < 0 {
				end = r.Pos
			} else {
				end -= delta
			}
		} else if consume[t].ref {
			end += co.Len()
		}
	}
	return end
}

// Strand returns an int8 indicating the strand of the alignment. A positive return indicates
// alignment in the forward orientation, a negative returns indicates alignemnt in the reverse
// orientation.
func (r *Record) Strand() int8 {
	if r.Flags&Reverse == Reverse {
		return -1
	}
	return 1
}

// String returns a string representation of the Record.
func (r *Record) String() string {
	end := r.End()
	return fmt.Sprintf("%s %v %v %d %s:%d..%d (%d) %d %s:%d %d %s %v %v",
		r.Name,
		r.Flags,
		r.Cigar,
		r.MapQ,
		r.Ref.Name(),
		r.Pos,
		end,
		int(reg2bin(r.Pos, end)),
		end-r.Pos,
		r.MateRef.Name(),
		r.MatePos,
		r.TempLen,
		r.Seq.Expand(),
		r.Qual,
		r.AuxTags,
	)
}

// BAM record types
type bamRecordFixed struct {
	BlockSize int32
	RefID     int32
	Pos       int32
	NLen      uint8
	MapQ      uint8
	Bin       uint16
	NCigar    uint16
	Flags     Flags
	LSeq      int32
	NextRefID int32
	NextPos   int32
	TLen      int32
}

type bamRecord struct {
	bamRecordFixed
	readName []byte
	cigar    []CigarOp
	seq      []NybblePair
	qual     []byte
	auxTags  []byte
}

var (
	lenFieldSize      = binary.Size(bamRecordFixed{}.BlockSize)
	bamFixedRemainder = binary.Size(bamRecordFixed{}) - lenFieldSize
)

func (br *bamRecord) unmarshal(h *Header) *Record {
	fixed := br.bamRecordFixed
	var ref, mateRef *Reference
	if fixed.RefID != -1 {
		ref = h.Refs()[fixed.RefID]
	}
	if fixed.NextRefID != -1 {
		mateRef = h.Refs()[fixed.NextRefID]
	}
	return &Record{
		Name:    string(br.readName[:len(br.readName)-1]), // The BAM spec indicates name is null terminated.
		Ref:     ref,
		Pos:     int(fixed.Pos),
		MapQ:    fixed.MapQ,
		Cigar:   br.cigar,
		Flags:   fixed.Flags,
		Seq:     NybbleSeq{Length: int(br.LSeq), Seq: br.seq},
		Qual:    br.qual,
		TempLen: int(fixed.TLen),
		MateRef: mateRef,
		MatePos: int(fixed.NextPos),
		AuxTags: parseAux(br.auxTags),
	}
}

func (br *bamRecord) readFrom(r io.Reader) error {
	h := &br.bamRecordFixed
	err := binary.Read(r, Endian, h)
	if err != nil {
		return err
	}
	n := int(br.BlockSize) - bamFixedRemainder

	br.readName = make([]byte, h.NLen)
	nf, err := r.Read(br.readName)
	if err != nil {
		return err
	}
	if nf != int(h.NLen) {
		return errors.New("bam: truncated record name")
	}
	n -= nf

	br.cigar, nf, err = readCigarOps(r, h.NCigar)
	if err != nil {
		return err
	}
	n -= nf

	seq := make([]byte, h.LSeq>>1)
	nf, err = r.Read(seq)
	if err != nil {
		return err
	}
	if nf != int((h.LSeq+1)>>1) {
		return errors.New("bam: truncated sequence")
	}
	br.seq = *(*[]NybblePair)(unsafe.Pointer(&seq))
	n -= nf

	br.qual = make([]byte, h.LSeq)
	nf, err = r.Read(br.qual)
	if err != nil {
		return err
	}
	if nf != int(h.LSeq) {
		return errors.New("bam: truncated quality")
	}
	n -= nf

	br.auxTags = make([]byte, n)
	nf, err = r.Read(br.auxTags)
	if err != nil {
		return err
	}
	if n != nf {
		return errors.New("bam: truncated auxilliary data")
	}

	return nil
}

func readCigarOps(r io.Reader, n uint16) (co []CigarOp, nf int, err error) {
	co = make([]CigarOp, n)
	size := binary.Size(CigarOp(0))
	for i := range co {
		err = binary.Read(r, Endian, &co[i])
		if err != nil {
			return nil, nf, err
		}
		nf += size
	}
	return
}

func (r *Record) marshal(br *bamRecord) int {
	tags := buildAux(r.AuxTags)
	recLen := bamFixedRemainder +
		len(r.Name) + 1 + // Null terminated.
		len(r.Cigar)<<2 + // CigarOps are 4 bytes.
		len(r.Seq.Seq) +
		len(r.Qual) +
		len(tags)
	*br = bamRecord{
		bamRecordFixed: bamRecordFixed{
			BlockSize: int32(recLen),
			RefID:     r.Ref.id,
			Pos:       int32(r.Pos),
			NLen:      byte(len(r.Name) + 1),
			MapQ:      r.MapQ,
			Bin:       reg2bin(r.Pos, r.End()), //r.bin,
			NCigar:    uint16(len(r.Cigar)),
			Flags:     r.Flags,
			LSeq:      int32(len(r.Qual)),
			NextRefID: int32(r.MateRef.id),
			NextPos:   int32(r.MatePos),
			TLen:      int32(r.TempLen),
		},
		readName: append([]byte(r.Name), 0),
		cigar:    r.Cigar,
		seq:      r.Seq.Seq,
		qual:     r.Qual,
		auxTags:  tags,
	}
	return recLen
}

func (br *bamRecord) writeTo(w io.Writer) error {
	err := binary.Write(w, Endian, br.bamRecordFixed)

	_, err = w.Write(br.readName)
	if err != nil {
		return err
	}

	err = writeCigarOps(w, br.cigar)
	if err != nil {
		return err
	}

	_, err = w.Write(*(*[]byte)(unsafe.Pointer(&br.seq)))
	if err != nil {
		return err
	}

	_, err = w.Write(br.qual)
	if err != nil {
		return err
	}

	_, err = w.Write(br.auxTags)
	if err != nil {
		return err
	}

	return nil
}

func writeCigarOps(w io.Writer, co []CigarOp) (err error) {
	for _, o := range co {
		err = binary.Write(w, Endian, o)
		if err != nil {
			return err
		}
	}
	return
}

type NybblePair byte

type NybbleSeq struct {
	Length int
	Seq    []NybblePair
}

var (
	n16TableRev = [16]byte{'=', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N'}
	n16Table    = [256]NybblePair{
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0x1, 0x2, 0x4, 0x8, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0x0, 0xf, 0xf,
		0xf, 0x1, 0xe, 0x2, 0xd, 0xf, 0xf, 0x4, 0xb, 0xf, 0xf, 0xc, 0xf, 0x3, 0xf, 0xf,
		0xf, 0xf, 0x5, 0x6, 0x8, 0xf, 0x7, 0x9, 0xf, 0xa, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0x1, 0xe, 0x2, 0xd, 0xf, 0xf, 0x4, 0xb, 0xf, 0xf, 0xc, 0xf, 0x3, 0xf, 0xf,
		0xf, 0xf, 0x5, 0x6, 0x8, 0xf, 0x7, 0x9, 0xf, 0xa, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
	}
)

func NewNybbleSeq(s []byte) NybbleSeq {
	return NybbleSeq{
		Length: len(s),
		Seq:    contract(s),
	}
}

func contract(s []byte) []NybblePair {
	ns := make([]NybblePair, (len(s)+1)>>1)
	var np NybblePair
	for i, b := range s {
		if i&1 == 0 {
			np = n16Table[b] << 4
		} else {
			ns[i>>1] = np | n16Table[b]
		}
	}
	return ns
}

func (ns NybbleSeq) Expand() []byte {
	s := make([]byte, ns.Length)
	for i := range s {
		if i&1 == 0 {
			s[i] = n16TableRev[ns.Seq[i>>1]>>4]
		} else {
			s[i] = n16TableRev[ns.Seq[i>>1]&0xf]
		}
	}

	return s
}
