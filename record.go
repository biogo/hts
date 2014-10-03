// Copyright ©2012-2013 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"errors"
	"fmt"
	"unsafe"
)

// Record represents a SAM/BAM record.
type Record struct {
	Name    string
	Ref     *Reference
	Pos     int
	MapQ    byte
	Cigar   Cigar
	Flags   Flags
	MateRef *Reference
	MatePos int
	TempLen int
	Seq     NybbleSeq
	Qual    []byte
	AuxTags []Aux
}

// NewRecord returns a Record, checking for consistency of the provided
// attributes.
func NewRecord(name string, ref, mRef *Reference, p, mPos, tLen int, mapQ byte, co []CigarOp, seq, qual []byte, aux []Aux) (*Record, error) {
	if !(validPos(p) && validPos(mPos) && validTmpltLen(tLen) && validLen(len(seq)) && validLen(len(qual))) {
		return nil, errors.New("bam: value out of range")
	}
	if len(qual) != len(seq) {
		return nil, errors.New("bam: sequence/quality length mismatch")
	}
	if ref != nil {
		if ref.id < 0 {
			return nil, errors.New("bam: linking to invalid reference")
		}
	} else {
		if p != -1 {
			return nil, errors.New("bam: specified position != -1 without reference")
		}
	}
	if mRef != nil {
		if mRef.id < 0 {
			return nil, errors.New("bam: linking to invalid mate reference")
		}
	} else {
		if mPos != -1 {
			return nil, errors.New("bam: specified mate position != -1 without mate reference")
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

// IsValidRecord returns whether the record satisfies the conditions that
// it has the Unmapped flag set if it not placed; that the MateUnmapped
// flag is set if it paired its mate is unplaced; that the CIGAR length
// matches the sequence and quality string lengths if they are non-zero; and
// that the Paired, ProperPair, Unmapped and MateUnmapped flags are consistent.
func IsValidRecord(r *Record) bool {
	if (r.Ref == nil || r.Pos == -1) && r.Flags&Unmapped == 0 {
		return false
	}
	if r.Flags&Paired != 0 && (r.MateRef == nil || r.MatePos == -1) && r.Flags&MateUnmapped == 0 {
		return false
	}
	if r.Flags&(Unmapped|ProperPair) == Unmapped|ProperPair {
		return false
	}
	if r.Flags&(Paired|MateUnmapped|ProperPair) == Paired|MateUnmapped|ProperPair {
		return false
	}
	if len(r.Qual) != 0 && r.Seq.Length != len(r.Qual) {
		return false
	}
	if cigarLen := r.Len(); cigarLen < 0 || (r.Seq.Length != 0 && r.Seq.Length != cigarLen) {
		return false
	}
	return true
}

// Reference returns the records reference.
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
	if r.Flags&Unmapped != 0 {
		return 4680 // reg2bin(-1, 0)
	}
	return int(reg2bin(r.Pos, r.End()))
}

// Len returns the length of the alignment.
func (r *Record) Len() int {
	return r.End() - r.Start()
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// End returns the highest query-consuming coordinate end of the alignment.
// The position returned by End is not valid if r.Cigar.IsValid(r.Seq.Length)
// is false.
func (r *Record) End() int {
	pos := r.Pos
	end := r.Pos
	var con Consume
	for _, co := range r.Cigar {
		con = co.Type().Consumes()
		pos += co.Len() * con.Reference
		if con.Query != 0 {
			end = max(end, pos)
		}
	}
	return end
}

// Strand returns an int8 indicating the strand of the alignment. A positive return indicates
// alignment in the forward orientation, a negative returns indicates alignment in the reverse
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

type NybblePair byte

type nybblePairs []NybblePair

func (np nybblePairs) Bytes() []byte { return *(*[]byte)(unsafe.Pointer(&np)) }

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
	// We haven't written the last base if the
	// sequence was odd length, so do that now.
	if len(s)&1 != 0 {
		ns[len(ns)-1] = np
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
