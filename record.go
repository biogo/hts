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
		return nil, errors.New("bam: sequence/quality length mismatch")
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
