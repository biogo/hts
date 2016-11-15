// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"fmt"
)

// Cigar is a set of CIGAR operations.
type Cigar []CigarOp

// IsValid returns whether the CIGAR string is valid for a record of the given
// sequence length. Validity is defined by the sum of query consuming operations
// matching the given length, clipping operations only being present at the ends
// of alignments, and that CigarBack operations only result in query-consuming
// positions at or right of the start of the alignment.
func (c Cigar) IsValid(length int) bool {
	var pos int
	for i, co := range c {
		ct := co.Type()
		if ct == CigarHardClipped && i != 0 && i != len(c)-1 {
			return false
		}
		if ct == CigarSoftClipped && i != 0 && i != len(c)-1 {
			if c[i-1].Type() != CigarHardClipped && c[i+1].Type() != CigarHardClipped {
				return false
			}
		}
		con := ct.Consumes()
		if pos < 0 && con.Query != 0 {
			return false
		}
		length -= co.Len() * con.Query
		pos += co.Len() * con.Reference
	}
	return length == 0
}

// String returns the CIGAR string for c.
func (c Cigar) String() string {
	if len(c) == 0 {
		return "*"
	}
	var b bytes.Buffer
	for _, co := range c {
		fmt.Fprint(&b, co)
	}
	return b.String()
}

// Lengths returns the number of reference and read bases described by the Cigar.
func (c Cigar) Lengths() (ref, read int) {
	var con Consume
	for _, co := range c {
		con = co.Type().Consumes()
		if co.Type() != CigarBack {
			ref += co.Len() * con.Reference
		}
		read += co.Len() * con.Query
	}
	return ref, read
}

// CigarOp is a single CIGAR operation including the operation type and the
// length of the operation.
type CigarOp uint32

// NewCigarOp returns a CIGAR operation of the specified type with length n.
func NewCigarOp(t CigarOpType, n int) CigarOp {
	return CigarOp(t) | (CigarOp(n) << 4)
}

// Type returns the type of the CIGAR operation for the CigarOp.
func (co CigarOp) Type() CigarOpType { return CigarOpType(co & 0xf) }

// Len returns the number of positions affected by the CigarOp CIGAR operation.
func (co CigarOp) Len() int { return int(co >> 4) }

// String returns the string representation of the CigarOp
func (co CigarOp) String() string { return fmt.Sprintf("%d%s", co.Len(), co.Type().String()) }

// A CigarOpType represents the type of operation described by a CigarOp.
type CigarOpType byte

const (
	CigarMatch       CigarOpType = iota // Alignment match (can be a sequence match or mismatch).
	CigarInsertion                      // Insertion to the reference.
	CigarDeletion                       // Deletion from the reference.
	CigarSkipped                        // Skipped region from the reference.
	CigarSoftClipped                    // Soft clipping (clipped sequences present in SEQ).
	CigarHardClipped                    // Hard clipping (clipped sequences NOT present in SEQ).
	CigarPadded                         // Padding (silent deletion from padded reference).
	CigarEqual                          // Sequence match.
	CigarMismatch                       // Sequence mismatch.
	CigarBack                           // Skip backwards.
	lastCigar
)

var cigarOps = []string{"M", "I", "D", "N", "S", "H", "P", "=", "X", "B", "?"}

// Consumes returns the CIGAR operation alignment consumption characteristics for the CigarOpType.
//
// The Consume values for each of the CigarOpTypes is as follows:
//
//                    Query  Reference
//  CigarMatch          1        1
//  CigarInsertion      1        0
//  CigarDeletion       0        1
//  CigarSkipped        0        1
//  CigarSoftClipped    1        0
//  CigarHardClipped    0        0
//  CigarPadded         0        0
//  CigarEqual          1        1
//  CigarMismatch       1        1
//  CigarBack           0       -1
//
func (ct CigarOpType) Consumes() Consume { return consume[ct] }

// String returns the string representation of a CigarOpType.
func (ct CigarOpType) String() string {
	if ct < 0 || ct > lastCigar {
		ct = lastCigar
	}
	return cigarOps[ct]
}

// Consume describes how CIGAR operations consume alignment bases.
type Consume struct {
	Query, Reference int
}

// A few years ago, Complete Genomics (CG) proposed to add a new CIGAR
// operator 'B' for an operation of moving backward along the reference
// genome. It is the opposite of the 'N', the reference skip. In a later
// discussion on a separate issue, Fred expressed his preference to a
// negative reference skip which is equivalent to a positive 'B' operation.
// Now the SRA group from NCBI intends to archive the CG alignment in
// the SAM format and raises this request again. I think it may be the
// time to add this backward operation.
//
// The backward operation is designed to describe such an alignment:
//
// REF:: GCATACGATCGACTAGTCACGT
// READ: --ATACGATCGA----------
// READ: ---------CGACTAGTCAC--
//
// i.e. there is an overlap between two segments of a read, which is quite
// frequent in CG data. We are unable to fully describe such an alignment
// with the original CIGAR. In the current spec, we suggest using a CIGAR
// 18M and storing the overlap information in optional tags. This is a
// little clumsy and is not compressed well for the purpose of archiving.
// With 'B', the new CIGAR is "10M3B11M" with no other optional tags.
//
// Using "B" in this case is cleaner, but the major concern is that it breaks
// the compatibility and is also likely to complicate SNP calling and many
// other applications. As I think now, the solution is to implement a
// "remove_B()" routine in samtools. This routine collapses overlapping
// sequences, recalculates base quality in the overlap and gives a CIGAR
// without 'B'. For the example above, remove_B() gives CIGAR 18M. For SNP
// calling, we may call remove_B() immediately after the alignment loaded
// into memory. The downstream pileup engine does not need any changes. Other
// SNP callers can do the same. A new option will be added to "samtools view"
// as a way to remove 'B' operations on the command-line.
//
// The implementation of remove_B() may be quite complicated in the generic
// case - we may be dealing with a multiple-sequence alignment, but it should
// be straightforward in the simple cases such as the example above. Users may
// not need to care too much about how remove_B() is implemented.
//
// http://sourceforge.net/p/samtools/mailman/message/28463294/
var consume = []Consume{
	CigarMatch:       {Query: 1, Reference: 1},
	CigarInsertion:   {Query: 1, Reference: 0},
	CigarDeletion:    {Query: 0, Reference: 1},
	CigarSkipped:     {Query: 0, Reference: 1},
	CigarSoftClipped: {Query: 1, Reference: 0},
	CigarHardClipped: {Query: 0, Reference: 0},
	CigarPadded:      {Query: 0, Reference: 0},
	CigarEqual:       {Query: 1, Reference: 1},
	CigarMismatch:    {Query: 1, Reference: 1},
	CigarBack:        {Query: 0, Reference: -1}, // See notes above.
	lastCigar:        {},
}

var cigarOpTypeLookup [256]CigarOpType

func init() {
	for i := range cigarOpTypeLookup {
		cigarOpTypeLookup[i] = lastCigar
	}
	for op, c := range []byte{'M', 'I', 'D', 'N', 'S', 'H', 'P', '=', 'X', 'B'} {
		cigarOpTypeLookup[c] = CigarOpType(op)
	}
}

var powers = []int{1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8}

// atoi returns the integer interpretation of b which must be an ASCII decimal number representation.
func atoi(b []byte, i int) (int, error) {
	n := 0
	k := len(b) - 1
	for i, v := range b {
		n += int(v-'0') * powers[k-i]
	}
	if n < 0 || 1<<28 <= n {
		return n, fmt.Errorf("sam: invalid cigar operation count: %q at %d", b, i)
	}
	return n, nil
}

// ParseCigar returns a Cigar parsed from the provided byte slice.
func ParseCigar(b []byte) (Cigar, error) {
	if len(b) == 1 && b[0] == '*' {
		return nil, nil
	}
	var (
		c   Cigar
		op  CigarOpType
		n   int
		err error
	)
	for i := 0; i < len(b); i++ {
		for j := i; j < len(b); j++ {
			if b[j] < '0' || '9' < b[j] {
				n, err = atoi(b[i:j], i)
				if err != nil {
					return nil, err
				}
				op = cigarOpTypeLookup[b[j]]
				i = j
				break
			}
		}
		if op == lastCigar {
			return nil, fmt.Errorf("sam: failed to parse cigar string %q: unknown operation %q", b, op)
		}
		c = append(c, NewCigarOp(op, n))
	}
	return c, nil
}
