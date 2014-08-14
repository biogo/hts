// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"fmt"
)

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
	Query, Reference bool
}

var consume = []Consume{
	CigarMatch:       {Query: true, Reference: true},
	CigarInsertion:   {Query: true, Reference: false},
	CigarDeletion:    {Query: false, Reference: true},
	CigarSkipped:     {Query: false, Reference: true},
	CigarSoftClipped: {Query: true, Reference: false},
	CigarHardClipped: {Query: true, Reference: false},
	CigarPadded:      {Query: false, Reference: false},
	CigarEqual:       {Query: true, Reference: true},
	CigarMismatch:    {Query: true, Reference: true},
	CigarBack:        {Query: false, Reference: false},
	lastCigar:        {},
}
