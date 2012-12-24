// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"fmt"
)

type CigarOp uint32

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

// String returns the string representation of a CigarOpType.
func (ct CigarOpType) String() string {
	if ct < 0 || ct > lastCigar {
		ct = lastCigar
	}
	return cigarOps[ct]
}

// cigarConsumer describes how cigar operations consumer alignment bases.
type cigarConsumer struct {
	query, ref bool
}

var consume = []cigarConsumer{
	CigarMatch:       {true, true},
	CigarInsertion:   {false, true},
	CigarDeletion:    {true, false},
	CigarSkipped:     {true, false},
	CigarSoftClipped: {false, true},
	CigarHardClipped: {false, false},
	CigarPadded:      {false, false},
	CigarEqual:       {true, true},
	CigarMismatch:    {true, true},
	CigarBack:        {false, false},
	lastCigar:        {},
}
