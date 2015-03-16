// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam_test

import (
	"fmt"

	"github.com/biogo/hts/sam"
)

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// Overlap returns the length of the overlap between the alignment
// of the SAM record and the interval specified.
//
// Note that this will count repeated matches to the same reference
// location if CigarBack operations are used.
func Overlap(r *sam.Record, start, end int) int {
	var overlap int
	pos := r.Pos
	for _, co := range r.Cigar {
		t := co.Type()
		con := t.Consumes()
		lr := co.Len() * con.Reference
		if con.Query == con.Reference {
			o := min(pos+lr, end) - max(pos, start)
			if o > 0 {
				overlap += o
			}
		}
		pos += lr
	}

	return overlap
}

func ExampleConsume() {
	// Example alignments from the SAM specification:
	//
	// @HD	VN:1.5	SO:coordinate
	// @SQ	SN:ref	LN:45
	// @CO	--------------------------------------------------------
	// @CO	Coor     12345678901234  5678901234567890123456789012345
	// @CO	ref      AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT
	// @CO	--------------------------------------------------------
	// @CO	+r001/1        TTAGATAAAGGATA*CTG
	// @CO	+r002         aaaAGATAA*GGATA
	// @CO	+r003       gcctaAGCTAA
	// @CO	+r004                     ATAGCT..............TCAGC
	// @CO	-r003                            ttagctTAGGC
	// @CO	-r001/2                                        CAGCGGCAT
	// @CO	--------------------------------------------------------
	// r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
	// r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
	// r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
	// r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
	// r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
	// r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1

	const (
		refStart = 0
		refEnd   = 45
	)

	records := []*sam.Record{
		{Name: "r001/1", Pos: 6, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarMatch, 8),
			sam.NewCigarOp(sam.CigarInsertion, 2),
			sam.NewCigarOp(sam.CigarMatch, 4),
			sam.NewCigarOp(sam.CigarDeletion, 1),
			sam.NewCigarOp(sam.CigarMatch, 3),
		}},
		{Name: "r002", Pos: 8, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarSoftClipped, 3),
			sam.NewCigarOp(sam.CigarMatch, 6),
			sam.NewCigarOp(sam.CigarPadded, 1),
			sam.NewCigarOp(sam.CigarInsertion, 1),
			sam.NewCigarOp(sam.CigarMatch, 4),
		}},
		{Name: "r003", Pos: 8, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarSoftClipped, 5),
			sam.NewCigarOp(sam.CigarMatch, 6),
		}},
		{Name: "r004", Pos: 15, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarMatch, 6),
			sam.NewCigarOp(sam.CigarSkipped, 14),
			sam.NewCigarOp(sam.CigarMatch, 5),
		}},
		{Name: "r003", Pos: 28, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarHardClipped, 6),
			sam.NewCigarOp(sam.CigarMatch, 5),
		}},
		{Name: "r001/2", Pos: 36, Cigar: []sam.CigarOp{
			sam.NewCigarOp(sam.CigarMatch, 9),
		}},
	}

	for _, r := range records {
		fmt.Printf("%q overlaps reference by %d letters\n", r.Name, Overlap(r, refStart, refEnd))
	}

	// Output:
	//
	// "r001/1" overlaps reference by 15 letters
	// "r002" overlaps reference by 10 letters
	// "r003" overlaps reference by 6 letters
	// "r004" overlaps reference by 11 letters
	// "r003" overlaps reference by 5 letters
	// "r001/2" overlaps reference by 9 letters
}
