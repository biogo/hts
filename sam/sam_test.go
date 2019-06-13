// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/check.v1"
)

var (
	bam    = flag.Bool("bam", false, "output failing bam data for inspection")
	allbam = flag.Bool("allbam", false, "output all bam data for inspection")
)

type failure bool

func (f failure) String() string {
	if f {
		return "fail"
	}
	return "ok"
}

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

func (s *S) TestCloneHeader(c *check.C) {
	for _, h := range []*Header{
		headerHG00096_1000,
	} {
		c.Check(h, check.DeepEquals, h.Clone())
	}
}

func (s *S) TestSpecExamples(c *check.C) {
	sr, err := NewReader(bytes.NewReader(specExamples.data))
	c.Assert(err, check.Equals, nil)
	h := sr.Header()
	c.Check(h.Version, check.Equals, specExamples.header.Version)
	c.Check(h.SortOrder, check.Equals, specExamples.header.SortOrder)
	c.Check(h.GroupOrder, check.Equals, specExamples.header.GroupOrder)
	c.Check(h.Comments, check.DeepEquals, specExamples.header.Comments)

	var buf bytes.Buffer
	sw, err := NewWriter(&buf, h, FlagDecimal)
	c.Assert(err, check.Equals, nil)
	for i, expect := range specExamples.records {
		r, err := sr.Read()
		if err != nil {
			c.Errorf("Unexpected early error: %v", err)
			continue
		}
		c.Check(r.Name, check.Equals, expect.Name)
		c.Check(r.Pos, check.Equals, expect.Pos) // Zero-based here.
		c.Check(r.Flags, check.Equals, expect.Flags)
		if r.Flags&Unmapped == 0 {
			c.Check(r.Ref, check.Not(check.Equals), nil)
			if r.Ref != nil {
				c.Check(r.Ref.Name(), check.Equals, h.Refs()[0].Name())
			}
		} else {
			c.Check(r.Ref, check.Equals, nil)
		}
		c.Check(r.MatePos, check.Equals, expect.MatePos) // Zero-based here.
		c.Check(r.Cigar, check.DeepEquals, expect.Cigar)
		c.Check(r.Cigar.IsValid(r.Seq.Length), check.Equals, true)
		c.Check(r.TempLen, check.Equals, expect.TempLen)
		c.Check(r.Seq, check.DeepEquals, expect.Seq, check.Commentf("got:%q expected:%q", r.Seq.Expand(), expect.Seq.Expand()))
		c.Check(r.Qual, check.DeepEquals, expect.Qual) // No valid qualities here.
		c.Check(r.End(), check.Equals, specExamples.readEnds[i], check.Commentf("unexpected end position for %q at %v, got:%d expected:%d", r.Name, r.Pos, r.End(), specExamples.readEnds[i]))
		c.Check(r.AuxFields, check.DeepEquals, expect.AuxFields)

		parsedCigar, err := ParseCigar([]byte(specExamples.cigars[i]))
		c.Check(err, check.Equals, nil)
		c.Check(parsedCigar, check.DeepEquals, expect.Cigar)

		// In all the examples the last base of the read and the last
		// base of the ref are valid, so we can check this.
		expSeq := r.Seq.Expand()
		c.Check(specExamples.ref[r.End()-1], check.Equals, expSeq[len(expSeq)-1])

		// Test round trip.
		err = sw.Write(r)
		c.Check(err, check.Equals, nil)
		b, err := r.MarshalText()
		c.Check(err, check.Equals, nil)
		var nr Record
		c.Check(nr.UnmarshalSAM(sr.Header(), b), check.Equals, nil)
		c.Check(&nr, check.DeepEquals, r)
	}
	c.Check(buf.String(), check.DeepEquals, string(specExamples.data))
}

func mustAux(a Aux, err error) Aux {
	if err != nil {
		panic(err)
	}
	return a
}

var specExamples = struct {
	ref      string
	data     []byte
	header   Header
	records  []*Record
	cigars   []string
	readEnds []int
}{
	ref: "AGCATGTTAGATAAGATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
	data: []byte(`@HD	VN:1.5	SO:coordinate
@SQ	SN:ref	LN:45
@CO	--------------------------------------------------------
@CO	Coor     12345678901234  5678901234567890123456789012345
@CO	ref      AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT
@CO	--------------------------------------------------------
@CO	+r001/1        TTAGATAAAGGATA*CTG
@CO	+r002         aaaAGATAA*GGATA
@CO	+r003       gcctaAGCTAA
@CO	+r004                     ATAGCT..............TCAGC
@CO	-r003                            ttagctTAGGC
@CO	-r001/2                                        CAGCGGCAT
@CO	--------------------------------------------------------
r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
`),
	header: Header{
		Version:    "1.5",
		SortOrder:  Coordinate,
		GroupOrder: GroupUnspecified,
		Comments: []string{
			"--------------------------------------------------------",
			"Coor     12345678901234  5678901234567890123456789012345",
			"ref      AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT",
			"--------------------------------------------------------",
			"+r001/1        TTAGATAAAGGATA*CTG",
			"+r002         aaaAGATAA*GGATA",
			"+r003       gcctaAGCTAA",
			"+r004                     ATAGCT..............TCAGC",
			"-r003                            ttagctTAGGC",
			"-r001/2                                        CAGCGGCAT",
			"--------------------------------------------------------",
		},
	},
	records: []*Record{
		{
			Name: "r001",
			Pos:  6,
			MapQ: 30,
			Cigar: Cigar{
				NewCigarOp(CigarMatch, 8),
				NewCigarOp(CigarInsertion, 2),
				NewCigarOp(CigarMatch, 4),
				NewCigarOp(CigarDeletion, 1),
				NewCigarOp(CigarMatch, 3),
			},
			Flags:   Paired | ProperPair | MateReverse | Read1,
			MatePos: 36,
			TempLen: 39,
			Seq:     NewSeq([]byte("TTAGATAAAGGATACTG")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			Name: "r002",
			Pos:  8,
			MapQ: 30,
			Cigar: Cigar{
				NewCigarOp(CigarSoftClipped, 3),
				NewCigarOp(CigarMatch, 6),
				NewCigarOp(CigarPadded, 1),
				NewCigarOp(CigarInsertion, 1),
				NewCigarOp(CigarMatch, 4),
			},
			MatePos: -1,
			TempLen: 0,
			Seq:     NewSeq([]byte("AAAAGATAAGGATA")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			Name: "r003",
			Pos:  8,
			MapQ: 30,
			Cigar: Cigar{
				NewCigarOp(CigarSoftClipped, 5),
				NewCigarOp(CigarMatch, 6),
			},
			MatePos: -1,
			TempLen: 0,
			Seq:     NewSeq([]byte("GCCTAAGCTAA")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			AuxFields: []Aux{
				mustAux(NewAux(NewTag("SA"), "ref,29,-,6H5M,17,0;")),
			},
		},
		{
			Name: "r004",
			Pos:  15,
			MapQ: 30,
			Cigar: Cigar{
				NewCigarOp(CigarMatch, 6),
				NewCigarOp(CigarSkipped, 14),
				NewCigarOp(CigarMatch, 5),
			},
			MatePos: -1,
			TempLen: 0,
			Seq:     NewSeq([]byte("ATAGCTTCAGC")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		},
		{
			Name: "r003",
			Pos:  28,
			MapQ: 17,
			Cigar: Cigar{
				NewCigarOp(CigarHardClipped, 6),
				NewCigarOp(CigarMatch, 5),
			},
			Flags:   Reverse | Supplementary,
			MatePos: -1,
			TempLen: 0,
			Seq:     NewSeq([]byte("TAGGC")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff},
			AuxFields: []Aux{
				mustAux(NewAux(NewTag("SA"), "ref,9,+,5S6M,30,1;")),
			},
		},
		{
			Name: "r001",
			Pos:  36,
			MapQ: 30,
			Cigar: Cigar{
				NewCigarOp(CigarMatch, 9),
			},
			Flags:   Paired | ProperPair | Reverse | Read2,
			MatePos: 6,
			TempLen: -39,
			Seq:     NewSeq([]byte("CAGCGGCAT")),
			Qual:    []uint8{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
			AuxFields: []Aux{
				mustAux(NewAux(NewTag("NM"), uint(1))),
			},
		},
	},
	cigars: []string{
		"8M2I4M1D3M",
		"3S6M1P1I4M",
		"5S6M",
		"6M14N5M",
		"6H5M",
		"9M",
	},
	// These coordinates are all open (and zero-based) so that
	// a slice of the reference doesn't need any alteration.
	readEnds: []int{
		22,
		18,
		14,
		40,
		33,
		45,
	},
}

var endTests = []struct {
	cigar Cigar
	end   int
}{
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 20),
			NewCigarOp(CigarBack, 5),
			NewCigarOp(CigarMatch, 20),
		},
		end: 35,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 10),
			NewCigarOp(CigarBack, 3),
			NewCigarOp(CigarMatch, 11),
		},
		end: 18,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarHardClipped, 10),
			NewCigarOp(CigarBack, 3),
		},
		end: 0,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarHardClipped, 10),
		},
		end: 3,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarSkipped, 10),
		},
		end: 13,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarMatch, 3),
		},
		end: 13,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarSoftClipped, 10),
			NewCigarOp(CigarHardClipped, 10),
		},
		end: 3,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarBack, 10),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarBack, 10),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarMatch, 3),
		},
		end: 3,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarBack, 10),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarBack, 5),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarMatch, 3),
		},
		end: 8,
	},
}

func (s *S) TestEnd(c *check.C) {
	for _, test := range endTests {
		c.Check((&Record{Cigar: test.cigar}).End(), check.Equals, test.end)
	}
}

var cigarTests = []struct {
	cigar  Cigar
	length int
	valid  bool
}{
	{
		cigar:  nil,
		length: 0,
		valid:  true,
	},

	// One thought is that if B is really intended only to provide the ability
	// to store CG and similar data where the read "skips" back a few bases now
	// and again vs. the reference one thing that would make this much easier
	// on those parsing SAM/BAM would be to limit the use of the B operator so
	// that it cannot skip backwards past the beginning of the read.
	//
	// So something like 20M5B20M would be valid, but 50M5000B20M would not be.
	//
	// http://sourceforge.net/p/samtools/mailman/message/28466477/
	{ // 20M5B20M
		cigar: Cigar{
			NewCigarOp(CigarMatch, 20),
			NewCigarOp(CigarBack, 5),
			NewCigarOp(CigarMatch, 20),
		},
		length: 40,
		valid:  true,
	},
	{ // 50M5000B20M
		cigar: Cigar{
			NewCigarOp(CigarMatch, 50),
			NewCigarOp(CigarBack, 5000),
			NewCigarOp(CigarMatch, 20),
		},
		length: 70,
		valid:  false,
	},

	// LH's example at http://sourceforge.net/p/samtools/mailman/message/28463294/
	{ // 10M3B11M
		// REF:: GCATACGATCGACTAGTCACGT
		// READ: --ATACGATCGA----------
		// READ: ---------CGACTAGTCAC--
		cigar: Cigar{
			NewCigarOp(CigarMatch, 10),
			NewCigarOp(CigarBack, 3),
			NewCigarOp(CigarMatch, 11),
		},
		length: 21,
		valid:  true,
	},

	{
		cigar: Cigar{
			NewCigarOp(CigarHardClipped, 10),
			NewCigarOp(CigarBack, 3),
			NewCigarOp(CigarMatch, 11),
		},
		length: 11,
		valid:  false,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarHardClipped, 10),
			NewCigarOp(CigarBack, 3),
		},
		length: 0,
		valid:  true,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarHardClipped, 10),
		},
		length: 3,
		valid:  true,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarHardClipped, 10),
			NewCigarOp(CigarHardClipped, 10),
		},
		length: 3,
		valid:  false,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarHardClipped, 10),
			NewCigarOp(CigarSoftClipped, 10),
		},
		length: 13,
		valid:  false,
	},
	{
		cigar: Cigar{
			NewCigarOp(CigarMatch, 3),
			NewCigarOp(CigarSoftClipped, 10),
			NewCigarOp(CigarHardClipped, 10),
		},
		length: 13,
		valid:  true,
	},

	// Stupid, but not reason not to be valid. We only care if the
	// there is a base from the query being used left of the start.
	{
		cigar: Cigar{
			NewCigarOp(CigarBack, 10),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarBack, 10),
			NewCigarOp(CigarSkipped, 10),
			NewCigarOp(CigarMatch, 3),
		},
		length: 3,
		valid:  true,
	},
}

func (s *S) TestCigarIsValid(c *check.C) {
	for _, test := range cigarTests {
		c.Check(test.cigar.IsValid(test.length), check.Equals, test.valid)
	}
}

func (s *S) TestNoHeader(c *check.C) {
	sam := []byte(`r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
`)

	sr, err := NewReader(bytes.NewReader(sam))
	c.Assert(err, check.Equals, nil)
	h := sr.Header()
	c.Check(h.Version, check.Equals, "")
	c.Check(h.SortOrder, check.Equals, UnknownOrder)
	c.Check(h.GroupOrder, check.Equals, GroupUnspecified)
	c.Check(h.Comments, check.DeepEquals, []string(nil))
	for {
		_, err := sr.Read()
		if err != nil {
			break
		}
	}
	refs := sr.Header().Refs()
	c.Assert(len(refs), check.Equals, 1)
	c.Check(refs[0].String(), check.Equals, "@SQ\tSN:ref\tLN:0")
}

func (s *S) TestIterator(c *check.C) {
	sam := [][]byte{
		[]byte(`r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
r005	4	ref	37	0	*	=	0	0	CAGCGGCAT	*
`),
		[]byte(`@HD	VN:1.5	SO:coordinate
@SQ	SN:ref	LN:45
@CO	--------------------------------------------------------
@CO	Coor     12345678901234  5678901234567890123456789012345
@CO	ref      AGCATGTTAGATAA**GATAGCTGTGCTAGTAGGCAGTCAGCGCCAT
@CO	--------------------------------------------------------
@CO	+r001/1        TTAGATAAAGGATA*CTG
@CO	+r002         aaaAGATAA*GGATA
@CO	+r003       gcctaAGCTAA
@CO	+r004                     ATAGCT..............TCAGC
@CO	-r003                            ttagctTAGGC
@CO	-r001/2                                        CAGCGGCAT
@CO	-r005     <unmapped>
@CO	--------------------------------------------------------
r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
r005	4	ref	37	0	*	=	0	0	CAGCGGCAT	*
`),
	}

	for _, s := range sam {
		sr, err := NewReader(bytes.NewReader(s))
		c.Assert(err, check.Equals, nil)
		i := NewIterator(sr)
		var n int
		for i.Next() {
			n++
		}
		c.Check(i.Error(), check.Equals, nil)
		c.Check(n, check.Equals, 7)
	}
}

var auxTests = []struct {
	sam string

	want []*Record
}{
	{
		sam: `1f001i8gk#GGCG#AA	0	*	0	0	*	*	0	0	*	*	NH:i:2	HI:i:1	AS:i:13	nM:i:4	NM:i:4	MD:Z:2C0T2T1C13	jM:B:c,-1	jI:B:i,-1
1f001i8gk#GGCG#AA	0	*	0	0	*	*	0	0	*	*	NH:i:2	HI:i:2	AS:i:12	nM:i:0	NM:i:0	MD:Z:22	jM:B:c,0	jI:B:i,629,1095	fT:f:3.14
1f001i8gk#GGCG#AA	0	*	0	0	*	*	0	0	*	*	NE:i:-100	MN:i:-1000
`,
		want: []*Record{
			{
				Name:    "1f001i8gk#GGCG#AA",
				Pos:     -1,
				MatePos: -1,
				AuxFields: AuxFields{
					{
						0x4e, 0x48, 0x43, 0x02, // |NHC.|
					},
					{
						0x48, 0x49, 0x43, 0x01, // |HIC.|
					},
					{
						0x41, 0x53, 0x43, 0x0d, // |ASC.|
					},
					{
						0x6e, 0x4d, 0x43, 0x04, // |nMC.|
					},
					{
						0x4e, 0x4d, 0x43, 0x04, // |NMC.|
					},
					{
						0x4d, 0x44, 0x5a, 0x32, 0x43, 0x30, 0x54, 0x32, 0x54, 0x31, 0x43, 0x31, 0x33, // |MDZ2C0T2T1C13|
					},
					{
						0x6a, 0x4d, 0x42, 0x63, 0x01, 0x00, 0x00, 0x00, 0xff, // |jMBc.....|
					},
					{
						0x6a, 0x49, 0x42, 0x69, 0x01, 0x00, 0x00, 0x00, 0xff, 0xff, 0xff, 0xff, // |jIBi........|
					},
				},
			},
			{
				Name:    "1f001i8gk#GGCG#AA",
				Pos:     -1,
				MatePos: -1,
				AuxFields: AuxFields{
					{
						0x4e, 0x48, 0x43, 0x02, // |NHC.|
					},
					{
						0x48, 0x49, 0x43, 0x02, // |HIC.|
					},
					{
						0x41, 0x53, 0x43, 0x0c, // |ASC.|
					},
					{
						0x6e, 0x4d, 0x43, 0x00, // |nMC.|
					},
					{
						0x4e, 0x4d, 0x43, 0x00, // |NMC.|
					},
					{
						0x4d, 0x44, 0x5a, 0x32, 0x32, // |MDZ22|
					},
					{
						0x6a, 0x4d, 0x42, 0x63, 0x01, 0x00, 0x00, 0x00, 0x00, // |jMBc.....|
					},
					{
						0x6a, 0x49, 0x42, 0x69, 0x02, 0x00, 0x00, 0x00, 0x75, 0x02, 0x00, 0x00, 0x47, 0x04, 0x00, 0x00, // |jIBi....u...G...|
					},
					{
						0x66, 0x54, 0x66, 0xc3, 0xf5, 0x48, 0x40, // |fTf..H@|
					},
				},
			},
			{
				Name:    "1f001i8gk#GGCG#AA",
				Pos:     -1,
				MatePos: -1,
				AuxFields: AuxFields{
					{
						0x4e, 0x45, 0x63, 0x9c, // |NEc.|
					},
					{
						0x4d, 0x4e, 0x73, 0x18, 0xfc, // |MNs..|
					},
				},
			},
		},
	},
}

func (s *S) TestAux(c *check.C) {
	for _, test := range auxTests {
		sr, err := NewReader(strings.NewReader(test.sam))
		c.Assert(err, check.Equals, nil)
		var recs []*Record
		for {
			r, err := sr.Read()
			if err != nil {
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			recs = append(recs, r)
		}
		c.Check(recs, check.DeepEquals, test.want)
	}
}

func (s *S) TestIssue26(c *check.C) {
	fuTag := NewTag("fu")

	var issue26 = struct {
		data   []byte
		header Header
		ref    Reference
		rg     ReadGroup
		prog   Program
	}{
		// This is a Pacific Biosciences header line. The SO is invalid.
		data: []byte(`@HD	VN:1.5	SO:UNKNOWN	pb:3.0b7
@SQ	SN:ref	LN:45	fu:bar
@RG	ID:group	fu:bar
@PG	ID:program	fu:bar
`),
		header: Header{
			Version:    "1.5",
			SortOrder:  UnknownOrder,
			GroupOrder: GroupUnspecified,
		},
		ref: Reference{
			id:        -1,
			name:      "ref",
			lRef:      45,
			otherTags: []tagPair{{tag: fuTag, value: "bar"}},
		},
		rg: ReadGroup{
			id:        -1,
			name:      "group",
			otherTags: []tagPair{{tag: fuTag, value: "bar"}},
		},
		prog: Program{
			id:        -1,
			uid:       "program",
			otherTags: []tagPair{{tag: fuTag, value: "bar"}},
		},
	}

	sr, err := NewReader(bytes.NewReader(issue26.data))
	c.Assert(err, check.Equals, nil)
	h := sr.Header()

	c.Check(h.Version, check.Equals, issue26.header.Version)
	c.Check(h.SortOrder, check.Equals, issue26.header.SortOrder)
	c.Check(h.GroupOrder, check.Equals, issue26.header.GroupOrder)
	c.Assert(len(h.Refs()), check.Equals, 1)
	ref := h.Refs()[0].Clone()
	c.Check(equalRefs(ref, &issue26.ref), check.Equals, true)
	c.Check(ref.Get(refNameTag), check.Equals, "ref")
	c.Check(ref.Get(refLengthTag), check.Equals, "45")
	c.Check(ref.Get(fuTag), check.Equals, "bar")
	c.Assert(len(h.RGs()), check.Equals, 1)
	rg := h.RGs()[0].Clone()
	c.Check(*rg, check.DeepEquals, issue26.rg)
	c.Check(rg.Get(idTag), check.Equals, "group")
	c.Check(rg.Get(fuTag), check.Equals, "bar")
	c.Assert(len(h.Progs()), check.Equals, 1)
	prog := h.Progs()[0].Clone()
	c.Check(*prog, check.DeepEquals, issue26.prog)
	c.Check(prog.Get(idTag), check.Equals, "program")
	c.Check(prog.Get(fuTag), check.Equals, "bar")
}

func (s *S) TestParseISO8601(c *check.C) {
	for _, test := range []struct {
		value string
		want  time.Time
	}{
		{value: "2017-05-10", want: time.Date(2017, 05, 10, 0, 0, 0, 0, time.Local)},
		{value: "2017-05-10T21:02:29", want: time.Date(2017, 05, 10, 21, 02, 29, 0, time.Local)},
		{value: "2017-05-10T21:02:29Z", want: time.Date(2017, 05, 10, 21, 02, 29, 0, time.UTC)},
		{value: "2017-05-10T21:02:29+0900", want: time.Date(2017, 05, 10, 21, 02, 29, 0, time.FixedZone("0900", 9*3600))},
		{value: "2010-10-19T00:00:00.000+00:00", want: time.Date(2010, 10, 19, 0, 0, 0, 0, time.FixedZone("0000", 0))},
	} {
		date, err := parseISO8601(test.value)
		c.Check(err, check.Equals, nil)
		c.Check(date.Equal(test.want), check.Equals, true)
	}
}

var cigTests = []struct {
	cig  []byte
	ref  int
	read int
}{
	{[]byte("151M"), 151, 151},
	{[]byte("10S10M"), 10, 20},
	{[]byte("11H11M"), 11, 11},
	{[]byte("11H1D11M"), 12, 11},
	{[]byte("5M21N5M"), 31, 10},
	{[]byte("21N"), 21, 0},
	{[]byte("0M1I1D"), 1, 1},
	{[]byte("1000000000M"), 1e9, 1e9},
	{[]byte("1000000000000M"), 1e12, 1e12},
}

func (s *S) TestLengths(c *check.C) {
	for _, ct := range cigTests {
		cig, err := ParseCigar(ct.cig)
		c.Check(err, check.IsNil)
		ref, read := cig.Lengths()
		c.Check(ref, check.Equals, ct.ref)
		c.Check(read, check.Equals, ct.read)
	}
}

func (s *S) TestIssue32(c *check.C) {
	sam := []byte(`@HD	VN:1.5	SO:coordinate
@SQ	SN:name	LN:1
@RG	ID:name
@PG	ID:name
`)

	r, err := NewReader(bytes.NewReader(sam))
	c.Check(err, check.Equals, nil)
	h := r.Header()
	c.Assert(len(h.Refs()), check.Equals, 1)
	c.Check(h.Refs()[0].Name(), check.Equals, "name")
	c.Assert(len(h.RGs()), check.Equals, 1)
	c.Check(h.RGs()[0].Name(), check.Equals, "name")
	c.Assert(len(h.Progs()), check.Equals, 1)
	c.Check(h.Progs()[0].UID(), check.Equals, "name")
}

func (s *S) TestIssue133(c *check.C) {
	sam := []byte(`@HD	VN:1.5	SO:coordinate
@SQ	SN:q2	LN:1000
q3	0	q2	4	7	1S13M2I18M	*	0	0	TCAGGAAGGCGGTATCGACGACGGTGCCGAGGCA	*	PG:i:0	NM:i:0
`)

	r, err := NewReader(bytes.NewReader(sam))
	c.Check(err, check.Equals, nil)
	rec, err := r.Read()
	c.Check(err, check.Equals, nil)
	c.Check(IsValidRecord(rec), check.Equals, true)
}

func (s *S) TestEqualRefs(c *check.C) {
	a, err := NewReference("aaa", "assem", "species", 1234, nil, nil)
	c.Assert(err, check.IsNil)
	b := a.Clone()
	c.Assert(b.Set(Tag{'T', 'A'}, "xxx"), check.IsNil)

	c.Assert(equalRefs(a, b), check.Equals, false)
	c.Assert(equalRefs(b, a), check.Equals, false)
}

func (s *S) TestAddClonedRef(c *check.C) {
	sr, err := NewReader(bytes.NewReader(specExamples.data))
	c.Assert(err, check.Equals, nil)
	h := sr.Header()
	ref := h.Refs()[0].Clone()
	c.Check(h.AddReference(ref), check.Equals, nil)
}

func (s *S) TestRenames(c *check.C) {
	sam := []byte(`@HD	VN:1.5	SO:coordinate
@SQ	SN:name	LN:1
@SQ	SN:taken	LN:1
@RG	ID:name
@RG	ID:taken
@PG	ID:name
@PG	ID:taken
`)

	r, err := NewReader(bytes.NewReader(sam))
	c.Check(err, check.Equals, nil)
	h := r.Header()
	c.Assert(len(h.Refs()), check.Equals, 2)
	c.Assert(h.Refs()[0].SetName("reference"), check.Equals, nil)
	c.Check(h.Refs()[0].Name(), check.Equals, "reference")
	c.Check(h.Refs()[0].SetName("taken"), check.DeepEquals, errors.New("sam: name exists"))

	c.Assert(len(h.RGs()), check.Equals, 2)
	c.Assert(h.RGs()[0].SetName("read group"), check.Equals, nil)
	c.Check(h.RGs()[0].Name(), check.Equals, "read group")
	c.Check(h.RGs()[0].SetName("taken"), check.DeepEquals, errors.New("sam: name exists"))

	c.Assert(len(h.Progs()), check.Equals, 2)
	c.Assert(h.Progs()[0].SetUID("program"), check.Equals, nil)
	c.Check(h.Progs()[0].UID(), check.Equals, "program")
	c.Check(h.Progs()[0].SetUID("taken"), check.DeepEquals, errors.New("sam: uid exists"))

	ref, err := NewReference("ref", "", "", 1, nil, nil)
	c.Assert(err, check.Equals, nil)
	c.Assert(ref.SetName("new ref"), check.Equals, nil)

	rg, err := NewReadGroup("rg", "", "", "", "", "", "", "", "", "", time.Time{}, 0)
	c.Assert(err, check.Equals, nil)
	c.Assert(rg.SetName("new rg"), check.Equals, nil)

	prog := NewProgram("prog", "", "", "", "")
	c.Assert(prog.SetUID("new prog"), check.Equals, nil)
}

func (s *S) TestSort(c *check.C) {
	sr, err := NewReader(bytes.NewReader(specExamples.data))
	c.Assert(err, check.Equals, nil)
	i := NewIterator(sr)
	var recs []*Record
	for i.Next() {
		recs = append(recs, i.Record())
	}
	c.Assert(i.Error(), check.Equals, nil)
	recs = append(recs, &Record{Name: "000", Ref: nil, Pos: -1})

	wantPos := []int{6, 8, 8, 15, 28, 36, -1}
	sort.Sort(byCoordinate(recs))
	for i, r := range recs {
		c.Check(r.Pos, check.Equals, wantPos[i])
	}

	wantName := []string{"000", "r001", "r001", "r002", "r003", "r003", "r004"}
	sort.Sort(byName(recs))
	for i, r := range recs {
		c.Check(r.Name, check.Equals, wantName[i])
	}
}

type byName []*Record

func (r byName) Len() int           { return len(r) }
func (r byName) Less(i, j int) bool { return r[i].LessByName(r[j]) }
func (r byName) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type byCoordinate []*Record

func (r byCoordinate) Len() int           { return len(r) }
func (r byCoordinate) Less(i, j int) bool { return r[i].LessByCoordinate(r[j]) }
func (r byCoordinate) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func (s *S) TestRemoveReference(c *check.C) {
	h := headerHG00096_1000.Clone()
	h.RemoveReference(h.Refs()[2])
	c.Check(len(h.Refs()), check.Equals, len(headerHG00096_1000.Refs())-1)
	c.Check(fmt.Sprint(h.Refs()[1]), check.Equals, fmt.Sprint(headerHG00096_1000.Refs()[1]))
	c.Check(fmt.Sprint(h.Refs()[2]), check.Equals, fmt.Sprint(headerHG00096_1000.Refs()[3]))
	r := headerHG00096_1000.Refs()[2].Clone()
	c.Check(h.AddReference(r), check.Equals, nil)
	c.Check(len(h.Refs()), check.Equals, len(headerHG00096_1000.Refs()))
}

func (s *S) TestRemoveReadGroup(c *check.C) {
	h := headerHG00096_1000.Clone()
	h.RemoveReadGroup(h.RGs()[1])
	c.Check(len(h.RGs()), check.Equals, len(headerHG00096_1000.RGs())-1)
	c.Check(fmt.Sprint(h.RGs()[0]), check.Equals, fmt.Sprint(headerHG00096_1000.RGs()[0]))
	c.Check(fmt.Sprint(h.RGs()[1]), check.Equals, fmt.Sprint(headerHG00096_1000.RGs()[2]))
	r := headerHG00096_1000.RGs()[1].Clone()
	c.Check(h.AddReadGroup(r), check.Equals, nil)
	c.Check(len(h.RGs()), check.Equals, len(headerHG00096_1000.RGs()))
}

func (s *S) TestRemoveProgram(c *check.C) {
	h := headerHG00096_1000.Clone()
	h.RemoveProgram(h.Progs()[2])
	c.Check(len(h.Progs()), check.Equals, len(headerHG00096_1000.Progs())-1)
	c.Check(fmt.Sprint(h.Progs()[1]), check.Equals, fmt.Sprint(headerHG00096_1000.Progs()[1]))
	c.Check(fmt.Sprint(h.Progs()[2]), check.Equals, fmt.Sprint(headerHG00096_1000.Progs()[3]))
	p := headerHG00096_1000.Progs()[2].Clone()
	c.Check(h.AddProgram(p), check.Equals, nil)
	c.Check(len(h.Progs()), check.Equals, len(headerHG00096_1000.Progs()))
}

func (s *S) TestMergeHeaders(c *check.C) {
	tests := []struct {
		src   []*Header
		want  *Header
		links [][]*Reference
		err   error
	}{
		{},
		{
			src:   []*Header{{}},
			links: nil,
			want:  &Header{},
			err:   nil,
		},
		{
			src: []*Header{
				{refs: []*Reference{{id: 0, name: "ref", lRef: 45}}, seenRefs: set{"ref": 0}, seenGroups: set{}, seenProgs: set{}},
				{refs: []*Reference{{id: 0, name: "ref", lRef: 45}}, seenRefs: set{"ref": 0}, seenGroups: set{}, seenProgs: set{}},
			},
			links: [][]*Reference{
				{{id: 0, name: "ref", lRef: 45}},
				{{id: 0, name: "ref", lRef: 45}},
			},
			want: &Header{
				refs:       []*Reference{{id: 0, name: "ref", lRef: 45}},
				seenRefs:   set{"ref": 0},
				seenGroups: set{},
				seenProgs:  set{},
			},
			err: nil,
		},
		{
			src: []*Header{
				{refs: []*Reference{{id: 0, name: "refa", lRef: 45}}, seenRefs: set{"refa": 0}, seenGroups: set{}, seenProgs: set{}},
				{refs: []*Reference{{id: 0, name: "refb", lRef: 45}}, seenRefs: set{"refb": 0}, seenGroups: set{}, seenProgs: set{}},
			},
			links: [][]*Reference{
				{{id: 0, name: "refa", lRef: 45}},
				{{id: 1, name: "refb", lRef: 45}},
			},
			want: &Header{
				refs: []*Reference{
					{id: 0, name: "refa", lRef: 45},
					{id: 1, name: "refb", lRef: 45},
				},
				seenRefs:   set{"refa": 0, "refb": 1},
				seenGroups: set{},
				seenProgs:  set{},
			},
			err: nil,
		},
		{
			src: []*Header{
				{refs: []*Reference{{id: 0, name: "ref", lRef: 45}}, seenRefs: set{"ref": 0}, seenGroups: set{}, seenProgs: set{}},
				{refs: []*Reference{{id: 0, name: "ref", lRef: 44}}, seenRefs: set{"ref": 0}, seenGroups: set{}, seenProgs: set{}},
			},
			links: nil,
			want:  nil,
			err:   errors.New("sam: duplicate reference name"),
		},
	}
	for _, test := range tests[3:] {
		// Prepare the internal links that cannot be expressed statically.
		if len(test.src) != 0 {
			for _, r := range test.src[0].refs {
				r.owner = test.src[0]
			}
		}
		for _, in := range test.links {
			for _, ref := range in {
				ref.owner = test.want
			}
		}
		if test.want != nil {
			for _, r := range test.want.refs {
				r.owner = test.want
			}
		}

		// Set up for identical input case.
		var identical bool
		if len(test.src) == 2 {
			identical = reflect.DeepEqual(test.src[0], test.src[1])
		}

		got, links, err := MergeHeaders(test.src)
		c.Check(err, check.DeepEquals, test.err)
		if err != nil {
			continue
		}
		c.Check(got, check.DeepEquals, test.want)
		c.Check(links, check.DeepEquals, test.links)
		if identical {
			c.Check(links[0][0], check.Equals, links[1][0])
		}
	}
}

func BenchmarkParseCigar(b *testing.B) {
	cig := []byte("69S17M5I30M1D45M1D23M5I14M2I4M1I10M2D7M1D6M14I33M1D6M1I7M1I18M1I8M1D4M1D4M2D57M1D21M1D6M1I14M1I7M1I3M1I9M1D3M1D7M1D37M1D9M1I5M1I15M4I12M1D10M1I10M1D8M1D26M7I12M1D20M1I36M1I22M3D8M1I23M1I13M2D10M1D12M1I15M6D4M1D4M1D1M2D5M1D3M17D1M1D13M3D7M1I29M2I9M1D2M4D7M2D8M5D3M1D1M1D23M1D10M6D19M3I24M1D8M1I11M6D14M1I5M8I12M1D8M2D5M2D2M1D23M1D11M4I35M2I19M1I4M1D13M7I33M1D21M3D2M1D9M4I19M1I14M1D7M1I41M1D23M3I18M1I6M1I13M1D9M1D1M1D20M1D23M5D8M1I13M2I11M1D78M2I18M10D9M2D10M1D10M2I6M1D3M1D21M2I7M1D7M2I12M1D20M2D18M1I12M1D8M4D18M1D6M1D20M1D14M1I1M2I23M1I10M1D7M1I15M1D4M1I9M1D11M1D12M1I8M1D21M1I13M2I59M1D12M1D18M1D13M1D22M1D13M1I19M1D13M1D19M1I11M2I27M2D10M1D17M6D13M2D17M1D13M1D19M1I3M1D13M2I33M1I26M2D9M2I21M2D10M1D36M1D32M5I23M1D13M2D17M1I14M2I24M1I5M2I8M2I24M2I9M1D7M1D2M1D15M3I19M1I2M1D3M1I7M1D5M2D24M5I1M4I33M1I13M3I34M1I2M1I23M1D3M2I8M1I5M5S")
	for i := 0; i < b.N; i++ {
		_, err := ParseCigar(cig)
		if err != nil {
			panic(err)
		}
	}
}

func benchmarkAux(b *testing.B, aux []byte) {
	for i := 0; i < b.N; i++ {
		_, err := ParseAux(aux)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParseAuxInt(b *testing.B)   { benchmarkAux(b, []byte("NM:i:1")) }
func BenchmarkParseAuxZ(b *testing.B)     { benchmarkAux(b, []byte("SA:Z:ref,29,-,6H5M,17,0;")) }
func BenchmarkParseAuxFloat(b *testing.B) { benchmarkAux(b, []byte("FL:f:100042.42")) }
func BenchmarkParseAuxArray(b *testing.B) { benchmarkAux(b, []byte("BB:B:i,629,1095")) }
