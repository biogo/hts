// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"flag"
	"io"
	"strings"
	"testing"

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
@CO	--------------------------------------------------------
r001	99	ref	7	30	8M2I4M1D3M	=	37	39	TTAGATAAAGGATACTG	*
r002	0	ref	9	30	3S6M1P1I4M	*	0	0	AAAAGATAAGGATA	*
r003	0	ref	9	30	5S6M	*	0	0	GCCTAAGCTAA	*	SA:Z:ref,29,-,6H5M,17,0;
r004	0	ref	16	30	6M14N5M	*	0	0	ATAGCTTCAGC	*
r003	2064	ref	29	17	6H5M	*	0	0	TAGGC	*	SA:Z:ref,9,+,5S6M,30,1;
r001	147	ref	37	30	9M	=	7	-39	CAGCGGCAT	*	NM:i:1
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
		c.Check(n, check.Equals, 6)
	}
}

var auxTests = []struct {
	sam string

	want []*Record
}{
	{
		sam: `1f001i8gk#GGCG#AA	0	*	0	0	*	*	0	0	*	*	NH:i:2	HI:i:1	AS:i:13	nM:i:4	NM:i:4	MD:Z:2C0T2T1C13	jM:B:c,-1	jI:B:i,-1
1f001i8gk#GGCG#AA	0	*	0	0	*	*	0	0	*	*	NH:i:2	HI:i:2	AS:i:12	nM:i:0	NM:i:0	MD:Z:22	jM:B:c,0	jI:B:i,629,1095
`,
		want: []*Record{
			{
				Name:    "1f001i8gk#GGCG#AA",
				Pos:     -1,
				MatePos: -1,
				AuxFields: AuxFields{
					mustAux(NewAux(NewTag("NH"), uint(2))),
					mustAux(NewAux(NewTag("HI"), uint(1))),
					mustAux(NewAux(NewTag("AS"), uint(13))),
					mustAux(NewAux(NewTag("nM"), uint(4))),
					mustAux(NewAux(NewTag("NM"), uint(4))),
					mustAux(NewAux(NewTag("MD"), "2C0T2T1C13")),
					mustAux(NewAux(NewTag("jM"), []int8{-1})),
					mustAux(NewAux(NewTag("jI"), []int32{-1})),
				},
			},
			{
				Name:    "1f001i8gk#GGCG#AA",
				Pos:     -1,
				MatePos: -1,
				AuxFields: AuxFields{
					mustAux(NewAux(NewTag("NH"), uint(2))),
					mustAux(NewAux(NewTag("HI"), uint(2))),
					mustAux(NewAux(NewTag("AS"), uint(12))),
					mustAux(NewAux(NewTag("nM"), uint(0))),
					mustAux(NewAux(NewTag("NM"), uint(0))),
					mustAux(NewAux(NewTag("MD"), "22")),
					mustAux(NewAux(NewTag("jM"), []int8{0})),
					mustAux(NewAux(NewTag("jI"), []int32{629, 1095})),
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
