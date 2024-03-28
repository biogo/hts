// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fai

import (
	"bytes"
	"encoding/csv"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestNewIndex(t *testing.T) {
	for i, test := range []struct {
		in  string
		idx Index
		err error
	}{
		{
			in:  ``,
			idx: Index{},
			err: nil,
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTC
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.2
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			// Index validated against samtools faidx for this set of sequences.
			idx: Index{
				"FN654386.1": Record{Name: "FN654386.1", Length: 100, Start: 12, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.2": Record{Name: "FN654386.2", Length: 100, Start: 126, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.3": Record{Name: "FN654386.3", Length: 100, Start: 240, BasesPerLine: 70, BytesPerLine: 71},
			},
			err: nil,
		},
		{
			// Index validated against samtools faidx for this set of sequences.
			in: `>FN654386.1	descriptive text separated by tab
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTC
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.2 descriptive text separated by space
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			// Index validated against samtools faidx for this set of sequences.
			idx: Index{
				"FN654386.1": Record{Name: "FN654386.1", Length: 100, Start: 46, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.2": Record{Name: "FN654386.2", Length: 100, Start: 196, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.3": Record{Name: "FN654386.3", Length: 100, Start: 310, BasesPerLine: 70, BytesPerLine: 71},
			},
			err: nil,
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAA
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.2
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			// Index validated against samtools faidx for this set of sequences.
			idx: Index{
				"FN654386.1": Record{Name: "FN654386.1", Length: 90, Start: 12, BasesPerLine: 60, BytesPerLine: 61},
				"FN654386.2": Record{Name: "FN654386.2", Length: 100, Start: 116, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.3": Record{Name: "FN654386.3", Length: 100, Start: 230, BasesPerLine: 70, BytesPerLine: 71},
			},
			err: nil,
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAA
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			idx: nil,
			err: errors.New("fai: missing sequence name at 104"),
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAA
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>` + "    " + `
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			idx: nil,
			err: errors.New("fai: missing sequence name at 104"),
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTC
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.1
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			idx: nil,
			err: errors.New("fai: duplicate sequence identifier FN654386.1 at 114"),
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTC
GAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTCTTTTTCAAAGACGTTA
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.2
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			idx: nil,
			err: errors.New("fai: unexpected short line before offset 153"),
		},
		{
			in: `>FN654386.1
TTTTTCAAAGACGTTAAGAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTC
GAGCATCAAACAGAATCATTTTGTTCTCGGATGAGAAGCTGAAAACGAGATTCTTTTTCAAAGACGTTACG
TCGTGTTGCTTCTCGGTCATACCAAAGACC
>FN654386.2
GCGAACTGATGGTCAAGCACAGCTAGAACGTCTTCGTTTGAAGCTGGAGACGATTGCCGCGCAGGCAACT
CGCCGACATCGACGATGTCTTCGTAGTCAT
>FN654386.3
CTGAATTGAGCCGCGGGCGATTGATTCGTGTCGGCGCGTCAGGAGGAAGTTCAAGTCGGAATCTCGCGTT
TTCATTAATCATTTGTACTGGATCTGTTCG
`,
			idx: nil,
			err: errors.New("fai: unexpected long line at offset 83"),
		},
	} {
		got, err := NewIndex(strings.NewReader(test.in))
		if !reflect.DeepEqual(err, test.err) {
			t.Errorf("unexpected error: got:%v want:%v", err, test.err)
		}
		if !reflect.DeepEqual(got, test.idx) {
			t.Errorf("unexpected result for test %d: got:%#v want:%#v", i, got, test.idx)
		}
	}
}

func TestReadFrom(t *testing.T) {
	for i, test := range []struct {
		in  string
		idx Index
		err error
	}{
		{
			in:  ``,
			idx: nil,
			err: nil,
		},
		{
			in: `NODE_7194_length_226_cov_2.672566	246	35	60	61
NODE_7195_length_193_cov_2.906736	213	321	60	61
NODE_7419_length_181_cov_4.668508	201	573	60	61
NODE_7473_length_222_cov_10.977477	242	814	60	61
NODE_11804_length_273_cov_2.186813	293	1097	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_19170_length_305_cov_2.147541	325	1689	60	61
NODE_23972_length_201_cov_2.452736	221	2056	60	61
NODE_25171_length_223_cov_2.869955	243	2317	60	61
NODE_26170_length_196_cov_2.658163	216	2601	60	61
NODE_28488_length_231_cov_2.290043	251	2857	60	61
NODE_29471_length_195_cov_5.102564	215	3149	60	61
NODE_30404_length_252_cov_1.480159	272	3404	60	61
NODE_30635_length_192_cov_2.947917	212	3717	60	61
NODE_34404_length_184_cov_7.989130	204	3969	60	61
NODE_36516_length_195_cov_3.517949	215	4213	60	61
NODE_41230_length_277_cov_3.498195	297	4468	60	61
NODE_42422_length_182_cov_2.609890	202	4806	60	61
NODE_43724_length_236_cov_3.500000	256	5048	60	61
NODE_44676_length_185_cov_1.421622	205	5345	60	61
NODE_53327_length_192_cov_1.854167	212	5590	60	61
`,
			idx: Index{
				"NODE_7194_length_226_cov_2.672566":  Record{Name: "NODE_7194_length_226_cov_2.672566", Length: 246, Start: 35, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_7195_length_193_cov_2.906736":  Record{Name: "NODE_7195_length_193_cov_2.906736", Length: 213, Start: 321, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_7419_length_181_cov_4.668508":  Record{Name: "NODE_7419_length_181_cov_4.668508", Length: 201, Start: 573, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_7473_length_222_cov_10.977477": Record{Name: "NODE_7473_length_222_cov_10.977477", Length: 242, Start: 814, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_11804_length_273_cov_2.186813": Record{Name: "NODE_11804_length_273_cov_2.186813", Length: 293, Start: 1097, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_12878_length_198_cov_2.358586": Record{Name: "NODE_12878_length_198_cov_2.358586", Length: 218, Start: 1431, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_19170_length_305_cov_2.147541": Record{Name: "NODE_19170_length_305_cov_2.147541", Length: 325, Start: 1689, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_23972_length_201_cov_2.452736": Record{Name: "NODE_23972_length_201_cov_2.452736", Length: 221, Start: 2056, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_25171_length_223_cov_2.869955": Record{Name: "NODE_25171_length_223_cov_2.869955", Length: 243, Start: 2317, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_26170_length_196_cov_2.658163": Record{Name: "NODE_26170_length_196_cov_2.658163", Length: 216, Start: 2601, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_28488_length_231_cov_2.290043": Record{Name: "NODE_28488_length_231_cov_2.290043", Length: 251, Start: 2857, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_29471_length_195_cov_5.102564": Record{Name: "NODE_29471_length_195_cov_5.102564", Length: 215, Start: 3149, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_30404_length_252_cov_1.480159": Record{Name: "NODE_30404_length_252_cov_1.480159", Length: 272, Start: 3404, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_30635_length_192_cov_2.947917": Record{Name: "NODE_30635_length_192_cov_2.947917", Length: 212, Start: 3717, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_34404_length_184_cov_7.989130": Record{Name: "NODE_34404_length_184_cov_7.989130", Length: 204, Start: 3969, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_36516_length_195_cov_3.517949": Record{Name: "NODE_36516_length_195_cov_3.517949", Length: 215, Start: 4213, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_41230_length_277_cov_3.498195": Record{Name: "NODE_41230_length_277_cov_3.498195", Length: 297, Start: 4468, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_42422_length_182_cov_2.609890": Record{Name: "NODE_42422_length_182_cov_2.609890", Length: 202, Start: 4806, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_43724_length_236_cov_3.500000": Record{Name: "NODE_43724_length_236_cov_3.500000", Length: 256, Start: 5048, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_44676_length_185_cov_1.421622": Record{Name: "NODE_44676_length_185_cov_1.421622", Length: 205, Start: 5345, BasesPerLine: 60, BytesPerLine: 61},
				"NODE_53327_length_192_cov_1.854167": Record{Name: "NODE_53327_length_192_cov_1.854167", Length: 212, Start: 5590, BasesPerLine: 60, BytesPerLine: 61},
			},
			err: nil,
		},
		{
			in: `NODE_7194_length_226_cov_2.672566	246	35	60	61
NODE_7195_length_193_cov_2.906736	213	321	60	61
NODE_7419_length_181_cov_4.668508	201	573	60	61
NODE_7473_length_222_cov_10.977477	242	814	60	61
NODE_11804_length_273_cov_2.186813	293	1097	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_19170_length_305_cov_2.147541	325	1689	60	61
NODE_23972_length_201_cov_2`,
			idx: nil,
			err: parseError(8, 1, csv.ErrFieldCount),
		},
		{
			in: `NODE_7194_length_226_cov_2.672566	246	35	60	61
NODE_7195_length_193_cov_2.906736	213	321	60	61
NODE_7419_length_181_cov_4.668508	201	573	60	61
NODE_7473_length_222_cov_10.977477	242	814	60	61
NODE_11804_length_273_cov_2.186813	293	1097	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_19170_length_305_cov_2.147541	325	1689	60	61
NODE_23972_length_201_cov_2.452736	221	2056	60	61
NODE_25171_length_223_cov_2.869955	243	2317	60	61
NODE_26170_length_196_cov_2.658163	216	2601	60	61
NODE_28488_length_231_cov_2.290043	251	2857	60	61
NODE_29471_length_195_cov_5.102564	215	3149	60	61
NODE_30404_length_252_cov_1.480159	272	3404	60	61` + "\t" + `
NODE_30635_length_192_cov_2.947917	212	3717	60	61
NODE_34404_length_184_cov_7.989130	204	3969	60	61
NODE_36516_length_195_cov_3.517949	215	4213	60	61
NODE_41230_length_277_cov_3.498195	297	4468	60	61
NODE_42422_length_182_cov_2.609890	202	4806	60	61
NODE_43724_length_236_cov_3.500000	256	5048	60	61
NODE_44676_length_185_cov_1.421622	205	5345	60	61
NODE_53327_length_192_cov_1.854167	212	5590	60	61
`,
			idx: nil,
			err: parseError(13, 1, csv.ErrFieldCount),
		},
		{
			in: `NODE_7194_length_226_cov_2.672566	246	35	60	61
NODE_7195_length_193_cov_2.906736	213	321	60	61
NODE_7419_length_181_cov_4.668508	201	573	60	61
NODE_7473_length_222_cov_10.977477	242	814	60	61
NODE_11804_length_273_cov_2.186813	293	1097	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_19170_length_305_cov_2.147541	325	1689	60	61
NODE_23972_length_201_cov_2.452736	221	2056	60	61
NODE_25171_length_223_cov_2.869955	243	2317	60	61
NODE_26170_length_196_cov_2.658163	216	2601	60	61
NODE_28488_length_231_cov_2.290043	251	2857	60	61
NODE_29471_length_195_cov_5.102564	215	3149	60	61
NODE_30404_length_252_cov_1.480159	272	3404	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_34404_length_184_cov_7.989130	204	3969	60	61
NODE_36516_length_195_cov_3.517949	215	4213	60	61
NODE_41230_length_277_cov_3.498195	297	4468	60	61
NODE_42422_length_182_cov_2.609890	202	4806	60	61
NODE_43724_length_236_cov_3.500000	256	5048	60	61
NODE_44676_length_185_cov_1.421622	205	5345	60	61
NODE_53327_length_192_cov_1.854167	212	5590	60	61
`,
			idx: nil,
			err: parseError(14, 0, ErrNonUnique),
		},
		{
			in: `NODE_7194_length_226_cov_2.672566	246	35	60	61
NODE_7195_length_193_cov_2.906736	213	321	60	61
NODE_7419_length_181_cov_4.668508	201	573	60	61
NODE_7473_length_222_cov_10.977477	242	814	60	61
NODE_11804_length_273_cov_2.186813	293	1097	60	61
NODE_12878_length_198_cov_2.358586	218	1431	60	61
NODE_19170_length_305_cov_2.147541	325	1689	60	61
NODE_23972_length_201_cov_2.452736	221	2056	sixty	61
NODE_25171_length_223_cov_2.869955	243	2317	60	61
NODE_26170_length_196_cov_2.658163	216	2601	60	61
NODE_28488_length_231_cov_2.290043	251	2857	60	61
NODE_29471_length_195_cov_5.102564	215	3149	60	61
NODE_30404_length_252_cov_1.480159	272	3404	60	61
NODE_30635_length_192_cov_2.947917	212	3717	60	61
NODE_34404_length_184_cov_7.989130	204	3969	60	61
NODE_36516_length_195_cov_3.517949	215	4213	60	61
NODE_41230_length_277_cov_3.498195	297	4468	60	61
NODE_42422_length_182_cov_2.609890	202	4806	60	61
NODE_43724_length_236_cov_3.500000	256	5048	60	61
NODE_44676_length_185_cov_1.421622	205	5345	60	61
NODE_53327_length_192_cov_1.854167	212	5590	60	61
`,
			idx: nil,
			err: parseError(8, 3, &strconv.NumError{
				Func: "ParseInt",
				Num:  "sixty",
				Err:  strconv.ErrSyntax,
			}),
		},
	} {
		got, err := ReadFrom(strings.NewReader(test.in))
		if !reflect.DeepEqual(err, test.err) {
			t.Errorf("unexpected error for test %d: got:%#v want:%#v", i, err, test.err)
		}
		if !reflect.DeepEqual(got, test.idx) {
			t.Errorf("unexpected result for test %d: got:%#v want:%#v", i, got, test.idx)
		}
	}
}

func TestWriteTo(t *testing.T) {
	for i, test := range []struct {
		idx  Index
		want string
	}{
		{
			idx: Index{
				"FN654386.1": Record{Name: "FN654386.1", Length: 100, Start: 12, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.2": Record{Name: "FN654386.2", Length: 100, Start: 126, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.3": Record{Name: "FN654386.3", Length: 100, Start: 240, BasesPerLine: 70, BytesPerLine: 71},
			},
			want: `FN654386.1	100	12	70	71
FN654386.2	100	126	70	71
FN654386.3	100	240	70	71
`,
		},
		{
			// Map iteration order is stated to be unspecified, but in practice,
			// maps that are shorter than one bucket have consistent order, so
			// provide a different ordering.
			idx: Index{
				"FN654386.2": Record{Name: "FN654386.2", Length: 100, Start: 126, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.1": Record{Name: "FN654386.1", Length: 100, Start: 12, BasesPerLine: 70, BytesPerLine: 71},
				"FN654386.3": Record{Name: "FN654386.3", Length: 100, Start: 240, BasesPerLine: 70, BytesPerLine: 71},
			},
			want: `FN654386.1	100	12	70	71
FN654386.2	100	126	70	71
FN654386.3	100	240	70	71
`,
		},
	} {
		var buf bytes.Buffer
		err := WriteTo(&buf, test.idx)
		if err != nil {
			t.Errorf("unexpected error: got:%v", err)
		}
		got := buf.String()
		if got != test.want {
			t.Errorf("unexpected result for test %d: got:\n%s\nwant:%s", i, got, test.want)
		}
	}
}
