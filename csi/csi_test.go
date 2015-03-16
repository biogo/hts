// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package csi

import (
	"bytes"
	"testing"

	"github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/index"

	"gopkg.in/check.v1"
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

// conceptualCSIv1data is an uncompressed CSIv1 for the alignments in the BAM
// corresponding to:
//
// @HD	VN:1.0	SO:coordinate
// @SQ	SN:conceptual	LN:134217728
// 60m66m:bin0	0	conceptual	62914561	40	6291456M	*	0	0	*	*
// 70m76m:bin2	0	conceptual	73400321	40	6291456M	*	0	0	*	*
// 73m75m:bin18	0	conceptual	76546049	40	2097152M	*	0	0	*	*
//
// This is a coordinate-translated version of the conceptual example in the
// SAM spec using binning as actually used by BAM rather than as presented.
/*
	0x43, 0x53, 0x49, 0x01, // Index\1
	0x0e, 0x00, 0x00, 0x00, // min_shift
	0x05, 0x00, 0x00, 0x00, // depth
	0x00, 0x00, 0x00, 0x00, // l_aux

	// no aux

	0x01, 0x00, 0x00, 0x00, // n_ref

		0x02, 0x00, 0x00, 0x00, // n_bin

			0x00, 0x00, 0x00, 0x00, // bin
			0x00, 0x00, 0x65, 0x00,	0x00, 0x00, 0x00, 0x00, // loffset
			0x01, 0x00, 0x00, 0x00, // n_chunk

				0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_beg {101,0}
				0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_end {228,0}

			// Not mentioned in the Index spec.
			0x4a, 0x92, 0x00, 0x00, // bin - always 0x924a
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // loffset
			0x02, 0x00, 0x00, 0x00, // n_chunk - always 2

				0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_beg
				0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_end

				0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mapped_count
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_count

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // n_no_coor
*/
var conceptualCSIv1data = []byte{
	0x43, 0x53, 0x49, 0x01, 0x0e, 0x00, 0x00, 0x00,
	0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x4a, 0x92, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

var chunkTests = []struct {
	beg, end int
	expect   []bgzf.Chunk
}{
	{
		beg: 65000, end: 71000, // Index does not use tiles, so this is hit.
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
	{
		beg: 77594624, end: 80740352, // 73m77m:bin2+bin18 - This is the equivalent to the given example.
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
	{
		beg: 62914561, end: 68157440, // 60m65m:bin0+bin2
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
	{
		beg: 72351744, end: 80740352, // 69m77m:bin0+bin2+18
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
	{
		beg: 61865984, end: 80740352, // 59m77m:bin0+bin2+bin18
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
	{
		beg: 80740352, end: 81788928, // 77m78m - Not in covered region, but Index does not use tiles, so this is hit.
		expect: []bgzf.Chunk{
			{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},
		},
	},
}

func (s *S) TestConceptualCSIv1(c *check.C) {
	csi, err := ReadFrom(bytes.NewReader(conceptualCSIv1data))
	c.Assert(err, check.Equals, nil)

	for _, test := range chunkTests {
		c.Check(csi.Chunks(0, test.beg, test.end), check.DeepEquals, test.expect,
			check.Commentf("Unexpected result for [%d,%d).", test.beg, test.end),
		)
	}
	stats, ok := csi.ReferenceStats(0)
	c.Check(ok, check.Equals, true)
	c.Check(stats, check.Equals, index.ReferenceStats{
		Chunk: bgzf.Chunk{
			Begin: bgzf.Offset{File: 101, Block: 0},
			End:   bgzf.Offset{File: 228, Block: 0},
		},
		Mapped: 3, Unmapped: 0})
	unmapped, ok := csi.Unmapped()
	c.Check(ok, check.Equals, true)
	c.Check(unmapped, check.Equals, uint64(0))
}

// conceptualCSIv2data is an uncompressed CSIv1 for the alignments in the BAM
// corresponding to:
//
// @HD	VN:1.0	SO:coordinate
// @SQ	SN:conceptual	LN:134217728
// 60m66m:bin0	0	conceptual	62914561	40	6291456M	*	0	0	*	*
// 70m76m:bin2	0	conceptual	73400321	40	6291456M	*	0	0	*	*
// 73m75m:bin18	0	conceptual	76546049	40	2097152M	*	0	0	*	*
//
// This is a coordinate-translated version of the conceptual example in the
// SAM spec using binning as actually used by BAM rather than as presented.
/*
	0x43, 0x53, 0x49, 0x02, // Index\1
	0x0e, 0x00, 0x00, 0x00, // min_shift
	0x05, 0x00, 0x00, 0x00, // depth
	0x00, 0x00, 0x00, 0x00, // l_aux

	// no aux

	0x01, 0x00, 0x00, 0x00, // n_ref

		0x02, 0x00, 0x00, 0x00, // n_bin

			0x00, 0x00, 0x00, 0x00, // bin
			0x00, 0x00, 0x65, 0x00,	0x00, 0x00, 0x00, 0x00, // loffset
			0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // n_rec
			0x01, 0x00, 0x00, 0x00, // n_chunk

				0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_beg
				0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00, // chunk_end

			0x4a, 0x92, 0x00, 0x00, // bin
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // loffset
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // n_rec
			0x02, 0x00, 0x00, 0x00, // n_chunk

			0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_beg
			0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_end

			0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // mapped_count
			0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // unmapped_count

	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
*/
var conceptualCSIv2data = []byte{
	0x43, 0x53, 0x49, 0x02, 0x0e, 0x00, 0x00, 0x00,
	0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x65, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x4a, 0x92, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x65, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0xe4, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
}

func (s *S) TestConceptualCSIv2(c *check.C) {
	csi, err := ReadFrom(bytes.NewReader(conceptualCSIv2data))
	c.Assert(err, check.Equals, nil)

	for _, test := range chunkTests {
		c.Check(csi.Chunks(0, test.beg, test.end), check.DeepEquals, test.expect,
			check.Commentf("Unexpected result for [%d,%d).", test.beg, test.end),
		)
	}
	stats, ok := csi.ReferenceStats(0)
	c.Check(ok, check.Equals, true)
	c.Check(stats, check.Equals, index.ReferenceStats{
		Chunk: bgzf.Chunk{
			Begin: bgzf.Offset{File: 101, Block: 0},
			End:   bgzf.Offset{File: 228, Block: 0},
		},
		Mapped: 3, Unmapped: 0})
	unmapped, ok := csi.Unmapped()
	c.Check(ok, check.Equals, true)
	c.Check(unmapped, check.Equals, uint64(0))
}

func uint64ptr(i uint64) *uint64 {
	return &i
}

var csiTestData = []struct {
	csi *Index
	err error
}{
	{
		csi: &Index{
			minShift: 14, depth: 5,
			refs: []refIndex{
				{
					bins: []bin{
						{
							bin: 4681, left: bgzf.Offset{File: 98, Block: 0},
							chunks: []bgzf.Chunk{
								{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
							},
						},
					},
					stats: &index.ReferenceStats{
						Chunk:    bgzf.Chunk{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
						Mapped:   8,
						Unmapped: 1,
					},
				},
			},
			unmapped: uint64ptr(1),
			isSorted: true,
		},
		err: nil,
	},
	{
		csi: &Index{
			minShift: 14, depth: 5,
			refs: []refIndex{
				{
					bins: []bin{
						{
							bin: 4681, left: bgzf.Offset{File: 98, Block: 0},
							chunks: []bgzf.Chunk{
								{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
							},
						},
					},
					stats: &index.ReferenceStats{
						Chunk:    bgzf.Chunk{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
						Mapped:   8,
						Unmapped: 1,
					},
				},
			},
			unmapped: nil,
			isSorted: true,
		},
		err: nil,
	},
	{
		csi: &Index{
			minShift: 14, depth: 5,
			refs: []refIndex{
				{
					bins: []bin{
						{
							bin: 4681, left: bgzf.Offset{File: 98, Block: 0},
							chunks: []bgzf.Chunk{
								{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
							},
						},
					},
					stats: nil,
				},
			},
			unmapped: uint64ptr(1),
			isSorted: true,
		},
		err: nil,
	},
	{
		csi: &Index{
			minShift: 14, depth: 5,
			refs: []refIndex{
				{
					bins: []bin{
						{
							bin: 4681, left: bgzf.Offset{File: 98, Block: 0},
							chunks: []bgzf.Chunk{
								{Begin: bgzf.Offset{File: 98, Block: 0}, End: bgzf.Offset{File: 401, Block: 0}},
							},
						},
					},
					stats: nil,
				},
			},
			unmapped: nil,
			isSorted: true,
		},
		err: nil,
	},
	{
		csi: &Index{
			Auxilliary: []byte("Various commentary strings"),
			minShift:   14, depth: 5,
			refs: func() []refIndex {
				idx := [86]refIndex{
					23: {
						bins: []bin{
							{
								bin: 0x2070,
								chunks: []bgzf.Chunk{
									{
										Begin: bgzf.Offset{File: 0x1246, Block: 0x0},
										End:   bgzf.Offset{File: 0x1246, Block: 0x1cf9},
									},
								},
							},
						},
						stats: &index.ReferenceStats{
							Chunk: bgzf.Chunk{
								Begin: bgzf.Offset{File: 0x1246, Block: 0x0},
								End:   bgzf.Offset{File: 0x1246, Block: 0x1cf9},
							},
							Mapped: 0, Unmapped: 0x1d,
						},
					},
					24: {
						bins: []bin{
							{
								bin: 0x124a,
								chunks: []bgzf.Chunk{
									{
										Begin: bgzf.Offset{File: 0x1246, Block: 0x1cf9},
										End:   bgzf.Offset{File: 0x1246, Block: 0x401d},
									},
								},
							},
						},
						stats: &index.ReferenceStats{
							Chunk: bgzf.Chunk{
								Begin: bgzf.Offset{File: 0x1246, Block: 0x1cf9},
								End:   bgzf.Offset{File: 0x1246, Block: 0x401d},
							},
							Mapped: 0, Unmapped: 0x25,
						},
					},
					72: {
						bins: []bin{
							{
								bin: 0x1253,
								chunks: []bgzf.Chunk{
									{
										Begin: bgzf.Offset{File: 0x1246, Block: 0x401d},
										End:   bgzf.Offset{File: 0x1246, Block: 0x41f5},
									},
								},
							},
						},
						stats: &index.ReferenceStats{
							Chunk: bgzf.Chunk{
								Begin: bgzf.Offset{File: 0x1246, Block: 0x401d},
								End:   bgzf.Offset{File: 0x1246, Block: 0x41f5},
							},
							Mapped: 0, Unmapped: 0x2,
						},
					},
				}
				return idx[:]
			}(),
			unmapped: uint64ptr(932),
			isSorted: true,
		},
		err: nil,
	},
}

func (s *S) TestCSIRoundtrip(c *check.C) {
	for i, test := range csiTestData {
		for test.csi.Version = 1; test.csi.Version <= 2; test.csi.Version++ {
			var buf bytes.Buffer
			c.Assert(WriteTo(&buf, test.csi), check.Equals, nil)
			got, err := ReadFrom(&buf)
			c.Assert(err, check.Equals, nil, check.Commentf("Test %d", i))
			c.Check(got, check.DeepEquals, test.csi, check.Commentf("Test %d", i))
		}
	}
}
