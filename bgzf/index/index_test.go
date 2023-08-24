// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"bytes"
	"flag"
	"io"
	"strings"
	"testing"

	"github.com/biogo/hts/bgzf"

	"gopkg.in/check.v1"
)

var conc = flag.Int("conc", 1, "sets the level of concurrency for compression")

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

// conceptualBAMdata is the BAM corresponding to:
//
// @HD	VN:1.0	SO:coordinate
// @SQ	SN:conceptual	LN:134217728
// 60m66m:bin0	0	conceptual	62914561	40	6291456M	*	0	0	*	*
// 70m76m:bin2	0	conceptual	73400321	40	6291456M	*	0	0	*	*
// 73m75m:bin18	0	conceptual	76546049	40	2097152M	*	0	0	*	*
//
// This is a coordinate-translated version of the conceptual example in the
// SAM spec using binning as actually used by BAM rather than as presented.
var conceptualBAMdata = []byte{
	// sam.Header block [{File:0, Block:0}, {File:0, Block:87}).
	0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
	0x06, 0x00, 0x42, 0x43, 0x02, 0x00, 0x64, 0x00, 0x73, 0x72,
	0xf4, 0x65, 0xb4, 0x60, 0x60, 0x60, 0x70, 0xf0, 0x70, 0xe1,
	0x0c, 0xf3, 0xb3, 0x32, 0xd4, 0x33, 0xe0, 0x0c, 0xf6, 0xb7,
	0x4a, 0xce, 0xcf, 0x2f, 0x4a, 0xc9, 0xcc, 0x4b, 0x2c, 0x49,
	0xe5, 0x72, 0x08, 0x0e, 0xe4, 0x0c, 0xf6, 0x03, 0x8a, 0xe4,
	0x25, 0xa7, 0x16, 0x94, 0x94, 0x26, 0xe6, 0x70, 0xfa, 0x00,
	0x95, 0x19, 0x9b, 0x18, 0x19, 0x9a, 0x9b, 0x1b, 0x59, 0x70,
	0x31, 0x02, 0xf5, 0x72, 0x03, 0x31, 0x42, 0x1e, 0xc8, 0x61,
	0xe0, 0x00, 0x00, 0x42, 0x51, 0xcc, 0xea, 0x57, 0x00, 0x00,
	0x00,

	// Record block [{File:101, Block:0}, {File:101, Block:157}).
	0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
	0x06, 0x00, 0x42, 0x43, 0x02, 0x00, 0x62, 0x00, 0x33, 0x60,
	0x80, 0x81, 0x03, 0xcc, 0x3c, 0x1a, 0x0c, 0x0c, 0x8c, 0x50,
	0xde, 0x7f, 0x28, 0x00, 0xb1, 0xcd, 0x0c, 0x72, 0xcd, 0xcc,
	0x72, 0xad, 0x92, 0x32, 0xf3, 0x0c, 0x40, 0x5c, 0x36, 0x03,
	0xb8, 0x9e, 0x04, 0x16, 0x1e, 0x0d, 0x26, 0xac, 0x7a, 0xcc,
	0x0d, 0x72, 0xcd, 0x21, 0x7a, 0x8c, 0xc0, 0x7a, 0x0c, 0xe1,
	0x7a, 0x26, 0xb0, 0xf0, 0x6a, 0x08, 0x61, 0xd7, 0x63, 0x9c,
	0x6b, 0x6e, 0x0a, 0xd6, 0x63, 0x68, 0x01, 0xe2, 0x33, 0x01,
	0x00, 0x5a, 0x80, 0xfe, 0xec, 0x9d, 0x00, 0x00, 0x00,

	// Magic block [{File:200, Block:0}, {File:200, Block:0}).
	0x1f, 0x8b, 0x08, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0xff,
	0x06, 0x00, 0x42, 0x43, 0x02, 0x00, 0x1b, 0x00, 0x03, 0x00,
	0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,

	// End {File:228, Block:0}
}

var conceptualChunks = []bgzf.Chunk{
	{Begin: bgzf.Offset{File: 0, Block: 0}, End: bgzf.Offset{File: 0, Block: 87}},        // header
	{Begin: bgzf.Offset{File: 101, Block: 0}, End: bgzf.Offset{File: 101, Block: 52}},    // 60m66m:bin0
	{Begin: bgzf.Offset{File: 101, Block: 52}, End: bgzf.Offset{File: 101, Block: 104}},  // 70m76m:bin2
	{Begin: bgzf.Offset{File: 101, Block: 104}, End: bgzf.Offset{File: 101, Block: 157}}, // 73m75m:bin18
	{Begin: bgzf.Offset{File: 228, Block: 0}, End: bgzf.Offset{File: 228, Block: 0}},     // EOF
}

// Test for issue #6 https://github.com/biogo/hts/issues/6
func (s *S) TestChunkReader(c *check.C) {
	br, err := bgzf.NewReader(bytes.NewReader(conceptualBAMdata), *conc)
	c.Assert(err, check.Equals, nil)
	defer br.Close()
	cr, err := NewChunkReader(br, conceptualChunks)
	c.Assert(err, check.Equals, nil)
	defer cr.Close()
	// 2 is shorter than the length of the first block.
	// This panics prior to the fix.
	n, err := cr.Read(make([]byte, 2))
	c.Check(n, check.Equals, 2)
	c.Check(err, check.Equals, nil)
}

// Test for issue #8 https://github.com/biogo/hts/issues/8
func (s *S) TestIssue8(c *check.C) {
	br, err := bgzf.NewReader(bytes.NewReader(conceptualBAMdata), *conc)
	c.Assert(err, check.Equals, nil)
	defer br.Close()
	cr, err := NewChunkReader(br, conceptualChunks[:2])
	c.Assert(err, check.Equals, nil)
	defer cr.Close()
	var last []byte
	for {
		p := make([]byte, 1024)
		n, err := cr.Read(p)
		if n != 0 {
			c.Check(p[:n], check.Not(check.DeepEquals), last[:min(n, len(last))])
		}
		last = p
		if err != nil {
			if err == io.EOF {
				break
			}
			c.Fatalf("unexpected error: %v", err)
		}
	}
}

// issue10Tests are test cases for https://github.com/biogo/hts/issues/10.
var issue10Tests = []struct {
	words     []wordBlocks
	chunks    []string
	canSquash bool
	canTrunc  bool
}{
	{
		// This is semantically identical to the test case given in issue 10.
		words:     commonWords,
		chunks:    []string{"<three>", "<five>"},
		canSquash: true,
		canTrunc:  false,
	},
	{
		words:     commonWords,
		chunks:    []string{"<one>", "<two>", "<three>"},
		canSquash: true,
		canTrunc:  false,
	},
	{
		words:     commonWords,
		chunks:    []string{"<two>", "<three>", "<four>", "<five>"},
		canSquash: true,
		canTrunc:  true,
	},
	{
		words:     commonWords,
		chunks:    []string{"<three>", "<four>"},
		canSquash: true,
		canTrunc:  true,
	},
	{
		words:     commonWords,
		chunks:    []string{"<seven>", "<eight>"},
		canSquash: true,
		canTrunc:  true,
	},
	{
		words:     commonWords,
		chunks:    []string{"<zero>", "<one>", "<two>", "<three>", "<four>", "<five>", "<six>", "<seven>", "<eight>"},
		canSquash: true,
		canTrunc:  true,
	},
	{
		// This case would never happen with an htslib-like index, but
		// it is a possible use case and not prohibited, so test it.
		words:  commonWords,
		chunks: []string{"<three>", "<zero>", "<five>", "<seven>", "<two>", "<eight>", "<five>"},

		// Not in order.
		canSquash: false,
		canTrunc:  false,
	},
}

var commonWords = []wordBlocks{
	// Begin:{File:0 Block:0} End:{File:0 Block:6}
	// Begin:{File:0 Block:6} End:{File:0 Block:11}
	{word: "<zero>"}, {word: "<one>", flush: true},
	// Begin:{File:43 Block:0} End:{File:43 Block:5}
	// Begin:{File:43 Block:5} End:{File:43 Block:12}
	// Begin:{File:43 Block:12} End:{File:43 Block:18}
	{word: "<two>"}, {word: "<three>"}, {word: "<four>", flush: true},
	// Begin:{File:93 Block:0} End:{File:93 Block:6}
	// Begin:{File:93 Block:6} End:{File:93 Block:11}
	{word: "<five>"}, {word: "<six>"}, {word: "<seven>", flush: true},
	// Begin:{File:142 Block:0} End:{File:142 Block:7}
	{word: "<eight>"},
}

type wordBlocks struct {
	word  string
	flush bool
}

type word int

func (w word) RefID() int { return 0 }
func (w word) Start() int { return int(w) }
func (w word) End() int   { return int(w + 1) }

func (s *S) TestIssue10(c *check.C) {
	for _, test := range issue10Tests {
		var buf bytes.Buffer

		// Write the set of words to a bgzf stream.
		w := bgzf.NewWriter(&buf, *conc)
		for _, wb := range test.words {
			w.Write([]byte(wb.word))
			if wb.flush {
				w.Flush()
			}
		}
		w.Close()

		for _, strategy := range []MergeStrategy{nil, adjacent} {
			if strategy != nil && !test.canSquash {
				continue
			}
			for _, clean := range []bool{false, true} {
				for _, truncFinal := range []bool{false, true} {
					if truncFinal && !test.canTrunc {
						continue
					}
					// Build an index into the words.
					r, err := bgzf.NewReader(bytes.NewReader(buf.Bytes()), *conc)
					c.Assert(err, check.Equals, nil)
					idx := make(map[string]bgzf.Chunk)
					for i, wb := range test.words {
						p := make([]byte, len(wb.word))
						n, err := r.Read(p)
						c.Assert(err, check.Equals, nil)
						c.Assert(string(p[:n]), check.Equals, wb.word)

						last := r.LastChunk()
						if !clean {
							// This simulates the index construction behaviour
							// that appears to be what is done by htslib. The
							// behaviour of bgzf is to elide seeks that will not
							// result in a productive read.
							if i != 0 && test.words[i-1].flush {
								last.Begin = idx[test.words[i-1].word].End
							}
						}
						idx[wb.word] = last
					}

					var chunks []bgzf.Chunk
					for _, w := range test.chunks {
						chunks = append(chunks, idx[w])
					}
					var want string
					if truncFinal {
						want = strings.Join(test.chunks[:len(test.chunks)-1], "")
						chunks[len(chunks)-2].End = chunks[len(chunks)-1].Begin
						chunks = chunks[:len(chunks)-1]
					} else {
						want = strings.Join(test.chunks, "")
					}

					if strategy != nil {
						chunks = strategy(chunks)
					}
					cr, err := NewChunkReader(r, chunks)
					c.Assert(err, check.Equals, nil)

					var got bytes.Buffer
					io.Copy(&got, cr)
					c.Check(got.String(), check.Equals, want,
						check.Commentf("clean=%t merge=%t trunc=%t chunks=%+v", clean, strategy != nil, truncFinal, chunks),
					)
				}
			}
		}
	}
}
