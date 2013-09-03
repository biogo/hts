// Copyright ©2013 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"code.google.com/p/biogo.bam/bgzf/egzip"
	"flag"
	"fmt"
	"io"
	check "launchpad.net/gocheck"
	"os"
	"reflect"
	"testing"
)

var (
	bam    = flag.String("bam", "", "output first failing bam data to this file for inspection")
	allbam = flag.String("allbam", "", "output all bam data to this file base for inspection")
)

func Test(t *testing.T) { check.TestingT(t) }

type S struct{}

var _ = check.Suite(&S{})

func (s *S) TestRead(c *check.C) {
	for i, t := range []struct {
		in     []byte
		limit  bool
		header *Header
		lines  int
	}{
		{
			in:     bamHG00096_1000,
			limit:  false,
			header: headerHG00096_1000,
			lines:  1000,
		},
		{
			in:     bamHG00096_1000,
			limit:  true,
			header: headerHG00096_1000,
			lines:  1000,
		},
	} {
		br, err := NewReader(bytes.NewBuffer(t.in), t.limit)
		c.Assert(err, check.Equals, nil)
		c.Check(br.Header(), check.DeepEquals, t.header)
		if !reflect.DeepEqual(br.Header(), t.header) {
			c.Check(br.Header().Refs(), check.DeepEquals, t.header.Refs())
			c.Check(br.Header().RGs(), check.DeepEquals, t.header.RGs())
			c.Check(br.Header().Progs(), check.DeepEquals, t.header.Progs())
			c.Check(br.Header().Comments, check.DeepEquals, t.header.Comments)
		}
		var lines int
		for {
			_, err := br.Read()
			if err != nil {
				if err == egzip.NewBlock {
					continue
				}
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			lines++
		}
		c.Check(lines, check.Equals, t.lines)
		if *allbam != "" {
			bf, err := os.Create(fmt.Sprintf("%s-%d.bam", *allbam, i))
			c.Assert(err, check.Equals, nil)
			bf.Write(t.in)
			bf.Close()
		}
		if c.Failed() && *bam != "" {
			bf, err := os.Create(*bam)
			c.Assert(err, check.Equals, nil)
			bf.Write(t.in)
			bf.Close()
			c.FailNow()
		}
	}
}

func (s *S) TestRoundTrip(c *check.C) {
	for _, t := range []struct {
		in     []byte
		header *Header
		lines  int
	}{
		{
			in:     bamHG00096_1000,
			header: headerHG00096_1000,
			lines:  1000,
		},
	} {
		br, err := NewReader(bytes.NewBuffer(t.in), false)
		c.Assert(err, check.Equals, nil)

		var buf bytes.Buffer
		bw, err := NewWriter(&buf, br.Header().Clone())
		for {
			r, err := br.Read()
			if err != nil {
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			bw.Write(r)
		}
		c.Assert(bw.Close(), check.Equals, nil)

		br, err = NewReader(bytes.NewBuffer(t.in), false)
		c.Assert(err, check.Equals, nil)
		brr, err := NewReader(&buf, false)
		c.Assert(err, check.Equals, nil)
		c.Check(brr.Header().String(), check.Equals, br.Header().String())
		c.Check(brr.Header(), check.DeepEquals, br.Header())
		if !reflect.DeepEqual(brr.Header(), br.Header()) {
			c.Check(brr.Header().Refs(), check.DeepEquals, br.Header().Refs())
			c.Check(brr.Header().RGs(), check.DeepEquals, br.Header().RGs())
			c.Check(brr.Header().Progs(), check.DeepEquals, br.Header().Progs())
			c.Check(brr.Header().Comments, check.DeepEquals, br.Header().Comments)
		}
		for {
			r, err := br.Read()
			if err != nil {
				c.Assert(err, check.Equals, io.EOF)
			}
			rr, err := brr.Read()
			if err != nil {
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			c.Check(rr, check.DeepEquals, r)
		}
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	for i := 0; i < b.N; i++ {
		br, _ := NewReader(bytes.NewBuffer(bamHG00096_1000), false)

		var buf bytes.Buffer
		bw, _ := NewWriter(&buf, br.Header().Clone())
		for {
			r, err := br.Read()
			if err != nil {
				break
			}
			bw.Write(r)
		}
	}
}
