// Copyright ©2013 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	check "launchpad.net/gocheck"
	"os"
	"reflect"
	"testing"
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

func (s *S) TestRead(c *check.C) {
	for i, t := range []struct {
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
		br, err := NewReader(bytes.NewBuffer(t.in))
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
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			lines++
		}
		c.Check(lines, check.Equals, t.lines)
		if ok := reflect.DeepEqual(br.Header(), t.header) && lines == t.lines; *bam && !ok || *allbam {
			bf, err := os.Create(fmt.Sprintf("read-%d-%s.bam", i, failure(!ok)))
			c.Assert(err, check.Equals, nil)
			bf.Write(t.in)
			bf.Close()
		}
	}
}

func (s *S) TestCloneHeader(c *check.C) {
	for _, h := range []*Header{
		headerHG00096_1000,
	} {
		c.Check(h, check.DeepEquals, h.Clone())
	}
}

func (s *S) TestRoundTrip(c *check.C) {
	for i, t := range []struct {
		in     []byte
		header *Header
		conc   int
		lines  int
	}{
		{
			in:     bamHG00096_1000,
			header: headerHG00096_1000,
			conc:   2,
			lines:  1000,
		},
	} {
		br, err := NewReader(bytes.NewBuffer(t.in))
		c.Assert(err, check.Equals, nil)

		var buf bytes.Buffer
		bw, err := NewWriter(&buf, br.Header().Clone(), t.conc)
		for {
			r, err := br.Read()
			if err != nil {
				c.Assert(err, check.Equals, io.EOF)
				break
			}
			bw.Write(r)
		}
		c.Assert(bw.Close(), check.Equals, nil)

		br, err = NewReader(bytes.NewBuffer(t.in))
		c.Assert(err, check.Equals, nil)
		brr, err := NewReader(&buf)
		c.Assert(err, check.Equals, nil)
		c.Check(brr.Header().String(), check.Equals, br.Header().String())
		c.Check(brr.Header(), check.DeepEquals, br.Header())
		if !reflect.DeepEqual(brr.Header(), br.Header()) {
			c.Check(brr.Header().Refs(), check.DeepEquals, br.Header().Refs())
			c.Check(brr.Header().RGs(), check.DeepEquals, br.Header().RGs())
			c.Check(brr.Header().Progs(), check.DeepEquals, br.Header().Progs())
			c.Check(brr.Header().Comments, check.DeepEquals, br.Header().Comments)
		}
		allOK := true
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
			if !reflect.DeepEqual(rr, r) {
				allOK = false
			}
		}
		if ok := reflect.DeepEqual(br.Header(), brr.Header()) && allOK; *bam && !ok || *allbam {
			bf, err := os.Create(fmt.Sprintf("roundtrip-%d-%s.bam", i, failure(!ok)))
			c.Assert(err, check.Equals, nil)
			bf.Write(t.in)
			bf.Close()
		}
	}
}

var (
	file = flag.String("bench.file", "", "file to read for benchmarking")
	conc = flag.Int("conc", 1, "sets the level of concurrency for compression")
)

func BenchmarkRead(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}
	b.StopTimer()
	f, err := os.Open(*file)
	if err != nil {
		b.Fatalf("Open failed: %v", err)
	}
	br, err := NewReader(f)
	if err != nil {
		b.Fatalf("NewReader failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		for {
			_, err := br.Read()
			if err != nil {
				break
			}
		}
	}
	f.Close()
}

func BenchmarkWrite(b *testing.B) {
	b.StopTimer()
	br, err := NewReader(bytes.NewReader(bamHG00096_1000))
	if err != nil {
		b.Fatalf("NewReader failed: %v", err)
	}
	r, err := br.Read()
	if err != nil {
		b.Fatalf("Read failed: %v", err)
	}
	bw, err := NewWriter(ioutil.Discard, br.Header().Clone(), *conc)
	if err != nil {
		b.Fatalf("NewWriter failed: %v", err)
	}
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		err = bw.Write(r)
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkReadFile(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}

	for i := 0; i < b.N; i++ {
		f, err := os.Open(*file)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		br, err := NewReader(f)
		if err != nil {
			b.Fatalf("NewReader failed: %v", err)
		}
		for {
			_, err := br.Read()
			if err != nil {
				break
			}
		}
		f.Close()
	}
}

// The is to comparable to `time samtools view -b $file > /dev/null'.
func BenchmarkRoundtripFile(b *testing.B) {
	if *file == "" {
		b.Skip("no file specified")
	}

	for i := 0; i < b.N; i++ {
		f, err := os.Open(*file)
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}
		br, err := NewReader(f)
		if err != nil {
			b.Fatalf("NewReader failed: %v", err)
		}
		bw, err := NewWriter(ioutil.Discard, br.Header().Clone(), *conc)
		if err != nil {
			b.Fatalf("NewWriter failed: %v", err)
		}
		for {
			r, err := br.Read()
			if err != nil {
				break
			}
			err = bw.Write(r)
			if err != nil {
				b.Fatalf("Write failed: %v", err)
			}
		}
		f.Close()
	}
}
