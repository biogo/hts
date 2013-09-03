// Copyright ©2013 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
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
		c.Check(br.Header(), check.DeepEquals, t.header)
		if !reflect.DeepEqual(br.Header(), t.header) {
			c.Check(br.Header().Refs(), check.DeepEquals, t.header.Refs())
			c.Check(br.Header().RGs(), check.DeepEquals, t.header.RGs())
			c.Check(br.Header().Progs(), check.DeepEquals, t.header.Progs())
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
