// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"testing"

	"github.com/biogo/hts/sam"
)

type byFunc struct {
	less func(a, b *sam.Record) bool
	recs []*sam.Record
}

func (r byFunc) Len() int           { return len(r.recs) }
func (r byFunc) Less(i, j int) bool { return r.less(r.recs[i], r.recs[j]) }
func (r byFunc) Swap(i, j int)      { r.recs[i], r.recs[j] = r.recs[j], r.recs[i] }

func sortBAM(r io.Reader, so sam.SortOrder, less func(a, b *sam.Record) bool, fn func(*sam.Record), shard int) error {
	br, err := NewReader(r, 0)
	if err != nil {
		return fmt.Errorf("failed to open bam reader: %w", err)
	}
	defer br.Close()

	h := br.Header().Clone()
	h.SortOrder = so

	recs := make([]*sam.Record, 0, shard)
	var t []*Reader
	it := sam.NewIterator(br)
	for {
		var n int
		for it.Next() {
			recs = append(recs, it.Record())
			if len(recs) == cap(recs) {
				r, err := writeSorted(h, recs, less)
				if err != nil {
					return err
				}
				defer r.Close()
				t = append(t, r)
				n, recs = len(recs), recs[:0]
			}
		}
		if len(recs) != 0 {
			r, err := writeSorted(h, recs, less)
			if err != nil {
				return err
			}
			defer r.Close()
			t = append(t, r)
			break
		}
		err = it.Error()
		if n == 0 || err != nil {
			break
		}
	}
	if err != nil {
		return fmt.Errorf("error during bam reading: %w", err)
	}

	m, err := NewMerger(less, t...)
	if err != nil {
		return fmt.Errorf("failed to create merger: %w", err)
	}
	sorted := sam.NewIterator(m)
	for sorted.Next() {
		fn(sorted.Record())
	}
	err = sorted.Error()
	if err != nil {
		return fmt.Errorf("error during bam reading: %w", err)
	}

	return nil
}

func writeSorted(h *sam.Header, recs []*sam.Record, less func(a, b *sam.Record) bool) (*Reader, error) {
	if less != nil {
		sort.Sort(byFunc{less, recs})
	}

	var buf bytes.Buffer

	bw, err := NewWriter(&buf, h, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open bam writer: %w", err)
	}
	for _, r := range recs {
		err = bw.Write(r)
		if err != nil {
			return nil, fmt.Errorf("failed to write record: %w", err)
		}
	}
	err = bw.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close bam writer: %w", err)
	}

	r, err := NewReader(&buf, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open bam writer: %w", err)
	}
	return r, err
}

var mergerTests = []struct {
	r func() io.Reader

	so     sam.SortOrder
	less   func(a, b *sam.Record) bool
	expect func(a, b *sam.Record) bool
	shard  int
}{
	{
		r:      func() io.Reader { return bytes.NewReader(bamHG00096_1000) },
		so:     sam.QueryName,
		less:   nil,
		expect: (*sam.Record).LessByName,
		shard:  199,
	},
	{
		r:      func() io.Reader { return bytes.NewReader(bamHG00096_1000) },
		so:     sam.QueryName,
		less:   nil,
		expect: (*sam.Record).LessByName,
		shard:  1e5,
	},
	{
		r:      func() io.Reader { return bytes.NewReader(bamHG00096_1000) },
		so:     sam.Unsorted,
		less:   nil,
		expect: (*sam.Record).LessByCoordinate, // HG00096 is sorted by coordinate.
		shard:  199,
	},
}

func TestMerger(t *testing.T) {
	for _, test := range mergerTests {
		var recs []*sam.Record
		fn := func(r *sam.Record) {
			recs = append(recs, r)
		}

		var less func(a, b *sam.Record) bool
		switch test.so {
		case sam.UnknownOrder:
			less = test.less
		case sam.Unsorted:
		case sam.QueryName:
			less = (*sam.Record).LessByName
		case sam.Coordinate:
			less = (*sam.Record).LessByCoordinate
		}
		err := sortBAM(test.r(), test.so, less, fn, test.shard)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if less == nil {
			continue
		}
		if !sort.IsSorted(byFunc{test.expect, recs}) {
			t.Error("not in expected sort order")
		}
	}
}
