// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam_test

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"

	"github.com/biogo/hts/bam"
	"github.com/biogo/hts/sam"
)

func ExampleMerger_sortByCoordinate() {
	// Inputs.
	var (
		// Input source of BAM data.
		r io.Reader

		// Operation to perform on each record of
		// sorted stream.
		fn func(*sam.Record)
	)

	// Specify sort chunk size.
	const chunk = 1e5

	// Open source.
	br, err := bam.NewReader(r, 0)
	if err != nil {
		log.Fatalf("failed to open bam reader: %v", err)
	}
	defer br.Close()

	// Make header with coordinate sort order.
	h := br.Header().Clone()
	h.SortOrder = sam.Coordinate

	// Create file system workspace and prepare
	// for clean up.
	dir, err := ioutil.TempDir("", "")
	if err != nil {
		log.Fatalf("failed to create temp directory: %v", err)
	}
	defer func() {
		os.RemoveAll(dir)
		r := recover()
		if r != nil {
			log.Fatal(r)
		}
	}()

	// Limit number of records for each sort chunk.
	recs := make([]*sam.Record, 0, chunk)

	// Keep the collection of shards for merging.
	var t []*bam.Reader

	it := sam.NewIterator(br)
	for {
		var n int
		for it.Next() {
			recs = append(recs, it.Record())
			if len(recs) == cap(recs) {
				r, err := writeChunk(dir, h, recs)
				if err != nil {
					log.Panic(err)
				}
				t = append(t, r)
				n, recs = len(recs), recs[:0]
			}
		}
		err = it.Error()
		if n == 0 || err != nil {
			break
		}
		if len(recs) != 0 {
			r, err := writeChunk(dir, h, recs)
			if err != nil {
				log.Panic(err)
			}
			t = append(t, r)
		}
	}
	if err != nil {
		log.Panicf("error during bam reading: %v", err)
	}

	// Create merge using the coordinate sort order.
	m, err := bam.NewMerger(nil, t...)
	if err != nil {
		log.Panicf("failed to created merger: %v", err)
	}
	sorted := sam.NewIterator(m)
	for sorted.Next() {
		// Operate on coordinate sorted stream.
		fn(sorted.Record())
	}
	// Close the underlying Readers.
	for i, r := range t {
		err = r.Close()
		if err != nil {
			log.Printf("failed to close reader %d: %v", i, err)
		}
	}
	err = sorted.Error()
	if err != nil {
		log.Panicf("error during bam reading: %v", err)
	}
}

// writeChunk writes out the records in recs to the given directory
// after sorting them.
func writeChunk(dir string, h *sam.Header, recs []*sam.Record) (*bam.Reader, error) {
	sort.Sort(byCoordinate(recs))

	f, err := ioutil.TempFile(dir, "")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file %q: %v", err)
	}

	bw, err := bam.NewWriter(f, h, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open bam writer: %v", err)
	}
	for _, r := range recs {
		err = bw.Write(r)
		if err != nil {
			return nil, fmt.Errorf("failed to write record: %v", err)
		}
	}
	err = bw.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close bam writer: %v", err)
	}
	err = f.Sync()
	if err != nil {
		return nil, fmt.Errorf("failed to sync file: %v", err)
	}

	// Make a reader of the written data.
	_, err = f.Seek(0, os.SEEK_SET)
	if err != nil {
		return nil, fmt.Errorf("failed to seek to start: %v", err)
	}
	r, err := bam.NewReader(f, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to open bam writer: %v", err)
	}
	return r, err
}

// byCoordinate implements the coordinate sort order.
type byCoordinate []*sam.Record

func (r byCoordinate) Len() int           { return len(r) }
func (r byCoordinate) Less(i, j int) bool { return r[i].LessByCoordinate(r[j]) }
func (r byCoordinate) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
