// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"container/heap"
	"errors"
	"io"

	"github.com/biogo/hts/sam"
)

// Merger implements merging BAM data with a defined sort
// order. It can be used for sorting, concatenating and
// deduplicating BAM data.
type Merger struct {
	h *sam.Header
	// refLinks is the set of mappings from a RefID in
	// a src Header to a Reference in the dst Header.
	refLinks [][]*sam.Reference

	less    func(a, b *sam.Record) bool
	readers []*reader
}

type reader struct {
	id int
	r  *Reader

	head *sam.Record
	err  error
}

// NewMerger returns a Merger that reads from the source
// Readers.
//
// The sort order of the stream merge is defined by the sort
// order field of the src Reader headers and the provided less
// function. The header sort order fields must agree.
//
// Sort order is determined using the following rules:
//  - for sam.QueryName the LessByName sam.Record method is used.
//  - for sam.Coordinate the LessByCoordinate sam.Record method is used.
//  - for sam.Unsorted the reader streams are concatenated.
//  - for sam.Unknown the provided less function is used - if nil
//    this is the same as sam.Unsorted.
// For all sort orders other than sam.Unknown, the less parameter
// is ignored.
// The src Readers should be closed individually after use to avoid
// leaking resources.
func NewMerger(less func(a, b *sam.Record) bool, src ...*Reader) (*Merger, error) {
	if len(src) == 0 {
		return nil, io.EOF
	}

	m := &Merger{readers: make([]*reader, len(src))}

	headers := make([]*sam.Header, len(src))
	so := src[0].Header().SortOrder
	for i, r := range src {
		h := r.Header()
		if h.SortOrder != so {
			return nil, errors.New("bam: sort order mismatch")
		}
		headers[i] = h
	}
	var err error
	m.h, m.refLinks, err = sam.MergeHeaders(headers)
	if err != nil {
		return nil, err
	}
	m.h.SortOrder = so

	readers := make([]reader, len(src))
	switch m.h.SortOrder {
	default:
		fallthrough
	case sam.UnknownOrder:
		m.less = less
	case sam.Unsorted:
	case sam.QueryName:
		m.less = (*sam.Record).LessByName
	case sam.Coordinate:
		m.less = (*sam.Record).LessByCoordinate
	}
	for i, r := range src {
		if m.less == nil {
			readers[i].id = i
			readers[i].r = r
			m.readers[i] = &readers[i]
			continue
		}
		rec, err := r.Read()
		readers[i] = reader{id: i, r: r, head: rec, err: err}
		m.readers[i] = &readers[i]
	}
	if m.less != nil {
		heap.Init((*bySortOrderAndID)(m))
	}

	return m, nil
}

// Header returns the SAM Header held by the Reader. This Header is
// constructed using the sam.MergeHeaders function.
func (m *Merger) Header() *sam.Header {
	return m.h
}

// Read returns the next sam.Record in the BAM stream.
//
// The Read behaviour will depend on the underlying Readers.
func (m *Merger) Read() (rec *sam.Record, err error) {
	if len(m.readers) == 0 {
		return nil, io.EOF
	}
	if m.less == nil {
		return m.cat()
	}
	return m.nextBySortOrder()
}

func (m *Merger) cat() (rec *sam.Record, err error) {
	id := m.readers[0].id
	rec, err = m.readers[0].r.Read()
	if err == io.EOF && len(m.readers) != 0 {
		m.readers = m.readers[1:]
		err = nil
	}
	if rec == nil {
		return m.Read()
	}
	m.reassignReference(id, rec)
	return rec, err
}

func (m *Merger) nextBySortOrder() (rec *sam.Record, err error) {
	reader := m.pop()
	rec, err = reader.head, reader.err
	reader.head, reader.err = reader.r.Read()
	if reader.err == nil {
		m.push(reader)
	}
	if rec == nil {
		return m.Read()
	}
	if err == io.EOF {
		err = nil
	}
	m.reassignReference(reader.id, rec)
	return rec, err
}

func (m *Merger) reassignReference(id int, rec *sam.Record) {
	if rec.Ref == nil || m.refLinks == nil {
		return
	}
	rec.Ref = m.refLinks[id][rec.RefID()]
}

func (m *Merger) push(r *reader) { heap.Push((*bySortOrderAndID)(m), r) }
func (m *Merger) pop() *reader   { return heap.Pop((*bySortOrderAndID)(m)).(*reader) }

type bySortOrderAndID Merger

func (m *bySortOrderAndID) Push(i interface{}) {
	m.readers = append(m.readers, i.(*reader))
}
func (m *bySortOrderAndID) Pop() interface{} {
	var r *reader
	r, m.readers = m.readers[len(m.readers)-1], m.readers[:len(m.readers)-1]
	return r
}
func (m *bySortOrderAndID) Len() int {
	return len(m.readers)
}
func (m *bySortOrderAndID) Less(i, j int) bool {
	if m.less(m.readers[i].head, m.readers[j].head) {
		return true
	}
	return m.readers[i].id < m.readers[j].id && !m.less(m.readers[j].head, m.readers[i].head)
}
func (m *bySortOrderAndID) Swap(i, j int) {
	m.readers[i], m.readers[j] = m.readers[j], m.readers[i]
}
