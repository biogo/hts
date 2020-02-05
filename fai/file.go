// Copyright ©2020 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fai

import (
	"errors"
	"io"
)

// File is a sequence file with an FAI index.
type File struct {
	Index

	r io.ReaderAt
}

// NewFile returns the sequence file for the given reader and associates it with
// the specified index.
func NewFile(r io.ReaderAt, idx Index) *File {
	return &File{r: r, Index: idx}
}

// Seq returns a handle to the complete sequence identified by the given name.
func (f *File) Seq(name string) (*Seq, error) {
	rec, ok := f.Index[name]
	if !ok {
		return nil, errors.New("fai: no sequence")
	}
	return &Seq{r: f.r, Record: rec, end: rec.Length}, nil
}

// Seq returns a handle to the sequence identified by the given name from the
// start position until the end position.
func (f *File) SeqRange(name string, start, end int) (*Seq, error) {
	if start < 0 || end < 0 || end < start {
		return nil, errors.New("fai: index out of range")
	}
	rec, ok := f.Index[name]
	if !ok {
		return nil, errors.New("fai: no sequence")
	}
	if rec.Length < start || rec.Length < end {
		return nil, errors.New("fai: index out of range")
	}
	return &Seq{r: f.r, Record: rec, cur: start, start: start, end: end}, nil
}

// Seq is a handle to a sequence segment obtained from a File.
type Seq struct {
	Record

	r io.ReaderAt

	cur        int
	start, end int
}

// Reset resets the position of the cursor into the sequence segment to the original start.
func (s *Seq) Reset() {
	s.cur = s.start
}

// Read reads sequence data from the Seq into b.
func (s *Seq) Read(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	var n int
	end := int(s.Record.position(s.end))
	for s.cur < s.end {
		cur := s.Record.position(s.cur)
		eol := min(s.Record.endOfLineOffset(s.cur), end-int(cur))
		_n, err := s.r.ReadAt(b[:min(eol, len(b))], cur)
		s.cur += _n
		n += _n
		b = b[_n:]
		if err != nil || len(b) == 0 {
			return n, err
		}
	}
	return n, io.EOF
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
