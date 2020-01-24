// Copyright ©2020 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package fai

import (
	"errors"
	"io"

	"golang.org/x/exp/mmap"
)

// File is a sequence file with an FAI index. File access is implemented via mmapped
// file memory, so integer indexing limits may impact on access to large files.
type File struct {
	f   *mmap.ReaderAt
	idx Index
}

// OpenFile opens the sequence file at the given path and associates it with
// the specified index.
func OpenFile(path string, idx Index) (*File, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, err
	}
	return &File{f: f, idx: idx}, nil
}

// Close closes the sequence file and releases the index.
// Seq values obtained from f must not be used after Close has been called.
func (f *File) Close() error {
	err := f.f.Close()
	*f = File{}
	return err
}

// Seq returns a handle to the complete sequence identified by the given name.
func (f *File) Seq(name string) (*Seq, error) {
	rec, ok := f.idx[name]
	if !ok {
		return nil, errors.New("fai: no sequence")
	}
	return &Seq{f: f.f, rec: rec, end: rec.Length}, nil
}

// Seq returns a handle to the sequence identified by the given name from the
// start position until the end position.
func (f *File) SeqRange(name string, start, end int) (*Seq, error) {
	if start < 0 || end < 0 || end < start {
		return nil, errors.New("fai: index out of range")
	}
	rec, ok := f.idx[name]
	if !ok {
		return nil, errors.New("fai: no sequence")
	}
	if rec.Length < start || rec.Length < end {
		return nil, errors.New("fai: index out of range")
	}
	return &Seq{f: f.f, rec: rec, cur: start, start: start, end: end}, nil
}

// Seq is a handle to a sequence segment obtained from a File.
type Seq struct {
	rec Record
	f   *mmap.ReaderAt

	cur        int
	start, end int
}

// Close closes the sequence.
func (s *Seq) Close() error {
	*s = Seq{}
	return nil
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
	end := int(s.rec.position(s.end))
	for s.cur < s.end {
		cur := s.rec.position(s.cur)
		eol := min(s.rec.endOfLineOffset(s.cur), end-int(cur))
		_n, err := s.f.ReadAt(b[:min(eol, len(b))], cur)
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

// At returns the sequence letter at i, which must be within the range specified when the
// the Seq was created otherwise At will panic.
func (s *Seq) At(i int) byte {
	if i < s.start || s.end <= i {
		panic("fai: index out of range")
	}
	p := s.rec.position(i)
	if int64(int(p)) != p {
		panic("fai: index out of range")
	}
	return s.f.At(int(p))
}
