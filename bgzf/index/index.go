// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package index provides common code for CSI and tabix BGZF indexing.
package index

import (
	"errors"
	"io"

	"github.com/biogo/hts/bgzf"
)

var (
	ErrNoReference = errors.New("index: no reference")
	ErrInvalid     = errors.New("index: invalid interval")
)

// ReferenceStats holds mapping statistics for a genomic reference.
type ReferenceStats struct {
	// Chunk is the span of the indexed BGZF
	// holding alignments to the reference.
	Chunk bgzf.Chunk

	// Mapped is the count of mapped reads.
	Mapped uint64

	// Unmapped is the count of unmapped reads.
	Unmapped uint64
}

// Reader wraps a bgzf.Reader to provide a mechanism to read a selection of
// BGZF chunks.
type ChunkReader struct {
	r *bgzf.Reader

	wasBlocked bool

	chunks []bgzf.Chunk
}

// NewChunkReader returns a ChunkReader to read from r, limiting the reads to
// the provided chunks. The provided bgzf.Reader will be put into Blocked mode.
func NewChunkReader(r *bgzf.Reader, chunks []bgzf.Chunk) (*ChunkReader, error) {
	b := r.Blocked
	r.Blocked = true
	if len(chunks) != 0 {
		err := r.Seek(chunks[0].Begin)
		if err != nil {
			return nil, err
		}
	}
	return &ChunkReader{r: r, wasBlocked: b, chunks: chunks}, nil
}

// Read satisfies the io.Reader interface.
func (r *ChunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 {
		return 0, io.EOF
	}
	last := r.r.LastChunk()
	if vOffset(last.End) >= vOffset(r.chunks[0].End) {
		return 0, io.EOF
	}

	// Ensure the byte slice does not extend beyond the end of
	// the current chunk. We do not need to consider reading
	// beyond the end of the block because the bgzf.Reader is in
	// blocked mode and so will stop there anyway.
	want := int(r.chunks[0].End.Block)
	if r.chunks[0].End.Block == 0 && r.chunks[0].End.File > last.End.File {
		// Special case for when the current end block offset
		// is zero.
		want = r.r.BlockLen()
	}
	var cursor int
	if last.End.File == r.chunks[0].End.File {
		// Our end is in the same block as the last chunk end
		// so set the cursor to the chunk block end to prevent
		// reading past the end of the chunk.
		cursor = int(last.End.Block)
	}
	n, err := r.r.Read(p[:min(len(p), want-cursor)])
	if err != nil {
		if n != 0 && err == io.EOF {
			err = nil
		}
		return n, err
	}

	// Check whether we are at or past the end of the current
	// chunk or we have not made progress for reasons other than
	// zero length p.
	this := r.r.LastChunk()
	if (len(p) != 0 && this == last) || vOffset(this.End) >= vOffset(r.chunks[0].End) {
		r.chunks = r.chunks[1:]
		if len(r.chunks) == 0 {
			return n, io.EOF
		}
		err = r.r.Seek(r.chunks[0].Begin)
	}

	return n, err
}

func vOffset(o bgzf.Offset) int64 {
	return o.File<<16 | int64(o.Block)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Close returns the bgzf.Reader to its original blocking mode and releases it.
// The bgzf.Reader is not closed.
func (r *ChunkReader) Close() error {
	r.r.Blocked = r.wasBlocked
	r.r = nil
	return nil
}
