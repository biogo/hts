// Copyright ©2015 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package index implement CSI and tabix BGZF indexing.
package index

import (
	"io"

	"code.google.com/p/biogo.bam/bgzf"
)

// Reader wraps a bgzf.Reader to provide a mechanism to read a selection of
// BGZF chunks.
type ChunkReader struct {
	r *bgzf.Reader

	chunks []bgzf.Chunk
}

// NewChunkReader returns a ChunkReader to read from r, limiting the reads to
// the provided chunks.
//
// Note: Currently the limiting is loose; a single read that extends beyond the
// end of a chunk will not be truncated. In practice this should not be an issue
// since reads should correspond to the reads that were performed during indexing.
// This behaviour may change in future.
func NewChunkReader(r *bgzf.Reader, chunks []bgzf.Chunk) (*ChunkReader, error) {
	if len(chunks) != 0 {
		err := r.Seek(chunks[0].Begin)
		if err != nil {
			return nil, err
		}
	}
	return &ChunkReader{r: r, chunks: chunks}, nil
}

// Read satisfies the io.Reader interface.
func (r *ChunkReader) Read(p []byte) (int, error) {
	if len(r.chunks) == 0 && vOffset(r.r.LastChunk().End) >= vOffset(r.chunks[0].End) {
		return 0, io.EOF
	}

	// TODO(kortschak): Make the chunk limits hard. Currently a read that
	// extends beyond the end of a chunk is not truncated. It probably
	// should be. This may require invasive changes to bgzf.Reader.
	n, err := r.r.Read(p)
	if err != nil {
		return n, err
	}
	if len(r.chunks) != 0 && vOffset(r.r.LastChunk().End) >= vOffset(r.chunks[0].End) {
		err = r.r.Seek(r.chunks[0].Begin)
		r.chunks = r.chunks[1:]
	}
	return n, err
}
