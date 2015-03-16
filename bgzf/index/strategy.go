// Copyright ©2015 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"github.com/biogo/hts/bgzf"
)

// MergeStrategy represents a chunk compression strategy.
type MergeStrategy func([]bgzf.Chunk) []bgzf.Chunk

var (
	// Identity leaves the []bgzf.Chunk unaltered.
	Identity MergeStrategy = identity

	// Adjacent merges contiguous bgzf.Chunks.
	Adjacent MergeStrategy = adjacent

	// Squash merges all bgzf.Chunks into a single bgzf.Chunk.
	Squash MergeStrategy = squash
)

// CompressorStrategy returns a MergeStrategy that will merge bgzf.Chunks
// that have a distance between BGZF block starts less than or equal
// to near.
func CompressorStrategy(near int64) MergeStrategy {
	return func(chunks []bgzf.Chunk) []bgzf.Chunk {
		if len(chunks) == 0 {
			return nil
		}
		for c := 1; c < len(chunks); c++ {
			leftChunk := chunks[c-1]
			rightChunk := &chunks[c]
			if leftChunk.End.File+near >= rightChunk.Begin.File {
				rightChunk.Begin = leftChunk.Begin
				if vOffset(leftChunk.End) > vOffset(rightChunk.End) {
					rightChunk.End = leftChunk.End
				}
				chunks = append(chunks[:c-1], chunks[c:]...)
				c--
			}
		}
		return chunks
	}
}

func identity(chunks []bgzf.Chunk) []bgzf.Chunk { return chunks }

func adjacent(chunks []bgzf.Chunk) []bgzf.Chunk {
	if len(chunks) == 0 {
		return nil
	}
	for c := 1; c < len(chunks); c++ {
		leftChunk := chunks[c-1]
		rightChunk := &chunks[c]
		leftEndOffset := vOffset(leftChunk.End)
		if leftEndOffset >= vOffset(rightChunk.Begin) {
			rightChunk.Begin = leftChunk.Begin
			if leftEndOffset > vOffset(rightChunk.End) {
				rightChunk.End = leftChunk.End
			}
			chunks = append(chunks[:c-1], chunks[c:]...)
			c--
		}
	}
	return chunks
}

func squash(chunks []bgzf.Chunk) []bgzf.Chunk {
	if len(chunks) == 0 {
		return nil
	}
	left := chunks[0].Begin
	right := chunks[0].End
	for _, c := range chunks[1:] {
		if vOffset(c.End) > vOffset(right) {
			right = c.End
		}
	}
	return []bgzf.Chunk{{Begin: left, End: right}}
}
