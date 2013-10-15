// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"code.google.com/p/biogo.bam/bgzf/egzip"

	"errors"
	"os"
)

const (
	BlockSize    = 0x0ff00 // Size of input data block.
	MaxBlockSize = 0x10000 // Maximum size of output block.
)

const (
	bgzfExtra = "BC\x02\x00\x00\x00"
	minFrame  = 20 + len(bgzfExtra) // Minimum bgzf header+footer length.

	// Magic EOF block.
	magicBlock = "\x1f\x8b\x08\x04\x00\x00\x00\x00\x00\xff\x06\x00\x42\x43\x02\x00\x1b\x00\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00"
)

var bgzfExtraPrefix = []byte(bgzfExtra[:4])

func compressBound(srcLen int) int {
	return srcLen + srcLen>>12 + srcLen>>14 + srcLen>>25 + 13 + minFrame
}

func init() {
	if compressBound(BlockSize) > MaxBlockSize {
		panic("bam: BlockSize too large")
	}
}

var (
	NewBlock         = egzip.NewBlock
	ErrClosed        = errors.New("bgzf: use of closed writer")
	ErrBlockOverflow = errors.New("bgzf: block overflow")
	ErrWrongFileType = errors.New("bgzf: file is a directory")
)

func CheckEOF(f *os.File) (bool, error) {
	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		return false, ErrWrongFileType
	}

	b := make([]byte, len(magicBlock))
	_, err = f.ReadAt(b, fi.Size()-int64(len(magicBlock)))
	if err != nil {
		return false, err
	}
	for i := range b {
		if b[i] != magicBlock[i] {
			return false, nil
		}
	}
	return true, nil
}
