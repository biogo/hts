// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"encoding/binary"
)

var Endian = binary.LittleEndian

// BAM index type
type baiFileHeaderFixed struct {
	magic [4]byte "BAI\x01" // Magic number for BAI files.
	nRef  int32
}

type offset uint64

type chunk struct {
	chunkBeg offset
	chunkEnd offset
}

type baiBin struct {
	bin     uint32
	nChunks int32
	chunks  []chunk
}

type baiBins struct {
	nBins int32
	bins  []baiBin
}

type baiIntervals struct {
	nIntv    int32
	iOffsets []offset
}

type baiIndex struct {
	baiBins
	baiIntervals
}

type baiFile struct {
	baiFileHeaderFixed
	indices []baiIndex
}

const (
	maxInt16  = int(int16(^uint16(0) >> 1))
	minInt16  = -int(maxInt16) - 1
	maxUint16 = int(^uint16(0))
	maxInt32  = int(int32(^uint32(0) >> 1))
	minInt32  = -int(maxInt32) - 1
	maxUint32 = int64(^uint32(0))
)

const wordBits = 29

func validInt16(i int) bool    { return minInt16 <= i && i <= maxInt16 }
func validInt32(i int) bool    { return minInt32 <= i && i <= maxInt32 }
func validUint16(i int) bool   { return 0 <= i && i <= maxUint16 }
func validUint32(i int) bool   { return 0 <= i && int64(i) <= maxUint32 }
func validPos(i int) bool      { return 0 <= i && i <= 1<<wordBits-1 }
func validTmpltLen(i int) bool { return -(1<<wordBits) <= i && i <= 1<<wordBits-1 }
func validLen(i int) bool      { return 1 <= i && i <= 1<<wordBits-1 }

const nextBinShift = 3

const (
	level0 = uint16(((1 << (iota * nextBinShift)) - 1) / 7)
	level1
	level2
	level3
	level4
	level5
)

const (
	level0Shift = wordBits - (iota * nextBinShift)
	level1Shift
	level2Shift
	level3Shift
	level4Shift
	level5Shift
)

// calculate bin given an alignment covering [beg,end) (zero-based, half-close-half-open)
func reg2bin(beg, end int) uint16 {
	end--
	switch {
	case beg>>level5Shift == end>>level5Shift:
		return level5 + uint16(beg>>level5Shift)
	case beg>>level4Shift == end>>level4Shift:
		return level4 + uint16(beg>>level4Shift)
	case beg>>level3Shift == end>>level3Shift:
		return level3 + uint16(beg>>level3Shift)
	case beg>>level2Shift == end>>level2Shift:
		return level2 + uint16(beg>>level2Shift)
	case beg>>level1Shift == end>>level1Shift:
		return level1 + uint16(beg>>level1Shift)
	}
	return level0
}

// calculate the list of bins that may overlap with region [beg,end) (zero-based)
func reg2bins(beg, end int) []uint16 {
	end--
	list := []uint16{level0}
	for _, r := range []struct {
		offset, shift uint16
	}{
		{level1, level1Shift},
		{level2, level2Shift},
		{level3, level3Shift},
		{level4, level4Shift},
		{level5, level5Shift},
	} {
		for k := r.offset + uint16(beg>>r.shift); k <= r.offset+uint16(end>>r.shift); k++ {
			list = append(list, k)
		}
	}
	return list
}
