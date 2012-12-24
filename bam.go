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

func validInt16(i int) bool    { return minInt16 <= i && i <= maxInt16 }
func validInt32(i int) bool    { return minInt32 <= i && i <= maxInt32 }
func validUint16(i int) bool   { return 0 <= i && i <= maxUint16 }
func validUint32(i int) bool   { return 0 <= i && int64(i) <= maxUint32 }
func validPos(i int) bool      { return 0 <= i && i <= 1<<29-1 }
func validTmpltLen(i int) bool { return -(1<<29) <= i && i <= 1<<29-1 }
func validLen(i int) bool      { return 1 <= i && i <= 1<<29-1 }

// calculate bin given an alignment covering [beg,end) (zero-based, half-close-half-open)
func reg2bin(beg, end int) uint16 {
	end--
	switch {
	case beg>>14 == end>>14:
		return uint16(((1<<15)-1)/7 + (beg >> 14))
	case beg>>17 == end>>17:
		return uint16(((1<<12)-1)/7 + (beg >> 17))
	case beg>>20 == end>>20:
		return uint16(((1<<9)-1)/7 + (beg >> 20))
	case beg>>23 == end>>23:
		return uint16(((1<<6)-1)/7 + (beg >> 23))
	case beg>>26 == end>>26:
		return uint16(((1<<3)-1)/7 + (beg >> 26))
	}
	return 0
}

const maxBin = (((1 << 18) - 1) / 7)

// calculate the list of bins that may overlap with region [beg,end) (zero-based)
func reg2bins(beg, end int) []uint16 {
	end--
	list := []uint16{0}
	for _, r := range []struct {
		offset, shift uint16
	}{
		{1, 26},
		{9, 23},
		{73, 20},
		{585, 17},
		{4681, 14},
	} {
		for k := r.offset + uint16(beg>>r.shift); k <= r.offset+uint16(end>>r.shift); k++ {
			list = append(list, k)
		}
	}
	return list
}
