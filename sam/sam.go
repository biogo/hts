// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

const (
	maxInt32 = int(int32(^uint32(0) >> 1))
	minInt32 = -int(maxInt32) - 1
)

const wordBits = 29

func validInt32(i int) bool    { return minInt32 <= i && i <= maxInt32 }
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
