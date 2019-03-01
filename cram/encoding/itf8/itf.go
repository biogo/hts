// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package itf8 provides ITF-8 integer encoding.
//
// ITF-8 integer encoding is specified in the CRAM format specification
// section 2.3.
package itf8

import "math/bits"

// Len returns the number of bytes required to encode u.
func Len(v int32) int {
	u := uint32(v)
	switch {
	case u < 0x80:
		return 1
	case u < 0x4000:
		return 2
	case u < 0x200000:
		return 3
	case u < 0x10000000:
		return 4
	default:
		return 5
	}
}

// Decode decodes the ITF-8 encoding in b and returns the int32 value, its
// width in bytes and whether the decoding was successful. If the encoding
// is invalid, the expected length of b and false are returned. If b has zero
// length, zero, zero and false are returned.
func Decode(b []byte) (v int32, n int, ok bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	n = bits.LeadingZeros8(^(b[0] & 0xf0)) + 1
	if len(b) < n {
		return 0, n, false
	}
	switch n {
	case 1:
		v = int32(b[0])
	case 2:
		v = int32(b[1]) | int32(b[0]&0x3f)<<8
	case 3:
		v = int32(b[2]) | int32(b[1])<<8 | int32(b[0]&0x1f)<<16
	case 4:
		v = int32(b[3]) | int32(b[2])<<8 | int32(b[1])<<16 | int32(b[0]&0x0f)<<24
	case 5:
		v = int32(b[4]&0x0f) | int32(b[3])<<4 | int32(b[2])<<12 | int32(b[1])<<20 | int32(b[0]&0x0f)<<28
	}
	return v, n, true
}

// Encode encodes v as an ITF-8 into b, which must be large enough, and
// and returns the number of bytes written.
func Encode(b []byte, v int32) int {
	u := uint32(v)
	switch {
	case u < 0x80:
		b[0] = byte(u)
		return 1
	case u < 0x4000:
		_ = b[1]
		b[0] = byte(u>>8)&0x3f | 0x80
		b[1] = byte(u)
		return 2
	case u < 0x200000:
		_ = b[2]
		b[0] = byte(u>>16)&0x1f | 0xc0
		b[1] = byte(u >> 8)
		b[2] = byte(u)
		return 3
	case u < 0x10000000:
		_ = b[3]
		b[0] = byte(u>>24)&0x0f | 0xe0
		b[1] = byte(u >> 16)
		b[2] = byte(u >> 8)
		b[3] = byte(u)
		return 4
	default:
		_ = b[4]
		b[0] = byte(u>>28) | 0xf0
		b[1] = byte(u >> 20)
		b[2] = byte(u >> 12)
		b[3] = byte(u >> 2)
		b[4] = byte(u)
		return 5
	}
}
