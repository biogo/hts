// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package itf8 provides ITF-8 integer encoding.
package itf8

var pop = [16]byte{
	0:  8,
	1:  7,
	4:  3,
	5:  6,
	6:  1,
	9:  4,
	10: 2,
	11: 5,
	14: 0,
}

// nlo returns the number of leading set bits in x.
func nlo(x byte) int {
	x = ^x
	x |= x >> 1
	x |= x >> 2
	x |= x >> 4
	x *= 27
	return int(pop[x>>4])
}

// Uint32Len returns the number of bytes required to encode u.
func Uint32Len(u uint32) int {
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

// DecodeUint32 decodes the ITF-8 encoding in b and returns the uint32 value,
// its width in bytes and whether the decoding was successful. If the encoding
// is invalid, the expected length of b and false are returned. If b has zero
// length, zero, zero and false are returned.
func DecodeUint32(b []byte) (u uint32, n int, ok bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	n = nlo(b[0]&0xf0) + 1
	if len(b) < n {
		return 0, n, false
	}
	switch n {
	case 1:
		u = uint32(b[0])
	case 2:
		u = uint32(b[1]) | uint32(b[0]&0x3f)<<8
	case 3:
		u = uint32(b[2]) | uint32(b[1])<<8 | uint32(b[0]&0x1f)<<16
	case 4:
		u = uint32(b[3]) | uint32(b[2])<<8 | uint32(b[1])<<16 | uint32(b[0]&0x0f)<<24
	case 5:
		u = uint32(b[4]&0x0f) | uint32(b[3])<<4 | uint32(b[2])<<12 | uint32(b[1])<<20 | uint32(b[0]&0x0f)<<28
	}
	return u, n, true
}

// Int32Len returns the number of bytes required to encode i.
func Int32Len(i int32) int {
	return Uint32Len(uint32(i))
}

// DecodeInt32 decodes the ITF-8 encoding in b and returns the int32 value,
// its width in bytes and whether the decoding was successful. If the encoding
// is invalid, the expected length of b and false are returned. If b has zero
// length, zero, zero and false are returned.
func DecodeInt32(b []byte) (i int32, n int, ok bool) {
	u, n, ok := DecodeUint32(b)
	return int32(u), n, ok
}

// EncodeUint32 encodes u as an ITF-8 into b, which must be large enough, and
// and returns the number of bytes written.
func EncodeUint32(b []byte, u uint32) int {
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

// EncodeInt32 encodes i as an ITF-8 into b, which must be large enough, and
// and returns the number of bytes written.
func EncodeInt32(b []byte, i int32) int {
	return EncodeUint32(b, uint32(i))
}
