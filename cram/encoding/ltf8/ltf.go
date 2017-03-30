// Copyright ©2017 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package ltf8 provides LTF-8 integer encoding.
package ltf8

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

// Uint64Len returns the number of bytes required to encode u.
func Uint64Len(u uint64) int {
	switch {
	case u < 0x80:
		return 1
	case u < 0x4000:
		return 2
	case u < 0x200000:
		return 3
	case u < 0x10000000:
		return 4
	case u < 0x800000000:
		return 5
	case u < 0x40000000000:
		return 6
	case u < 0x2000000000000:
		return 7
	case u < 0x100000000000000:
		return 8
	default:
		return 9
	}
}

// DecodeUint64 decodes the LTF-8 encoding in b and returns the uint64 value,
// its width in bytes and whether the decoding was successful. If the encoding
// is invalid, the expected length of b and false are returned. If b has zero
// length, zero, zero and false are returned.
func DecodeUint64(b []byte) (u uint64, n int, ok bool) {
	if len(b) == 0 {
		return 0, 0, false
	}
	n = nlo(b[0]) + 1
	if len(b) < n {
		return 0, n, false
	}
	switch n {
	case 1:
		u = uint64(b[0])
	case 2:
		u = uint64(b[1]) | uint64(b[0]&0x3f)<<8
	case 3:
		u = uint64(b[2]) | uint64(b[1])<<8 | uint64(b[0]&0x1f)<<16
	case 4:
		u = uint64(b[3]) | uint64(b[2])<<8 | uint64(b[1])<<16 | uint64(b[0]&0x0f)<<24
	case 5:
		u = uint64(b[4]) | uint64(b[3])<<8 | uint64(b[2])<<16 | uint64(b[1])<<24 | uint64(b[0]&0x07)<<32
	case 6:
		u = uint64(b[5]) | uint64(b[4])<<8 | uint64(b[3])<<16 | uint64(b[2])<<24 | uint64(b[1])<<32 | uint64(b[0]&0x03)<<40
	case 7:
		u = uint64(b[6]) | uint64(b[5])<<8 | uint64(b[4])<<16 | uint64(b[3])<<24 | uint64(b[2])<<32 | uint64(b[1])<<40 | uint64(b[0]&0x01)<<48
	case 8:
		u = uint64(b[7]) | uint64(b[6])<<8 | uint64(b[5])<<16 | uint64(b[4])<<24 | uint64(b[3])<<32 | uint64(b[2])<<40 | uint64(b[1])<<48
	case 9:
		u = uint64(b[8]) | uint64(b[7])<<8 | uint64(b[6])<<16 | uint64(b[5])<<24 | uint64(b[4])<<32 | uint64(b[3])<<40 | uint64(b[2])<<48 | uint64(b[1])<<56
	}
	return u, n, true
}

// Int64Len returns the number of bytes required to encode i.
func Int64Len(i int64) int {
	return Uint64Len(uint64(i))
}

// DecodeInt64 decodes the LTF-8 encoding in b and returns the int64 value,
// its width in bytes and whether the decoding was successful. If the encoding
// is invalid, the expected length of b and false are returned. If b has zero
// length, zero, zero and false are returned.
func DecodeInt64(b []byte) (i int64, n int, ok bool) {
	u, n, ok := DecodeUint64(b)
	return int64(u), n, ok
}

// EncodeUint64 encodes u as an LTF-8 into b, which must be large enough, and
// and returns the number of bytes written.
func EncodeUint64(b []byte, u uint64) int {
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
	case u < 0x800000000:
		_ = b[4]
		b[0] = byte(u>>32)&0x07 | 0xf0
		b[1] = byte(u >> 24)
		b[2] = byte(u >> 16)
		b[3] = byte(u >> 8)
		b[4] = byte(u)
		return 5
	case u < 0x40000000000:
		_ = b[5]
		b[0] = byte(u>>40)&0x03 | 0xf8
		b[1] = byte(u >> 32)
		b[2] = byte(u >> 24)
		b[3] = byte(u >> 16)
		b[4] = byte(u >> 8)
		b[5] = byte(u)
		return 6
	case u < 0x2000000000000:
		_ = b[6]
		b[0] = byte(u>>48)&0x01 | 0xfc
		b[1] = byte(u >> 40)
		b[2] = byte(u >> 32)
		b[3] = byte(u >> 24)
		b[4] = byte(u >> 16)
		b[5] = byte(u >> 8)
		b[6] = byte(u)
		return 7
	case u < 0x100000000000000:
		_ = b[7]
		b[0] = 0xfe
		b[1] = byte(u >> 48)
		b[2] = byte(u >> 40)
		b[3] = byte(u >> 32)
		b[4] = byte(u >> 24)
		b[5] = byte(u >> 16)
		b[6] = byte(u >> 8)
		b[7] = byte(u)
		return 8
	default:
		_ = b[8]
		b[0] = 0xff
		b[1] = byte(u >> 56)
		b[2] = byte(u >> 48)
		b[3] = byte(u >> 40)
		b[4] = byte(u >> 32)
		b[5] = byte(u >> 24)
		b[6] = byte(u >> 16)
		b[7] = byte(u >> 8)
		b[8] = byte(u)
		return 9
	}
}

// EncodeInt64 encodes u as an LTF-8 into b, which must be large enough, and
// and returns the number of bytes written.
func EncodeInt64(b []byte, i int64) int {
	return EncodeUint64(b, uint64(i))
}
