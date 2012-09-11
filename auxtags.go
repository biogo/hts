// Copyright ©2012 Dan Kortschak <dan.kortschak@adelaide.edu.au>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package bam

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"unsafe"
)

// An Aux represents an auxilliary tag data field from a SAM alignment record.
type Aux []byte

var (
	jumps = [256]int{
		'A': 1,
		'c': 1, 'C': 1,
		's': 2, 'S': 2,
		'i': 4, 'I': 4,
		'f': 4,
		'Z': -1,
		'H': -1,
		'B': -1,
	}
	auxTypes = [256]byte{
		'A': 'A',
		'c': 'i', 'C': 'i',
		's': 'i', 'S': 'i',
		'i': 'i', 'I': 'i',
		'f': 'f',
		'Z': 'Z',
		'H': 'H',
		'B': 'B',
	}
)

// parseAux examines the data of a SAM record's OPT fields,
// returning a slice of Aux that are backed by the original data.
func parseAux(aux []byte) (aa []Aux) {
	for i := 0; i+2 < len(aux); {
		t := aux[i+2]
		switch j := jumps[t]; {
		case j > 0:
			j += 3
			aa = append(aa, Aux(aux[i:i+j]))
			i += j
		case j < 0:
			switch t {
			case 'Z', 'H':
				var (
					j int
					v byte
				)
				for j, v = range aux[i:] {
					if v == 0 { // C string termination
						break // Truncate terminal zero.
					}
				}
				aa = append(aa, Aux(aux[i:i+j]))
				i += j + 1
			case 'B':
				var length int32
				err := binary.Read(bytes.NewBuffer([]byte(aux[i+4:i+8])), Endian, &length)
				if err != nil {
					panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
				}
				j = int(length)*jumps[aux[i+3]] + int(unsafe.Sizeof(length)) + 4
				aa = append(aa, Aux(aux[i:i+j]))
				i += j
			}
		default:
			panic(fmt.Sprintf("bam: unrecognised optional field type: %q", t))
		}
	}
	return
}

// buildAux constructs a single byte slice that represents a slice of Aux.
func buildAux(aa []Aux) (aux []byte) {
	for _, a := range aa {
		// TODO: validate each 'a'
		aux = append(aux, []byte(a)...)
		switch a.Type() {
		case 'Z', 'H':
			aux = append(aux, 0)
		}
	}
	return
}

// String returns the string representation of an Aux type.
func (a Aux) String() string {
	return fmt.Sprintf("%s:%c:%v", []byte(a[:2]), auxTypes[a.Type()], a.Value())
}

// A Tag represents an auxilliary tag label.
type Tag [2]byte

// String returns a string representation of a Tag.
func (t Tag) String() string { return string(t[:]) }

// Tag returns the string representation of the tag ID.
func (a Aux) Tag() Tag { var t Tag; copy(t[:], a[:2]); return t }

// Type returns a byte corresponding to the type of the auxilliary tag.
// Returned values are in {'A', 'c', 'C', 's', 'S', 'i', 'I', 'f', 'Z', 'H', 'B'}.
func (a Aux) Type() byte { return a[2] }

// Value returns v containing the value of the auxilliary tag.
func (a Aux) Value() (v interface{}) {
	switch t := a.Type(); t {
	case 'A':
		return a[3]
	case 'c':
		return int8(a[3])
	case 'C':
		return uint(a[3])
	case 's':
		s := int16(0)
		err := binary.Read(bytes.NewBuffer([]byte(a[4:6])), Endian, &s)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		return s
	case 'S':
		S := uint16(0)
		err := binary.Read(bytes.NewBuffer([]byte(a[4:6])), Endian, &S)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		return S
	case 'i':
		i := int32(0)
		err := binary.Read(bytes.NewBuffer([]byte(a[4:8])), Endian, &i)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		return i
	case 'I':
		I := uint32(0)
		err := binary.Read(bytes.NewBuffer([]byte(a[4:8])), Endian, &I)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		return I
	case 'f':
		f := float32(0)
		err := binary.Read(bytes.NewBuffer([]byte(a[4:8])), Endian, &f)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		return f
	case 'Z': // Z and H Require that parsing stops before the terminating zero.
		return string(a[3:])
	case 'H':
		h := make([]byte, hex.DecodedLen(len(a[3:])))
		_, err := hex.Decode(h, []byte(a[3:]))
		if err != nil {
			panic(fmt.Sprintf("bam: hex decoding error: %v", err))
		}
		return h
	case 'B':
		var length int32
		err := binary.Read(bytes.NewBuffer([]byte(a[4:8])), Endian, &length)
		if err != nil {
			panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
		}
		switch t := a[3]; t {
		case 'c':
			c := a[4:]
			return *(*[]int8)(unsafe.Pointer(&c))
		case 'C':
			return []uint8(a[4:])
		case 's':
			Bs := make([]int16, length)
			err := binary.Read(bytes.NewBuffer([]byte(a[8:])), Endian, &Bs)
			if err != nil {
				panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
			}
			return Bs
		case 'S':
			BS := make([]uint16, length)
			err := binary.Read(bytes.NewBuffer([]byte(a[8:])), Endian, &BS)
			if err != nil {
				panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
			}
			return BS
		case 'i':
			Bi := make([]int32, length)
			err := binary.Read(bytes.NewBuffer([]byte(a[8:])), Endian, &Bi)
			if err != nil {
				panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
			}
			return Bi
		case 'I':
			BI := make([]uint32, length)
			err := binary.Read(bytes.NewBuffer([]byte(a[8:])), Endian, &BI)
			if err != nil {
				panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
			}
			return BI
		case 'f':
			Bf := make([]float32, length)
			err := binary.Read(bytes.NewBuffer([]byte(a[8:])), Endian, &Bf)
			if err != nil {
				panic(fmt.Sprintf("bam: binary.Read failed: %v", err))
			}
			return Bf
		default:
			panic(fmt.Sprintf("bam: unknown array type %q", t))
		}
	default:
		panic(fmt.Sprintf("bam: unknown type %q", t))
	}
	return
}
