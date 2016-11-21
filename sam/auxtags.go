// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"unsafe"
)

// ASCII is a printable ASCII character included in an Aux tag.
type ASCII byte

// Hex is a byte slice represented as a hex string in an Aux tag.
type Hex []byte

// Text is a byte slice represented as a string in an Aux tag.
type Text []byte

// An Aux represents an auxiliary data field from a SAM alignment record.
type Aux []byte

// NewAux returns a new Aux with the given tag, type and value. Acceptable value
// types and their corresponding SAM type are:
//
//  A - ASCII
//  c - int8
//  C - uint8
//  s - int16
//  S - uint16
//  i - int, uint or int32
//  I - int, uint or uint32
//  f - float32
//  Z - Text or string
//  H - Hex
//  B - []int8, []int16, []int32, []uint8, []uint16, []uint32 or []float32
//
// The handling of int and uint types is provided as a convenience - values must
// fit within either int32 or uint32 and are converted to the smallest possible
// representation.
//
func NewAux(t Tag, value interface{}) (Aux, error) {
	var a Aux
	switch v := value.(type) {
	case ASCII:
		a = Aux{t[0], t[1], 'A', byte(v)}
	case int:
		switch {
		case math.MinInt8 <= v && v <= math.MaxInt8:
			a = Aux{t[0], t[1], 'c', byte(v)}
		case math.MinInt16 <= v && v <= math.MaxInt16:
			a = Aux{t[0], t[1], 's', 0, 0}
			binary.LittleEndian.PutUint16(a[3:5], uint16(v))
		case math.MinInt32 <= v && v <= math.MaxInt32:
			a = Aux{t[0], t[1], 'i', 0, 0, 0, 0}
			binary.LittleEndian.PutUint32(a[3:7], uint32(v))
		default:
			return nil, fmt.Errorf("sam: integer value out of range %d > %d", v, math.MaxInt32)
		}
	case uint:
		switch {
		case v <= math.MaxUint8:
			a = Aux{t[0], t[1], 'C', byte(v)}
		case v <= math.MaxUint16:
			a = Aux{t[0], t[1], 'S', 0, 0}
			binary.LittleEndian.PutUint16(a[3:5], uint16(v))
		case v <= math.MaxUint32:
			a = Aux{t[0], t[1], 'I', 0, 0, 0, 0}
			binary.LittleEndian.PutUint32(a[3:7], uint32(v))
		default:
			return nil, fmt.Errorf("sam: unsigned integer value out of range %d > %d", v, uint(math.MaxUint32))
		}
	case int8:
		a = Aux{t[0], t[1], 'c', byte(v)}
	case uint8:
		a = Aux{t[0], t[1], 'C', v}
	case int16:
		a = Aux{t[0], t[1], 's', 0, 0}
		binary.LittleEndian.PutUint16(a[3:5], uint16(v))
	case uint16:
		a = Aux{t[0], t[1], 'S', 0, 0}
		binary.LittleEndian.PutUint16(a[3:5], v)
	case int32:
		a = Aux{t[0], t[1], 'i', 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(a[3:7], uint32(v))
	case uint32:
		a = Aux{t[0], t[1], 'I', 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(a[3:7], v)
	case float32:
		a = Aux{t[0], t[1], 'f', 0, 0, 0, 0}
		binary.LittleEndian.PutUint32(a[3:7], math.Float32bits(v))
	case Text:
		a = append(Aux{t[0], t[1], 'Z'}, v...)
	case string:
		a = append(Aux{t[0], t[1], 'Z'}, v...)
	case Hex:
		a = make(Aux, 3, len(v)+3)
		copy(a, Aux{t[0], t[1], 'H'})
		a = append(a, v...)
	default:
		rv := reflect.ValueOf(value)
		rt := rv.Type()
		if k := rt.Kind(); k != reflect.Array && k != reflect.Slice {
			return nil, fmt.Errorf("sam: unknown type %T", value)
		}
		l := rv.Len()
		if uint(l) > math.MaxUint32 {
			return nil, fmt.Errorf("sam: array too long")
		}
		a = Aux{t[0], t[1], 'B', 0xff, 0, 0, 0, 0}
		binary.LittleEndian.PutUint32([]byte(a[4:8]), uint32(l))

		switch rt.Elem().Kind() {
		case reflect.Int8:
			a[3] = 'c'
			value := value.([]int8)
			b := *(*[]byte)(unsafe.Pointer(&value))
			return append(a, b...), nil
		case reflect.Uint8:
			a[3] = 'C'
			return append(a, value.([]uint8)...), nil
		case reflect.Int16:
			a[3] = 's'
		case reflect.Uint16:
			a[3] = 'S'
		case reflect.Int32:
			a[3] = 'i'
		case reflect.Uint32:
			a[3] = 'I'
		case reflect.Float32:
			a[3] = 'f'
		default:
			return nil, fmt.Errorf("sam: unsupported array type: %T", value)
		}
		buf := bytes.NewBuffer(a)
		err := binary.Write(buf, binary.LittleEndian, value)
		a = buf.Bytes()
		if err != nil {
			return nil, fmt.Errorf("sam: failed to encode array: %v", err)
		}
	}
	return a, nil
}

// ParseAux returns an AUX parsed from the given text.
func ParseAux(text []byte) (Aux, error) {
	tf := bytes.SplitN(text, []byte{':'}, 3)
	if len(tf) != 3 || len(tf[1]) != 1 {
		return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
	}
	var value interface{}
	switch typ := tf[1][0]; typ {
	case 'A':
		if len(tf[2]) != 1 {
			return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
		}
		value = ASCII(tf[2][0])
	case 'i':
		i, err := strconv.Atoi(string(tf[2]))
		if err != nil {
			return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
		}
		if i < 0 {
			value = i
		} else {
			value = uint(i)
		}
	case 'f':
		f, err := strconv.ParseFloat(string(tf[2]), 32)
		if err != nil {
			return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
		}
		value = float32(f)
	case 'Z':
		value = Text(tf[2])
	case 'H':
		b := make([]byte, hex.DecodedLen(len(tf[2])))
		_, err := hex.Decode(b, tf[2])
		if err != nil {
			return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
		}
		value = Hex(b)
	case 'B':
		if tf[2][1] != ',' {
			return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
		}
		nf := bytes.Split(tf[2][2:], []byte{','})
		if len(nf) == 0 {
			return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
		}
		switch tf[2][0] {
		case 'c':
			a := make([]int8, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseInt(string(n), 0, 8)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = int8(v)
			}
			value = a
		case 'C':
			a := make([]uint8, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseUint(string(n), 0, 8)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = uint8(v)
			}
			value = a
		case 's':
			a := make([]int16, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseInt(string(n), 0, 16)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = int16(v)
			}
			value = a
		case 'S':
			a := make([]uint16, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseUint(string(n), 0, 16)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = uint16(v)
			}
			value = a
		case 'i':
			a := make([]int32, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseInt(string(n), 0, 32)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = int32(v)
			}
			value = a
		case 'I':
			a := make([]uint32, len(nf))
			for i, n := range nf {
				v, err := strconv.ParseUint(string(n), 0, 32)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = uint32(v)
			}
			value = a
		case 'f':
			a := make([]float32, len(nf))
			for i, n := range nf {
				f, err := strconv.ParseFloat(string(n), 32)
				if err != nil {
					return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
				}
				a[i] = float32(f)
			}
			value = a
		default:
			return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
		}
	default:
		return nil, fmt.Errorf("sam: invalid aux tag field: %q", text)
	}
	var t Tag
	if copy(t[:], tf[0]) != 2 {
		return nil, fmt.Errorf("sam: invalid aux tag: %q", tf[0])
	}
	aux, err := NewAux(t, value)
	if err != nil {
		return nil, fmt.Errorf("sam: invalid aux tag field: %v", err)
	}
	return aux, nil
}

var auxKind = [256]byte{
	'A': 'A',
	'c': 'i', 'C': 'i',
	's': 'i', 'S': 'i',
	'i': 'i', 'I': 'i',
	'f': 'f',
	'Z': 'Z',
	'H': 'H',
	'B': 'B',
}

// String returns the string representation of an Aux type.
func (a Aux) String() string {
	switch a.Type() {
	case 'A':
		return fmt.Sprintf("%s:%c:%c", []byte(a[:2]), a.Kind(), a.Value())
	case 'H':
		return fmt.Sprintf("%s:%c:%02x", []byte(a[:2]), a.Kind(), a.Value())
	case 'B':
		return fmt.Sprintf("%s:%c:%c:%v", []byte(a[:2]), a.Kind(), a[3], a.Value())
	}
	return fmt.Sprintf("%s:%c:%v", []byte(a[:2]), a.Kind(), a.Value())
}

// samAux implements SAM aux field formatting.
type samAux Aux

// String returns the string representation of an Aux type.
func (sa samAux) String() string {
	a := Aux(sa)
	switch a.Type() {
	case 'A':
		return fmt.Sprintf("%s:%c:%c", []byte(a[:2]), a.Kind(), a.Value())
	case 'H':
		return fmt.Sprintf("%s:%c:%02x", []byte(a[:2]), a.Kind(), a.Value())
	case 'B':
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "%s:%c:%c", []byte(a[:2]), a.Kind(), a[3])
		rv := reflect.ValueOf(a.Value())
		for i := 0; i < rv.Len(); i++ {
			fmt.Fprintf(&buf, ",%v", rv.Index(i).Interface())
		}
		return buf.String()
	}
	return fmt.Sprintf("%s:%c:%v", []byte(a[:2]), a.Kind(), a.Value())
}

// A Tag represents an auxiliary tag label.
type Tag [2]byte

var (
	headerTag       = Tag{'H', 'D'}
	versionTag      = Tag{'V', 'N'}
	sortOrderTag    = Tag{'S', 'O'}
	groupOrderTag   = Tag{'G', 'O'}
	refDictTag      = Tag{'S', 'Q'}
	refNameTag      = Tag{'S', 'N'}
	refLengthTag    = Tag{'L', 'N'}
	assemblyIDTag   = Tag{'A', 'S'}
	md5Tag          = Tag{'M', '5'}
	speciesTag      = Tag{'S', 'P'}
	uriTag          = Tag{'U', 'R'}
	readGroupTag    = Tag{'R', 'G'}
	centerTag       = Tag{'C', 'N'}
	descriptionTag  = Tag{'D', 'S'}
	dateTag         = Tag{'D', 'T'}
	flowOrderTag    = Tag{'F', 'O'}
	keySequenceTag  = Tag{'K', 'S'}
	libraryTag      = Tag{'L', 'B'}
	insertSizeTag   = Tag{'P', 'I'}
	platformTag     = Tag{'P', 'L'}
	platformUnitTag = Tag{'P', 'U'}
	sampleTag       = Tag{'S', 'M'}
	programTag      = Tag{'P', 'G'}
	idTag           = Tag{'I', 'D'}
	programNameTag  = Tag{'P', 'N'}
	commandLineTag  = Tag{'C', 'L'}
	previousProgTag = Tag{'P', 'P'}
	commentTag      = Tag{'C', 'O'}
)

// NewTag returns a Tag from the tag string. It panics is len(tag) != 2.
func NewTag(tag string) Tag {
	var t Tag
	if copy(t[:], tag) != 2 {
		panic("sam: illegal tag length")
	}
	return t
}

// String returns a string representation of a Tag.
func (t Tag) String() string { return string(t[:]) }

// Tag returns the Tag representation of the Aux tag ID.
func (a Aux) Tag() Tag { var t Tag; copy(t[:], a[:2]); return t }

// Type returns a byte corresponding to the type of the auxiliary tag.
// Returned values are in {'A', 'c', 'C', 's', 'S', 'i', 'I', 'f', 'Z', 'H', 'B'}.
func (a Aux) Type() byte { return a[2] }

// Kind returns a byte corresponding to the kind of the auxiliary tag.
// Returned values are in {'A', 'i', 'f', 'Z', 'H', 'B'}.
func (a Aux) Kind() byte { return auxKind[a[2]] }

// Value returns v containing the value of the auxiliary tag.
func (a Aux) Value() interface{} {
	switch t := a.Type(); t {
	case 'A':
		return a[3]
	case 'c':
		return int8(a[3])
	case 'C':
		return uint8(a[3])
	case 's':
		return int16(binary.LittleEndian.Uint16(a[3:5]))
	case 'S':
		return binary.LittleEndian.Uint16(a[3:5])
	case 'i':
		return int32(binary.LittleEndian.Uint32(a[3:7]))
	case 'I':
		return binary.LittleEndian.Uint32(a[3:7])
	case 'f':
		return math.Float32frombits(binary.LittleEndian.Uint32(a[3:7]))
	case 'Z': // Z and H Require that parsing stops before the terminating zero.
		return string(a[3:])
	case 'H':
		return []byte(a[3:])
	case 'B':
		length := int32(binary.LittleEndian.Uint32(a[4:8]))
		switch t := a[3]; t {
		case 'c':
			c := a[8:]
			return *(*[]int8)(unsafe.Pointer(&c))
		case 'C':
			return []uint8(a[8:])
		case 's':
			Bs := make([]int16, length)
			err := binary.Read(bytes.NewBuffer(a[8:]), binary.LittleEndian, &Bs)
			if err != nil {
				panic(fmt.Sprintf("sam: binary.Read of s field failed: %v", err))
			}
			return Bs
		case 'S':
			BS := make([]uint16, length)
			err := binary.Read(bytes.NewBuffer(a[8:]), binary.LittleEndian, &BS)
			if err != nil {
				panic(fmt.Sprintf("sam: binary.Read of S field failed: %v", err))
			}
			return BS
		case 'i':
			Bi := make([]int32, length)
			err := binary.Read(bytes.NewBuffer(a[8:]), binary.LittleEndian, &Bi)
			if err != nil {
				panic(fmt.Sprintf("sam: binary.Read of i field failed: %v", err))
			}
			return Bi
		case 'I':
			BI := make([]uint32, length)
			err := binary.Read(bytes.NewBuffer(a[8:]), binary.LittleEndian, &BI)
			if err != nil {
				panic(fmt.Sprintf("sam: binary.Read of I field failed: %v", err))
			}
			return BI
		case 'f':
			Bf := make([]float32, length)
			err := binary.Read(bytes.NewBuffer(a[8:]), binary.LittleEndian, &Bf)
			if err != nil {
				panic(fmt.Sprintf("sam: binary.Read of f field failed: %v", err))
			}
			return Bf
		default:
			return fmt.Errorf("%!(UNKNOWN ARRAY type=%c)", t)
		}
	default:
		return fmt.Errorf("%!(UNKNOWN type=%c)", t)
	}
}

func (a Aux) matches(tag []byte) bool {
	return a[1] == tag[1] && a[0] == tag[0]
}

// AuxFields is a set of auxiliary fields.
type AuxFields []Aux

// Get returns the auxiliary field identified by the given tag, or nil
// if no field matches.
func (a AuxFields) Get(tag Tag) Aux {
	for _, f := range a {
		if f.Tag() == tag {
			return f
		}
	}
	return nil
}
