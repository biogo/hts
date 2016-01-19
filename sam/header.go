// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	errDupReference  = errors.New("sam: duplicate reference name")
	errDupReadGroup  = errors.New("sam: duplicate read group name")
	errDupProgram    = errors.New("sam: duplicate program name")
	errUsedReference = errors.New("sam: reference already used")
	errUsedReadGroup = errors.New("sam: read group already used")
	errUsedProgram   = errors.New("sam: program already used")
	errBadLen        = errors.New("sam: reference length out of range")
)

// SortOrder indicates the sort order of a SAM or BAM file.
type SortOrder int

const (
	UnknownOrder SortOrder = iota
	Unsorted
	QueryName
	Coordinate
)

var (
	sortOrder = [...]string{
		UnknownOrder: "unknown",
		Unsorted:     "unsorted",
		QueryName:    "queryname",
		Coordinate:   "coordinate",
	}
	sortOrderMap = map[string]SortOrder{
		"unknown":    UnknownOrder,
		"unsorted":   Unsorted,
		"queryname":  QueryName,
		"coordinate": Coordinate,
	}
)

// String returns the string representation of a SortOrder.
func (so SortOrder) String() string {
	if so < Unsorted || so > Coordinate {
		return sortOrder[UnknownOrder]
	}
	return sortOrder[so]
}

// GroupOrder indicates the grouping order of a SAM or BAM file.
type GroupOrder int

const (
	GroupUnspecified GroupOrder = iota
	GroupNone
	GroupQuery
	GroupReference
)

var (
	groupOrder = [...]string{
		GroupUnspecified: "none",
		GroupNone:        "none",
		GroupQuery:       "query",
		GroupReference:   "reference",
	}
	groupOrderMap = map[string]GroupOrder{
		"none":      GroupNone,
		"query":     GroupQuery,
		"reference": GroupReference,
	}
)

// String returns the string representation of a GroupOrder.
func (g GroupOrder) String() string {
	if g < GroupNone || g > GroupReference {
		return groupOrder[GroupUnspecified]
	}
	return groupOrder[g]
}

type set map[string]int32

// Header is a SAM or BAM header.
type Header struct {
	Version    string
	SortOrder  SortOrder
	GroupOrder GroupOrder
	otherTags  []tagPair

	refs       []*Reference
	rgs        []*ReadGroup
	progs      []*Program
	seenRefs   set
	seenGroups set
	seenProgs  set

	Comments []string
}

type tagPair struct {
	tag   Tag
	value string
}

// NewHeader returns a new Header based on the given text and list
// of References. If there is a conflict between the text and the
// given References NewHeader will return a non-nil error.
func NewHeader(text []byte, r []*Reference) (*Header, error) {
	var err error
	bh := &Header{
		refs:       r,
		seenRefs:   set{},
		seenGroups: set{},
		seenProgs:  set{},
	}
	for i, r := range bh.refs {
		r.id = int32(i)
	}
	if text != nil {
		err = bh.UnmarshalText(text)
		if err != nil {
			return nil, err
		}
	}
	return bh, nil
}

// Get returns the string representation of the value associated with the
// given header line tag. If the tag is not present the empty string is returned.
func (bh *Header) Get(t Tag) string {
	switch t {
	case versionTag:
		return bh.Version
	case sortOrderTag:
		return bh.SortOrder.String()
	case groupOrderTag:
		return bh.GroupOrder.String()
	}
	for _, tp := range bh.otherTags {
		if t == tp.tag {
			return tp.value
		}
	}
	return ""
}

// Set sets the value associated with the given header line tag to the specified
// value. If value is the empty string and the tag may be absent, it is deleted
// or set to a meaningful default (SO:UnknownOrder and GO:GroupUnspecified),
// otherwise an error is returned.
func (bh *Header) Set(t Tag, value string) error {
	switch t {
	case versionTag:
		if value == "" {
			return errBadHeader
		}
		bh.Version = value
	case sortOrderTag:
		if value == "" {
			bh.SortOrder = UnknownOrder
			return nil
		}
		sortOrder, ok := sortOrderMap[value]
		if !ok {
			return errBadHeader
		}
		bh.SortOrder = sortOrder
	case groupOrderTag:
		if value == "" {
			bh.GroupOrder = GroupUnspecified
			return nil
		}
		groupOrder, ok := groupOrderMap[value]
		if !ok {
			return errBadHeader
		}
		bh.GroupOrder = groupOrder
	default:
		if value == "" {
			for i, tp := range bh.otherTags {
				if t == tp.tag {
					copy(bh.otherTags[i:], bh.otherTags[i+1:])
					bh.otherTags = bh.otherTags[:len(bh.otherTags)-1]
					return nil
				}
			}
		} else {
			for i, tp := range bh.otherTags {
				if t == tp.tag {
					bh.otherTags[i].value = value
					return nil
				}
			}
			bh.otherTags = append(bh.otherTags, tagPair{tag: t, value: value})
		}
	}
	return nil
}

// Clone returns a deep copy of the receiver.
func (bh *Header) Clone() *Header {
	c := &Header{
		Version:    bh.Version,
		SortOrder:  bh.SortOrder,
		GroupOrder: bh.GroupOrder,
		otherTags:  append([]tagPair(nil), bh.otherTags...),
		Comments:   append([]string(nil), bh.Comments...),
		refs:       make([]*Reference, len(bh.refs)),
		rgs:        make([]*ReadGroup, len(bh.rgs)),
		progs:      make([]*Program, len(bh.progs)),
		seenRefs:   make(set, len(bh.seenRefs)),
		seenGroups: make(set, len(bh.seenGroups)),
		seenProgs:  make(set, len(bh.seenProgs)),
	}

	for i, r := range bh.refs {
		if r == nil {
			continue
		}
		c.refs[i] = new(Reference)
		*c.refs[i] = *r
	}
	for i, r := range bh.rgs {
		c.rgs[i] = new(ReadGroup)
		*c.rgs[i] = *r
	}
	for i, p := range bh.progs {
		c.progs[i] = new(Program)
		*c.progs[i] = *p
	}
	for k, v := range bh.seenRefs {
		c.seenRefs[k] = v
	}
	for k, v := range bh.seenGroups {
		c.seenGroups[k] = v
	}
	for k, v := range bh.seenProgs {
		c.seenProgs[k] = v
	}

	return c
}

// MarshalText implements the encoding.TextMarshaler interface.
func (bh *Header) MarshalText() ([]byte, error) {
	var buf bytes.Buffer
	if bh.Version != "" {
		if bh.GroupOrder == GroupUnspecified {
			fmt.Fprintf(&buf, "@HD\tVN:%s\tSO:%s", bh.Version, bh.SortOrder)
		} else {
			fmt.Fprintf(&buf, "@HD\tVN:%s\tSO:%s\tGO:%s", bh.Version, bh.SortOrder, bh.GroupOrder)
		}
		for _, tp := range bh.otherTags {
			fmt.Fprintf(&buf, "\t%s:%s", tp.tag, tp.value)
		}
		buf.WriteByte('\n')
	}
	for _, r := range bh.refs {
		fmt.Fprintf(&buf, "%s\n", r)
	}
	for _, rg := range bh.rgs {
		fmt.Fprintf(&buf, "%s\n", rg)
	}
	for _, p := range bh.progs {
		fmt.Fprintf(&buf, "%s\n", p)
	}
	for _, co := range bh.Comments {
		fmt.Fprintf(&buf, "@CO\t%s\n", co)
	}
	return buf.Bytes(), nil
}

// MarshalBinary implements the encoding.BinaryMarshaler.
func (bh *Header) MarshalBinary() ([]byte, error) {
	b := &bytes.Buffer{}
	err := bh.EncodeBinary(b)
	if err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

// EncodeBinary writes a binary encoding of the Header to the given io.Writer.
// The format of the encoding is defined in the SAM specification, section 4.2.
func (bh *Header) EncodeBinary(w io.Writer) error {
	wb := &errWriter{w: w}

	binary.Write(wb, binary.LittleEndian, bamMagic)
	text, _ := bh.MarshalText()
	binary.Write(wb, binary.LittleEndian, int32(len(text)))
	wb.Write(text)
	binary.Write(wb, binary.LittleEndian, int32(len(bh.refs)))

	if !validInt32(len(bh.refs)) {
		return errors.New("sam: value out of range")
	}
	var name []byte
	for _, r := range bh.refs {
		name = append(name, []byte(r.name)...)
		name = append(name, 0)
		binary.Write(wb, binary.LittleEndian, int32(len(name)))
		wb.Write(name)
		name = name[:0]
		binary.Write(wb, binary.LittleEndian, r.lRef)
	}
	if wb.err != nil {
		return wb.err
	}

	return nil
}

type errWriter struct {
	w   io.Writer
	err error
}

func (w *errWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	var n int
	n, w.err = w.w.Write(p)
	return n, w.err
}

// Validate checks r against the Header for record validity according to the
// SAM specification:
//
//  - a program auxiliary field must refer to a program listed in the header
//  - a read group auxiliary field must refer to a read group listed in the
//    header and these must agree on platform unit and library.
func (bh *Header) Validate(r *Record) error {
	rp := r.AuxFields.Get(programTag)
	found := false
	for _, hp := range bh.Progs() {
		if hp.UID() == rp.Value() {
			found = true
			break
		}
	}
	if !found && len(bh.Progs()) != 0 {
		return fmt.Errorf("sam: program uid not found: %v", rp.Value())
	}

	rg := r.AuxFields.Get(readGroupTag)
	found = false
	for _, hg := range bh.RGs() {
		if hg.Name() == rg.Value() {
			rPlatformUnit := r.AuxFields.Get(platformUnitTag).Value()
			if rPlatformUnit != hg.PlatformUnit() {
				return fmt.Errorf("sam: mismatched platform for read group %s: %v != %v: %v", hg.Name(), rPlatformUnit, hg.platformUnit)
			}
			rLibrary := r.AuxFields.Get(libraryTag).Value()
			if rLibrary != hg.Library() {
				return fmt.Errorf("sam: mismatched library for read group %s: %v != %v: %v", hg.Name(), rLibrary, hg.library)
			}
			found = true
			break
		}
	}
	if !found && len(bh.RGs()) != 0 {
		return fmt.Errorf("sam: read group not found: %v", rg.Value())
	}

	return nil
}

// Refs returns the Header's list of References. The returned slice
// should not be altered.
func (bh *Header) Refs() []*Reference {
	return bh.refs
}

// RGs returns the Header's list of ReadGroups. The returned slice
// should not be altered.
func (bh *Header) RGs() []*ReadGroup {
	return bh.rgs
}

// Progs returns the Header's list of Programs. The returned slice
// should not be altered.
func (bh *Header) Progs() []*Program {
	return bh.progs
}

// AddReference adds r to the Header.
func (bh *Header) AddReference(r *Reference) error {
	if dupID, dup := bh.seenRefs[r.name]; dup {
		er := bh.refs[dupID]
		if equalRefs(er, r) {
			return nil
		} else if !equalRefs(r, &Reference{id: er.id, name: er.name, lRef: er.lRef}) {
			return errDupReference
		}
		if r.md5 == "" {
			r.md5 = er.md5
		}
		if r.assemID == "" {
			r.assemID = er.assemID
		}
		if r.species == "" {
			r.species = er.species
		}
		if r.uri == nil {
			r.uri = er.uri
		}
		bh.refs[dupID] = r
		return nil
	}
	if r.id >= 0 {
		return errUsedReference
	}
	r.id = int32(len(bh.refs))
	bh.seenRefs[r.name] = r.id
	bh.refs = append(bh.refs, r)
	return nil
}

// AddReadGroup adds rg to the Header.
func (bh *Header) AddReadGroup(rg *ReadGroup) error {
	if _, ok := bh.seenGroups[rg.name]; ok {
		return errDupReadGroup
	}
	if rg.id >= 0 {
		return errUsedReadGroup
	}
	rg.id = int32(len(bh.rgs))
	bh.seenGroups[rg.name] = rg.id
	bh.rgs = append(bh.rgs, rg)
	return nil
}

// AddProgram adds p to the Header.
func (bh *Header) AddProgram(p *Program) error {
	if _, ok := bh.seenProgs[p.uid]; ok {
		return errDupProgram
	}
	if p.id >= 0 {
		return errUsedProgram
	}
	p.id = int32(len(bh.progs))
	bh.seenProgs[p.uid] = p.id
	bh.progs = append(bh.progs, p)
	return nil
}
