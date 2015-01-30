// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
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
	dupReference  = errors.New("sam: duplicate reference name")
	dupReadGroup  = errors.New("sam: duplicate read group name")
	dupProgram    = errors.New("sam: duplicate program name")
	usedReference = errors.New("sam: reference already used")
	usedReadGroup = errors.New("sam: read group already used")
	usedProgram   = errors.New("sam: program already used")
	badLen        = errors.New("sam: reference length out of range")
)

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

func (so SortOrder) String() string {
	if so < Unsorted || so > Coordinate {
		return sortOrder[UnknownOrder]
	}
	return sortOrder[so]
}

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

func (g GroupOrder) String() string {
	if g < GroupNone || g > GroupReference {
		return groupOrder[GroupUnspecified]
	}
	return groupOrder[g]
}

type set map[string]int32

type Header struct {
	Version    string
	SortOrder  SortOrder
	GroupOrder GroupOrder
	Comments   []string
	refs       []*Reference
	rgs        []*ReadGroup
	progs      []*Program
	seenRefs   set
	seenGroups set
	seenProgs  set
}

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

func (bh *Header) Clone() *Header {
	c := &Header{
		Version:    bh.Version,
		SortOrder:  bh.SortOrder,
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
		c.rgs[i].flowOrder = append([]byte(nil), r.flowOrder...)
		c.rgs[i].keySeq = append([]byte(nil), r.keySeq...)
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
			fmt.Fprintf(&buf, "@HD\tVN:%s\tSO:%s\n", bh.Version, bh.SortOrder)
		} else {
			fmt.Fprintf(&buf, "@HD\tVN:%s\tSO:%s\tGO:%s\n", bh.Version, bh.SortOrder, bh.GroupOrder)
		}
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

func (bh *Header) Refs() []*Reference {
	return bh.refs
}

func (bh *Header) RGs() []*ReadGroup {
	return bh.rgs
}

func (bh *Header) Progs() []*Program {
	return bh.progs
}

func (bh *Header) AddReference(r *Reference) error {
	if dupID, dup := bh.seenRefs[r.name]; dup {
		er := bh.refs[dupID]
		if *er == *r {
			return nil
		} else if tr := (Reference{id: er.id, name: er.name, lRef: er.lRef}); *r != tr {
			return dupReference
		}
		if r.md5 == nil {
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
		return usedReference
	}
	r.id = int32(len(bh.refs))
	bh.seenRefs[r.name] = r.id
	bh.refs = append(bh.refs, r)
	return nil
}

func (bh *Header) AddReadGroup(rg *ReadGroup) error {
	if _, ok := bh.seenGroups[rg.name]; ok {
		return dupReadGroup
	}
	if rg.id >= 0 {
		return usedReadGroup
	}
	rg.id = int32(len(bh.rgs))
	bh.seenGroups[rg.name] = rg.id
	bh.rgs = append(bh.rgs, rg)
	return nil
}

func (bh *Header) AddProgram(p *Program) error {
	if _, ok := bh.seenProgs[p.uid]; ok {
		return dupProgram
	}
	if p.id >= 0 {
		return usedProgram
	}
	p.id = int32(len(bh.progs))
	bh.seenProgs[p.uid] = p.id
	bh.progs = append(bh.progs, p)
	return nil
}
