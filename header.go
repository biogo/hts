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
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

var (
	badHeader     = errors.New("bam: malformed header line")
	dupReference  = errors.New("bam: duplicate reference name")
	dupReadGroup  = errors.New("bam: duplicate read group name")
	dupProgram    = errors.New("bam: duplicate program name")
	usedReference = errors.New("bam: reference already used")
	usedReadGroup = errors.New("bam: read group already used")
	usedProgram   = errors.New("bam: program already used")
	dupRefLen     = errors.New("bam: repeated reference length")
	badLen        = errors.New("bam: reference length out of range")
	dupTag        = errors.New("bam: duplicate field")
)

type tag [2]byte

type headerRecDef struct {
	tag      tag
	minLen   int
	unique   bool
	required bool
}

var (
	headerTag       = tag{'H', 'D'}
	versionTag      = tag{'V', 'N'}
	sortOrderTag    = tag{'S', 'O'}
	refDictTag      = tag{'S', 'Q'}
	refNameTag      = tag{'S', 'N'}
	refLengthTag    = tag{'L', 'N'}
	assemblyIDTag   = tag{'A', 'S'}
	md5Tag          = tag{'M', '5'}
	speciesTag      = tag{'S', 'P'}
	uriTag          = tag{'U', 'R'}
	readGroupTag    = tag{'R', 'G'}
	centerTag       = tag{'C', 'N'}
	descriptionTag  = tag{'D', 'S'}
	dateTag         = tag{'D', 'T'}
	flowOrderTag    = tag{'F', 'O'}
	keySequenceTag  = tag{'K', 'S'}
	libraryTag      = tag{'L', 'B'}
	insertSizeTag   = tag{'P', 'I'}
	platformTag     = tag{'P', 'L'}
	platformUnitTag = tag{'P', 'U'}
	sampleTag       = tag{'S', 'M'}
	programTag      = tag{'P', 'G'}
	idTag           = tag{'I', 'D'}
	programNameTag  = tag{'P', 'N'}
	commandLineTag  = tag{'C', 'L'}
	previousProgTag = tag{'P', 'P'}
	commentTag      = tag{'C', 'O'}

	// "GO" unspecified

)

var bamMagic = [4]byte{'B', 'A', 'M', 0x1}

type SortOrder int

const (
	UnknownOrder SortOrder = iota
	Unsorted
	QueryName
	Coordinate
)

var (
	sortOrder = []string{
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

type set map[string]int32

type Header struct {
	Version    string
	SortOrder  SortOrder
	refs       []*Reference
	rgs        []*ReadGroup
	progs      []*Program
	comments   []string
	seenRefs   set
	seenGroups set
	seenProgs  set
}

func NewHeader(text []byte, r []*Reference) (*Header, error) {
	var err error
	bh := &Header{refs: r, seenRefs: set{}, seenGroups: set{}}
	for i, r := range bh.refs {
		r.id = int32(i)
	}
	if text != nil {
		err = bh.parseHeader(text)
		if err != nil {
			return nil, err
		}
	}
	return bh, nil
}

func (bh *Header) String() string {
	var refs = make([]string, len(bh.refs))
	for i, r := range bh.refs {
		refs[i] = r.String()
	}
	if bh.Version != "" {
		return fmt.Sprintf("@HD\tVN:%s\tSO:%s\n%v\n",
			bh.Version,
			bh.SortOrder,
			strings.Trim(strings.Join(refs, "\n"), "[]"))
	}
	return strings.Trim(strings.Join(refs, "\n"), "[]")
}

func (bh *Header) formatHeader() ([]byte, error) {
	return nil, nil
}

func (fh *Header) read(r io.Reader) error {
	var (
		lText, nRef int32
		err         error
	)
	var magic [4]byte
	err = binary.Read(r, Endian, &magic)
	if err != nil {
		return err
	}
	if magic != bamMagic {
		return errors.New("bam: magic number mismatch")
	}
	err = binary.Read(r, Endian, &lText)
	if err != nil {
		return err
	}
	text := make([]byte, lText)
	n, err := r.Read(text)
	if err != nil {
		return err
	}
	if n != int(lText) {
		return errors.New("bam: truncated header")
	}
	err = fh.parseHeader(text)
	if err != nil {
		return err
	}
	err = binary.Read(r, Endian, &nRef)
	if err != nil {
		return err
	}
	refs, err := readRefRecords(r, nRef)
	if err != nil {
		return err
	}
	for _, r := range refs {
		err = fh.AddReference(r)
		if err != nil {
			return err
		}
	}
	return nil
}

func readRefRecords(r io.Reader, n int32) ([]*Reference, error) {
	rr := make([]*Reference, n)
	var (
		lName int32
		err   error
	)
	for i := range rr {
		rr[i] = &Reference{id: int32(i)}
		err = binary.Read(r, Endian, &lName)
		if err != nil {
			return nil, err
		}
		name := make([]byte, lName)
		n, err := r.Read(name)
		if err != nil {
			return nil, err
		}
		if n != int(lName) || name[n-1] != 0 {
			return nil, errors.New("bam: truncated reference name")
		}
		rr[i].name = string(name[:n-1])
		err = binary.Read(r, Endian, &rr[i].lRef)
		if err != nil {
			return nil, err
		}
	}
	return rr, nil
}

func (bh *Header) parseHeader(text []byte) error {
	var t tag
	for i, l := range bytes.Split(text, []byte{'\n'}) {
		if len(l) > 0 && l[len(l)-1] == '\r' {
			l = l[:len(l)-1]
		}
		if len(l) == 0 {
			continue
		}
		if l[0] != '@' || len(l) < 3 {
			return badHeader
		}
		copy(t[:], l[1:3])
		var err error
		switch {
		case t == headerTag:
			err = headerLine(l, bh)
		case t == refDictTag:
			err = referenceLine(l, bh)
		case t == readGroupTag:
			err = readGroupLine(l, bh)
		case t == programTag:
			err = programLine(l, bh)
		case t == commentTag:
			err = commentLine(l, bh)
		default:
			return badHeader
		}
		if err != nil {
			return fmt.Errorf("%v: line %d: %q", err, i+1, l)
		}
	}

	return nil
}

func headerLine(l []byte, bh *Header) error {
	fields := bytes.Split(l, []byte{'\t'})
	if len(fields) < 2 {
		return badHeader
	}

	var t tag
	for _, f := range fields[1:] {
		if f[2] != ':' {
			return badHeader
		}
		copy(t[:], f[:2])
		fs := string(f[3:])
		switch {
		case t == versionTag:
			if bh.Version != "" {
				return badHeader
			}
			bh.Version = fs
		case t == sortOrderTag:
			if bh.SortOrder != UnknownOrder {
				return badHeader
			}
			bh.SortOrder = sortOrderMap[fs]
		default:
			return badHeader
		}
	}

	if bh.Version == "" {
		return badHeader
	}

	return nil
}

func referenceLine(l []byte, bh *Header) error {
	fields := bytes.Split(l, []byte{'\t'})
	if len(fields) < 3 {
		return badHeader
	}

	var (
		t        tag
		rf       = &Reference{}
		seen     = map[tag]struct{}{}
		nok, lok bool
		dupID    int32
		dup      bool
	)

	for _, f := range fields[1:] {
		if f[2] != ':' {
			return badHeader
		}
		copy(t[:], f[:2])
		if _, ok := seen[t]; ok {
			return dupTag
		}
		seen[t] = struct{}{}
		fs := string(f[3:])
		switch {
		case t == refNameTag:
			dupID, dup = bh.seenRefs[fs]
			rf.name = fs
			nok = true
		case t == refLengthTag:
			l, err := strconv.Atoi(fs)
			if err != nil {
				return badHeader
			}
			if !validLen(l) {
				return badLen
			}
			rf.lRef = int32(l)
			lok = true
		case t == assemblyIDTag:
			rf.assemID = fs
		case t == md5Tag:
			hb := [16]byte{}
			n, err := hex.Decode(hb[:], f[3:])
			if err != nil {
				return err
			}
			if n != 16 {
				return badHeader
			}
			rf.md5 = &hb
		case t == speciesTag:
			rf.species = fs
		case t == uriTag:
			var err error
			rf.uri, err = url.Parse(fs)
			if err != nil {
				return err
			}
			if rf.uri.Scheme != "http" || rf.uri.Scheme != "ftp" {
				rf.uri.Scheme = "file"
			}
		default:
			return badHeader
		}
	}

	if dup {
		if er := bh.refs[dupID]; *er == *rf {
			return nil
		} else if tr := (Reference{id: er.id, name: er.name, lRef: er.lRef}); *er != tr {
			return dupReference
		}
		bh.refs[dupID] = rf
		return nil
	}
	if !nok || !lok {
		return badHeader
	}
	id := int32(len(bh.refs))
	rf.id = id
	bh.seenRefs[rf.name] = id
	bh.refs = append(bh.refs, rf)

	return nil
}

const (
	iso8601Date     = "2006-01-02"
	iso8601TimeDate = "2006-01-02T15:04Z"
)

var iso8601 = []string{iso8601Date, iso8601TimeDate}

func readGroupLine(l []byte, bh *Header) error {
	fields := bytes.Split(l, []byte{'\t'})
	if len(fields) < 2 {
		return badHeader
	}

	var (
		t    tag
		rg   = &ReadGroup{}
		seen = map[tag]struct{}{}
		idok bool
	)

L:
	for _, f := range fields[1:] {
		if f[2] != ':' {
			return badHeader
		}
		copy(t[:], f[:2])
		if _, ok := seen[t]; ok {
			return dupTag
		}
		seen[t] = struct{}{}
		fs := string(f[3:])
		switch {
		case t == idTag:
			if _, ok := bh.seenRefs[fs]; ok {
				return dupReadGroup
			}
			rg.name = fs
			idok = true
		case t == centerTag:
			rg.center = fs
		case t == descriptionTag:
			rg.description = fs
		case t == dateTag:
			var err error
			for _, tf := range iso8601 {
				rg.date, err = time.Parse(tf, fs)
				if err == nil {
					continue L
				}
			}
			return err
		case t == flowOrderTag:
			rg.flowOrder = append([]byte(nil), fs...)
		case t == keySequenceTag:
			rg.keySeq = append([]byte(nil), fs...)
		case t == libraryTag:
			rg.library = fs
		case t == programTag:
			rg.program = fs
		case t == insertSizeTag:
			i, err := strconv.Atoi(fs)
			if err != nil {
				return err
			}
			if !validInt32(i) {
				return badLen
			}
			rg.insertSize = i
		case t == platformTag:
			rg.platform = fs
		case t == platformUnitTag:
			rg.platformUnit = fs
		case t == sampleTag:
			rg.sample = fs
		default:
			return badHeader
		}
	}

	if !idok {
		return badHeader
	}
	id := int32(len(bh.rgs))
	rg.id = id
	bh.seenGroups[rg.name] = id
	bh.rgs = append(bh.rgs, rg)

	return nil
}

func programLine(l []byte, bh *Header) error {
	fields := bytes.Split(l, []byte{'\t'})
	if len(fields) < 2 {
		return badHeader
	}

	var (
		t    tag
		p    = &Program{}
		seen = map[tag]struct{}{}
		idok bool
	)

	for _, f := range fields[1:] {
		if f[2] != ':' {
			return badHeader
		}
		copy(t[:], f[:2])
		if _, ok := seen[t]; ok {
			return dupTag
		}
		seen[t] = struct{}{}
		fs := string(f[3:])
		switch {
		case t == idTag:
			if _, ok := bh.seenProgs[fs]; ok {
				return dupProgram
			}
			p.uid = fs
			idok = true
		case t == programNameTag:
			p.name = fs
		case t == commandLineTag:
			p.command = fs
		case t == previousProgTag:
			p.previous = fs
		case t == versionTag:
			p.version = fs
		default:
			return badHeader
		}
	}

	if !idok {
		return badHeader
	}
	id := int32(len(bh.progs))
	p.id = id
	bh.seenProgs[p.uid] = id
	bh.progs = append(bh.progs, p)

	return nil
}

func commentLine(l []byte, bh *Header) error {
	fields := bytes.Split(l, []byte{'\t'})
	if len(fields) < 2 {
		return badHeader
	}
	bh.comments = append(bh.comments, string(fields[1]))
	return nil
}

func (bh *Header) Copy() *Header {
	c := &Header{
		Version:    bh.Version,
		SortOrder:  bh.SortOrder,
		comments:   append([]string(nil), bh.comments...),
		refs:       make([]*Reference, len(bh.refs)),
		rgs:        make([]*ReadGroup, len(bh.rgs)),
		progs:      make([]*Program, len(bh.progs)),
		seenRefs:   make(set, len(bh.seenRefs)),
		seenGroups: make(set, len(bh.seenGroups)),
		seenProgs:  make(set, len(bh.seenProgs)),
	}

	for i, r := range bh.refs {
		*c.refs[i] = *r
	}
	for i, r := range bh.rgs {
		*c.rgs[i] = *r
		c.rgs[i].flowOrder = append([]byte(nil), r.flowOrder...)
		c.rgs[i].keySeq = append([]byte(nil), r.keySeq...)
	}
	for i, p := range bh.progs {
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

func (bh *Header) Bytes() []byte {
	return []byte(bh.String())
}

func (bh *Header) Len() int {
	return len(bh.refs)
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
		if er := bh.refs[dupID]; *er == *r {
			return nil
		} else if tr := (Reference{id: er.id, name: er.name, lRef: er.lRef}); *er != tr {
			return dupReference
		}
		bh.refs[dupID] = r
		return nil
	}
	if r.id >= 0 {
		return usedReference
	}
	r.id = int32(len(bh.refs))
	bh.refs = append(bh.refs, r)
	return nil
}

func (bh *Header) AddReadGroup(r *ReadGroup) error {
	if _, ok := bh.seenGroups[r.name]; ok {
		return dupReadGroup
	}
	if r.id >= 0 {
		return usedReadGroup
	}
	r.id = int32(len(bh.rgs))
	bh.rgs = append(bh.rgs, r)
	return nil
}

func (bh *Header) AddProgram(r *ReadGroup) error {
	if _, ok := bh.seenProgs[r.name]; ok {
		return dupProgram
	}
	if r.id >= 0 {
		return usedProgram
	}
	r.id = int32(len(bh.rgs))
	bh.rgs = append(bh.rgs, r)
	return nil
}

func (fh *Header) writeTo(w io.Writer) (err error) {
	err = binary.Write(w, Endian, bamMagic)
	if err != nil {
		return
	}
	text := fh.Bytes()
	err = binary.Write(w, Endian, int32(len(text)))
	if err != nil {
		return
	}
	_, err = w.Write(text)
	if err != nil {
		return
	}
	err = binary.Write(w, Endian, int32(len(fh.refs)))
	if err != nil {
		return
	}
	_, err = writeRefRecords(w, fh.refs)
	if err != nil {
		return
	}
	return
}

func writeRefRecords(w io.Writer, refs []*Reference) (n int, err error) {
	if !validInt32(len(refs)) {
		return 0, errors.New("bam: value out of range")
	}
	var (
		nf   int
		name []byte
	)
	for _, r := range refs {
		name = append(name, []byte(r.name)...)
		name = append(name, 0)
		err = binary.Write(w, Endian, int32(len(name)))
		if err != nil {
			return n, err
		}
		n += 4
		nf, err = w.Write(name)
		name = name[:0]
		if err != nil {
			return n, err
		}
		n += nf
		err = binary.Write(w, Endian, r.lRef)
		if err != nil {
			return n, err
		}
		n += 4
	}
	return n, nil
}

type Reference struct {
	id      int32
	name    string
	lRef    int32
	md5     *[16]byte
	assemID string
	species string
	uri     *url.URL
}

func NewReference(name, assemID, species string, length int, md5 []byte, uri *url.URL) (*Reference, error) {
	if !validLen(length) {
		return nil, errors.New("bam: length out of range")
	}
	if name == "" {
		return nil, errors.New("bam: no name provided")
	}
	var h *[16]byte
	if md5 != nil {
		h = &[16]byte{}
		copy(h[:], md5)
	}
	return &Reference{
		id:      -1, // This is altered by a Header when added.
		name:    name,
		lRef:    int32(length),
		md5:     h,
		assemID: assemID,
		species: species,
		uri:     uri,
	}, nil
}

func (r *Reference) ID() int {
	if r == nil {
		return -1
	}
	return int(r.id)
}
func (r *Reference) Name() string {
	if r == nil {
		return "*"
	}
	return r.name
}
func (r *Reference) AssemblyID() string {
	if r == nil {
		return ""
	}
	return r.assemID
}
func (r *Reference) Species() string {
	if r == nil {
		return ""
	}
	return r.species
}
func (r *Reference) Md5() []byte {
	if r == nil || r.md5 == nil {
		return nil
	}
	return r.md5[:]
}
func (r *Reference) URI() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%s", r.uri)
}
func (r *Reference) Len() int {
	if r == nil {
		return -1
	}
	return int(r.lRef)
}

func (r *Reference) String() string {
	s := fmt.Sprintf("@SQ\tSN:%s\tLN:%d", r.name, r.lRef)
	if r.md5 != nil {
		s += fmt.Sprintln("\tM5:%x", *r.md5)
	}
	if r.assemID != "" {
		s += fmt.Sprintln("\tAS:%s", r.assemID)
	}
	if r.species != "" {
		s += fmt.Sprintln("\tSP:%s", r.species)
	}
	if r.uri != nil {
		s += fmt.Sprintln("\tUR:%s", r.uri)
	}
	return s
}

func (r *Reference) Copy() *Reference {
	if r == nil {
		return nil
	}
	cr := *r
	cr.id = -1
	if r.md5 != nil {
		cr.md5 = &[16]byte{}
		*cr.md5 = *r.md5
	}
	if r.uri != nil {
		cr.uri = &url.URL{}
		*cr.uri = *r.uri
		if r.uri.User != nil {
			cr.uri.User = &url.Userinfo{}
			*cr.uri.User = *r.uri.User
		}
	}
	return &cr
}

type ReadGroup struct {
	id           int32
	name         string
	center       string
	description  string
	date         time.Time
	flowOrder    []byte
	keySeq       []byte
	library      string
	program      string
	insertSize   int
	platform     string
	platformUnit string
	sample       string
}

func NewReadGroup(name, center, desc, lib, prog, plat, unit, sample string, date time.Time, size int, flow, key []byte) (*ReadGroup, error) {
	if !validInt32(size) {
		return nil, errors.New("bam: length overflow")
	}
	return &ReadGroup{
		id:           -1, // This is altered by a Header when added.
		name:         name,
		center:       center,
		description:  desc,
		date:         date,
		flowOrder:    flow,
		keySeq:       key,
		library:      lib,
		program:      prog,
		insertSize:   size,
		platform:     plat,
		platformUnit: unit,
		sample:       sample,
	}, nil
}

func (r *ReadGroup) ID() int {
	if r == nil {
		return -1
	}
	return int(r.id)
}
func (r *ReadGroup) Name() string {
	if r == nil {
		return "*"
	}
	return r.name
}

func (r *ReadGroup) Copy() *ReadGroup {
	if r == nil {
		return nil
	}
	cr := *r
	cr.id = -1
	cr.flowOrder = append([]byte(nil), r.flowOrder...)
	cr.keySeq = append([]byte(nil), r.keySeq...)
	return &cr
}

type Program struct {
	id       int32
	uid      string
	previous string
	name     string
	command  string
	version  string
}

func NewProgram(uid, name, command, prev, v string) *Program {
	return &Program{
		id:       -1,
		uid:      uid,
		previous: prev,
		name:     name,
		command:  command,
		version:  v,
	}
}

func (p *Program) ID() int {
	if p == nil {
		return -1
	}
	return int(p.id)
}
func (p *Program) UID() string {
	if p == nil {
		return ""
	}
	return p.uid
}
func (p *Program) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}
func (p *Program) Previous() string {
	if p == nil {
		return ""
	}
	return p.previous
}
func (p *Program) Version() string {
	if p == nil {
		return ""
	}
	return p.version
}
func (p *Program) Copy() *Program {
	if p == nil {
		return nil
	}
	cp := *p
	cp.id = -1
	return &cp
}
