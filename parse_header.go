// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

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
	"time"
)

var (
	badHeader = errors.New("bam: malformed header line")
	dupTag    = errors.New("bam: duplicate field")
)

type tag [2]byte

var (
	headerTag       = tag{'H', 'D'}
	versionTag      = tag{'V', 'N'}
	sortOrderTag    = tag{'S', 'O'}
	groupOrderTag   = tag{'G', 'O'}
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
)

var bamMagic = [4]byte{'B', 'A', 'M', 0x1}

func (bh *Header) read(r io.Reader) error {
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
	err = bh.parseHeader(text)
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
		err = bh.AddReference(r)
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
		case t == groupOrderTag:
			if bh.GroupOrder != GroupUnspecified {
				return badHeader
			}
			bh.GroupOrder = groupOrderMap[fs]
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
	bh.Comments = append(bh.Comments, string(fields[1]))
	return nil
}
