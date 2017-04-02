// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"errors"
	"fmt"
)

// Program represents a SAM program.
type Program struct {
	owner     *Header
	id        int32
	uid       string
	previous  string
	name      string
	command   string
	version   string
	otherTags []tagPair
}

// NewProgram returns a Program with the given unique ID, name, command,
// previous program ID in the pipeline and version.
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

// ID returns the header ID for the Program.
func (p *Program) ID() int {
	if p == nil {
		return -1
	}
	return int(p.id)
}

// UID returns the unique program ID for the program.
func (p *Program) UID() string {
	if p == nil {
		return ""
	}
	return p.uid
}

// SetUID sets the unique program ID to uid.
func (r *Program) SetUID(uid string) error {
	if r.owner != nil {
		id, exists := r.owner.seenProgs[uid]
		if exists {
			if id != r.id {
				return errors.New("sam: uid exists")
			}
			return nil
		}
		delete(r.owner.seenProgs, r.uid)
		r.owner.seenProgs[uid] = r.id
	}
	r.uid = uid
	return nil
}

// Name returns the program's name.
func (p *Program) Name() string {
	if p == nil {
		return ""
	}
	return p.name
}

// Command returns the program's command line.
func (p *Program) Command() string {
	if p == nil {
		return ""
	}
	return p.command
}

// Previous returns the unique ID for the previous program in the pipeline.
func (p *Program) Previous() string {
	if p == nil {
		return ""
	}
	return p.previous
}

// Version returns the version of the program.
func (p *Program) Version() string {
	if p == nil {
		return ""
	}
	return p.version
}

// Clone returns a deep copy of the Program.
func (p *Program) Clone() *Program {
	if p == nil {
		return nil
	}
	cp := *p
	if len(cp.otherTags) != 0 {
		cp.otherTags = make([]tagPair, len(cp.otherTags))
	}
	copy(cp.otherTags, p.otherTags)
	cp.id = -1
	cp.owner = nil
	return &cp
}

// Tags applies the function fn to each of the tag-value pairs of the Program.
// The function fn must not add or delete tags held by the receiver during
// iteration.
func (p *Program) Tags(fn func(t Tag, value string)) {
	if fn == nil {
		return
	}
	fn(idTag, p.UID())
	if p.name != "" {
		fn(programNameTag, p.name)
	}
	if p.command != "" {
		fn(commandLineTag, p.command)
	}
	if p.previous != "" {
		fn(previousProgTag, p.previous)
	}
	if p.version != "" {
		fn(versionTag, p.version)
	}
	for _, tp := range p.otherTags {
		fn(tp.tag, tp.value)
	}
}

// Get returns the string representation of the value associated with the
// given program line tag. If the tag is not present the empty string is returned.
func (p *Program) Get(t Tag) string {
	switch t {
	case idTag:
		return p.UID()
	case programNameTag:
		return p.Name()
	case commandLineTag:
		return p.Command()
	case previousProgTag:
		return p.Previous()
	case versionTag:
		return p.Version()
	}
	for _, tp := range p.otherTags {
		if t == tp.tag {
			return tp.value
		}
	}
	return ""
}

// Set sets the value associated with the given program line tag to the specified
// value. If value is the empty string and the tag may be absent, it is deleted.
func (p *Program) Set(t Tag, value string) error {
	switch t {
	case idTag:
		if value == "" {
			return errDupProgram
		}
	case programNameTag:
		p.name = value
	case commandLineTag:
		p.command = value
	case previousProgTag:
		p.previous = value
	case versionTag:
		p.version = value
	default:
		if value == "" {
			for i, tp := range p.otherTags {
				if t == tp.tag {
					copy(p.otherTags[i:], p.otherTags[i+1:])
					p.otherTags = p.otherTags[:len(p.otherTags)-1]
					return nil
				}
			}
		} else {
			for i, tp := range p.otherTags {
				if t == tp.tag {
					p.otherTags[i].value = value
					return nil
				}
			}
			p.otherTags = append(p.otherTags, tagPair{tag: t, value: value})
		}
	}
	return nil
}

// String returns a string representation of the program according to the
// SAM specification section 1.3,
func (p *Program) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "@PG\tID:%s", p.uid)
	if p.name != "" {
		fmt.Fprintf(&buf, "\tPN:%s", p.name)
	}
	if p.command != "" {
		fmt.Fprintf(&buf, "\tCL:%s", p.command)
	}
	if p.previous != "" {
		fmt.Fprintf(&buf, "\tPP:%s", p.previous)
	}
	if p.version != "" {
		fmt.Fprintf(&buf, "\tVN:%s", p.version)
	}
	for _, tp := range p.otherTags {
		fmt.Fprintf(&buf, "\t%s:%s", tp.tag, tp.value)
	}
	return buf.String()
}
