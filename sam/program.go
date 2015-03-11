// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"fmt"
)

// Program represents a SAM program.
type Program struct {
	id       int32
	uid      string
	previous string
	name     string
	command  string
	version  string
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

// Name returns the program's name.
func (p *Program) Name() string {
	if p == nil {
		return ""
	}
	return p.name
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
	cp.id = -1
	return &cp
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
	return buf.String()
}
