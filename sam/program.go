// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"fmt"
)

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
func (p *Program) Clone() *Program {
	if p == nil {
		return nil
	}
	cp := *p
	cp.id = -1
	return &cp
}

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
