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
