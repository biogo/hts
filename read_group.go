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
	"errors"
	"time"
)

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
