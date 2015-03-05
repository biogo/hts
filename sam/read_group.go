// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

// ReadGroup represents a sequencing read group.
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

// NewReadGroup returns a ReadGroup with the given name, center, description,
// library, program, platform, unique platform unit, sample name, date of
// read group production, predicted median insert size, flow order and key
// sequence.
func NewReadGroup(name, center, desc, lib, prog, plat, unit, sample string, date time.Time, size int, flow, key []byte) (*ReadGroup, error) {
	if !validInt32(size) {
		return nil, errors.New("sam: length overflow")
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

// ID returns the header ID for the ReadGroup.
func (r *ReadGroup) ID() int {
	if r == nil {
		return -1
	}
	return int(r.id)
}

// Name returns the read group's name.
func (r *ReadGroup) Name() string {
	if r == nil {
		return "*"
	}
	return r.name
}

// Clone returns a deep copy of the ReadGroup.
func (r *ReadGroup) Clone() *ReadGroup {
	if r == nil {
		return nil
	}
	cr := *r
	cr.id = -1
	cr.flowOrder = append([]byte(nil), r.flowOrder...)
	cr.keySeq = append([]byte(nil), r.keySeq...)
	return &cr
}

// Library returns the library name for the read group.
func (r *ReadGroup) Library() string { return r.library }

// PlatformUnit returns the unique platform unit for the read group.
func (r *ReadGroup) PlatformUnit() string { return r.platformUnit }

// Time returns the time the read group was produced.
func (r *ReadGroup) Time() time.Time { return r.date }

// String returns a string representation of the read group according to the
// SAM specification section 1.3,
func (r *ReadGroup) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "@RG\tID:%s", r.name)
	if r.center != "" {
		fmt.Fprintf(&buf, "\tCN:%s", r.center)
	}
	if r.description != "" {
		fmt.Fprintf(&buf, "\tDS:%s", r.description)
	}
	if (r.date != time.Time{}) {
		fmt.Fprintf(&buf, "\tDT:%s", r.date.Format(iso8601TimeDateN))
	}
	if r.flowOrder != nil {
		fmt.Fprintf(&buf, "\tFO:%s", r.flowOrder)
	}
	if r.keySeq != nil {
		fmt.Fprintf(&buf, "\tKS:%s", r.keySeq)
	}
	if r.library != "" {
		fmt.Fprintf(&buf, "\tLB:%s", r.library)
	}
	if r.program != "" {
		fmt.Fprintf(&buf, "\tPG:%s", r.program)
	}
	if r.insertSize != 0 {
		fmt.Fprintf(&buf, "\tPI:%d", r.insertSize)
	}
	if r.platform != "" {
		fmt.Fprintf(&buf, "\tPL:%s", r.platform)
	}
	if r.platformUnit != "" {
		fmt.Fprintf(&buf, "\tPU:%s", r.platformUnit)
	}
	if r.sample != "" {
		fmt.Fprintf(&buf, "\tSM:%s", r.sample)
	}
	return buf.String()
}
