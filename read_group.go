// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"errors"
	"fmt"
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
		fmt.Fprintf(&buf, "\tDT:%s", r.date.Format(iso8601TimeDate))
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
