// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// ReadGroup represents a sequencing read group.
type ReadGroup struct {
	id           int32
	name         string
	center       string
	description  string
	date         time.Time
	flowOrder    string
	keySeq       string
	library      string
	program      string
	insertSize   int
	platform     string
	platformUnit string
	sample       string
	otherTags    []tagPair
}

// NewReadGroup returns a ReadGroup with the given name, center, description,
// library, program, platform, unique platform unit, sample name, flow order,
// key, date of read group production, and predicted median insert size sequence.
func NewReadGroup(name, center, desc, lib, prog, plat, unit, sample, flow, key string, date time.Time, size int) (*ReadGroup, error) {
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
	cr.otherTags = make([]tagPair, len(cr.otherTags))
	copy(cr.otherTags, r.otherTags)
	cr.id = -1
	return &cr
}

// Library returns the library name for the read group.
func (r *ReadGroup) Library() string { return r.library }

// PlatformUnit returns the unique platform unit for the read group.
func (r *ReadGroup) PlatformUnit() string { return r.platformUnit }

// Time returns the time the read group was produced.
func (r *ReadGroup) Time() time.Time { return r.date }

// Get returns the string representation of the value associated with the
// given read group line tag. If the tag is not present the empty string is returned.
func (r *ReadGroup) Get(t Tag) string {
	switch t {
	case idTag:
		return r.Name()
	case centerTag:
		return r.center
	case descriptionTag:
		return r.description
	case dateTag:
		return r.date.Format(iso8601TimeDateN)
	case flowOrderTag:
		if r.flowOrder == "" {
			return "*"
		}
		return r.flowOrder
	case keySequenceTag:
		return r.keySeq
	case libraryTag:
		return r.library
	case programTag:
		return r.program
	case insertSizeTag:
		return fmt.Sprint(r.insertSize)
	case platformTag:
		return r.platform
	case platformUnitTag:
		return r.platformUnit
	case sampleTag:
		return r.sample
	}
	for _, tp := range r.otherTags {
		if t == tp.tag {
			return tp.value
		}
	}
	return ""
}

// Set sets the value associated with the given read group line tag to the specified
// value. If value is the empty string and the tag may be absent, it is deleted.
func (r *ReadGroup) Set(t Tag, value string) error {
	switch t {
	case idTag:
		r.name = value
	case centerTag:
		r.center = value
	case descriptionTag:
		r.description = value
	case dateTag:
		if value == "" {
			r.date = time.Time{}
			return nil
		}
		var err error
		for _, tf := range iso8601 {
			var date time.Time
			date, err = time.ParseInLocation(tf, value, nil)
			if err == nil {
				r.date = date
				break
			}
		}
		return err
	case flowOrderTag:
		if value == "" || value == "*" {
			r.flowOrder = ""
			return nil
		}
		r.flowOrder = value
	case keySequenceTag:
		r.keySeq = value
	case libraryTag:
		r.library = value
	case programTag:
		r.program = value
	case insertSizeTag:
		if value == "" {
			r.insertSize = 0
			return nil
		}
		i, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		if !validInt32(i) {
			return errBadLen
		}
		r.insertSize = i
	case platformTag:
		r.platform = value
	case platformUnitTag:
		r.platformUnit = value
	case sampleTag:
		r.sample = value
	default:
		if value == "" {
			for i, tp := range r.otherTags {
				if t == tp.tag {
					copy(r.otherTags[i:], r.otherTags[i+1:])
					r.otherTags = r.otherTags[:len(r.otherTags)-1]
					return nil
				}
			}
		} else {
			for i, tp := range r.otherTags {
				if t == tp.tag {
					r.otherTags[i].value = value
					return nil
				}
			}
			r.otherTags = append(r.otherTags, tagPair{tag: t, value: value})
		}
	}
	return nil
}

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
	if r.flowOrder != "" {
		fmt.Fprintf(&buf, "\tFO:%s", r.flowOrder)
	}
	if r.keySeq != "" {
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
	for _, tp := range r.otherTags {
		fmt.Fprintf(&buf, "\t%s:%s", tp.tag, tp.value)
	}
	return buf.String()
}
