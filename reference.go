// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bam

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
)

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
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "@SQ\tSN:%s\tLN:%d", r.name, r.lRef)
	if r.md5 != nil {
		fmt.Fprintf(&buf, "\tM5:%x", *r.md5)
	}
	if r.assemID != "" {
		fmt.Fprintf(&buf, "\tAS:%s", r.assemID)
	}
	if r.species != "" {
		fmt.Fprintf(&buf, "\tSP:%s", r.species)
	}
	if r.uri != nil {
		fmt.Fprintf(&buf, "\tUR:%s", r.uri)
	}
	return buf.String()
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
