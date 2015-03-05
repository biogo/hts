// Copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
)

// Reference is a mapping reference.
type Reference struct {
	id      int32
	name    string
	lRef    int32
	md5     *[16]byte
	assemID string
	species string
	uri     *url.URL
}

// NewReference returns a new Reference based on the given parameters.
// Only name and length are mandatory and length must be a valid reference
// length according to the SAM specification, [1, 1<<31).
func NewReference(name, assemID, species string, length int, md5 []byte, uri *url.URL) (*Reference, error) {
	if !validLen(length) {
		return nil, errors.New("sam: length out of range")
	}
	if name == "" {
		return nil, errors.New("sam: no name provided")
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

// ID returns the header ID of the Reference.
func (r *Reference) ID() int {
	if r == nil {
		return -1
	}
	return int(r.id)
}

// Name returns the reference name.
func (r *Reference) Name() string {
	if r == nil {
		return "*"
	}
	return r.name
}

// AssemblyID returns the assembly ID of the reference.
func (r *Reference) AssemblyID() string {
	if r == nil {
		return ""
	}
	return r.assemID
}

// Species returns the reference species.
func (r *Reference) Species() string {
	if r == nil {
		return ""
	}
	return r.species
}

// MD5 returns a 16 byte slice holding the MD5 sum of the reference sequence.
// The returned slice should not be altered.
func (r *Reference) MD5() []byte {
	if r == nil || r.md5 == nil {
		return nil
	}
	return r.md5[:]
}

// URI returns the URI of the reference.
func (r *Reference) URI() string {
	if r == nil {
		return ""
	}
	return fmt.Sprintf("%s", r.uri)
}

// Len returns the length of the reference sequence.
func (r *Reference) Len() int {
	if r == nil {
		return -1
	}
	return int(r.lRef)
}

// SetLen sets the length of the reference sequence to l. The given length
// must be a valid SAM reference length.
func (r *Reference) SetLen(l int) error {
	if !validLen(l) {
		return errors.New("sam: length out of range")
	}
	r.lRef = int32(l)
	return nil
}

// String returns a string representation of the Reference according to the
// SAM specification section 1.3,
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

// Clone returns a deep copy of the Reference.
func (r *Reference) Clone() *Reference {
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
