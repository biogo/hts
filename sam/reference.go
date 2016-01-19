// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
)

// Reference is a mapping reference.
type Reference struct {
	id        int32
	name      string
	lRef      int32
	md5       string
	assemID   string
	species   string
	uri       *url.URL
	otherTags []tagPair
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
	var h string
	if md5 != nil {
		if len(md5) != 16 {
			return nil, errors.New("sam: invalid md5 sum length")
		}
		h = string(md5[:])
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
func (r *Reference) MD5() []byte {
	if r == nil || r.md5 == "" {
		return nil
	}
	return []byte(r.md5)
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

// Get returns the string representation of the value associated with the
// given reference line tag. If the tag is not present the empty string is returned.
func (r *Reference) Get(t Tag) string {
	switch t {
	case refNameTag:
		return r.Name()
	case refLengthTag:
		return fmt.Sprint(r.lRef)
	case assemblyIDTag:
		return r.assemID
	case md5Tag:
		if r.md5 == "" {
			return ""
		}
		return fmt.Sprintf("%x", []byte(r.md5))
	case speciesTag:
		return r.species
	case uriTag:
		if r.uri == nil {
			return ""
		}
		return r.uri.String()
	}
	for _, tp := range r.otherTags {
		if t == tp.tag {
			return tp.value
		}
	}
	return ""
}

// Set sets the value associated with the given reference line tag to the specified
// value. If value is the empty string and the tag may be absent, it is deleted.
func (r *Reference) Set(t Tag, value string) error {
	switch t {
	case refNameTag:
		if value == "*" {
			r.name = ""
			return nil
		}
		r.name = value
	case refLengthTag:
		l, err := strconv.Atoi(value)
		if err != nil {
			return errBadHeader
		}
		if !validLen(l) {
			return errBadLen
		}
		r.lRef = int32(l)
	case assemblyIDTag:
		r.assemID = value
	case md5Tag:
		if value == "" {
			r.md5 = ""
			return nil
		}
		hb := [16]byte{}
		n, err := hex.Decode(hb[:], []byte(value))
		if err != nil {
			return err
		}
		if n != 16 {
			return errBadHeader
		}
		r.md5 = string(hb[:])
	case speciesTag:
		r.species = value
	case uriTag:
		if value == "" {
			r.uri = nil
			return nil
		}
		uri, err := url.Parse(value)
		if err != nil {
			return err
		}
		r.uri = uri
		if r.uri.Scheme != "http" && r.uri.Scheme != "ftp" {
			r.uri.Scheme = "file"
		}
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

// String returns a string representation of the Reference according to the
// SAM specification section 1.3,
func (r *Reference) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "@SQ\tSN:%s\tLN:%d", r.name, r.lRef)
	if r.md5 != "" {
		fmt.Fprintf(&buf, "\tM5:%x", []byte(r.md5))
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
	for _, tp := range r.otherTags {
		fmt.Fprintf(&buf, "\t%s:%s", tp.tag, tp.value)
	}
	return buf.String()
}

// Clone returns a deep copy of the Reference.
func (r *Reference) Clone() *Reference {
	if r == nil {
		return nil
	}
	cr := *r
	cr.otherTags = make([]tagPair, len(cr.otherTags))
	copy(cr.otherTags, r.otherTags)
	cr.id = -1
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

func equalRefs(a, b *Reference) bool {
	if a == b {
		return true
	}
	if a.id != b.id ||
		a.name != b.name ||
		a.lRef != b.lRef ||
		a.md5 != b.md5 ||
		a.assemID != b.assemID ||
		a.species != b.species ||
		a.uri != b.uri {
		return false
	}
	if a.uri != nil && b.uri != nil && a.uri.String() != b.uri.String() {
		return false
	}
	aOther := make(tagPairs, len(a.otherTags))
	copy(aOther, a.otherTags)
	sort.Sort(aOther)
	bOther := make(tagPairs, len(b.otherTags))
	copy(bOther, b.otherTags)
	sort.Sort(bOther)
	for i, ap := range aOther {
		bp := bOther[i]
		if ap.tag != bp.tag || ap.value != bp.value {
			return false
		}
	}
	return true
}

type tagPairs []tagPair

func (p tagPairs) Len() int { return len(p) }
func (p tagPairs) Less(i, j int) bool {
	return p[i].tag[0] < p[j].tag[0] || (p[i].tag[0] == p[j].tag[0] && p[i].tag[1] < p[j].tag[1])
}
func (p tagPairs) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
