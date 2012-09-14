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
	s := fmt.Sprintf("@SQ\tSN:%s\tLN:%d", r.name, r.lRef)
	if r.md5 != nil {
		s += fmt.Sprintln("\tM5:%x", *r.md5)
	}
	if r.assemID != "" {
		s += fmt.Sprintln("\tAS:%s", r.assemID)
	}
	if r.species != "" {
		s += fmt.Sprintln("\tSP:%s", r.species)
	}
	if r.uri != nil {
		s += fmt.Sprintln("\tUR:%s", r.uri)
	}
	return s
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
