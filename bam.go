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
	"encoding/binary"
)

var endian = binary.LittleEndian

type CigarOp uint32

type Flags uint32

type bamRecordFixed struct {
	blockSize int32
	refID     int32
	pos       int32
	nLen      uint8
	mapQ      uint8
	bin       uint16
	nCigar    uint16
	flag      Flags
	lSeq      int32
	nextRefID int32
	nextPos   int32
	tLen      int32
}

type bamRecord struct {
	bamRecordFixed
	readName []byte
	cigar    []CigarOp
	seq      []byte
	qual     []byte
	aux      []byte
}
