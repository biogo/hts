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
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unsafe"
)

type Record struct {
	name    string
	ref     *Reference
	pos     int
	mapQ    byte
	bin     uint16
	cigar   []CigarOp
	flag    Flags
	mateRef *Reference
	matePos int
	tLen    int
	seq     nybbleSeq
	qual    []byte
	auxTags []Aux
}

func NewRecord(name string, ref, mRef *Reference, p, mPos, tLen int, mapQ byte, co []CigarOp, seq, qual []byte, aux []Aux) (*Record, error) {
	if !(validPos(p) && validPos(mPos) && validTmpltLen(tLen) && validLen(len(seq)) && validLen(len(qual))) {
		return nil, errors.New("bam: value out of range")
	}
	if len(qual) != len(seq) {

	}
	if ref != nil {
		if ref.id < 0 {
			return nil, errors.New("bam: linking to invalid reference")
		}
	}
	if mRef != nil {
		if mRef.id < 0 {
			return nil, errors.New("bam: linking to invalid mate reference")
		}
	}
	r := &Record{
		name:    name,
		ref:     ref,
		pos:     p,
		mapQ:    mapQ,
		cigar:   co,
		mateRef: mRef,
		matePos: mPos,
		tLen:    tLen,
		seq:     contract(seq),
		qual:    qual,
		auxTags: aux,
	}
	r.bin = reg2bin(r.pos, r.End())
	return r, nil
}

func (r *Record) Reference() *Reference {
	return r.ref
}

// Name returns the name of the alignment query.
func (r *Record) Name() string {
	return r.name
}

// Seq returns a byte slice containing the sequence of the alignment query.
func (r *Record) Seq() []byte {
	return r.seq.expand(len(r.qual))
}

// Quality returns an int8 slice containing the Phred quality scores of the alignment query.
func (r *Record) Quality() []byte {
	return r.qual
}

// SetSeq sets the sequence of the alignment query to the byte slice s.
func (r *Record) SetSeq(s []byte) {
	r.seq = contract(s)
}

// SetQuality sets the sequence of the alignment query to the int8 slice q.
func (r *Record) SetQuality(q []byte) {
	r.qual = q
}

// Cigar returns a slice of CigarOps describing the alignment.
func (r *Record) Cigar() []CigarOp {
	return r.cigar
}

// Tag returns an Aux tag whose tag ID matches the first two bytes of tag and true.
// If no tag matches, nil and false are returned.
func (r *Record) Tag(tag []byte) (v Aux, ok bool) {
	for i := range r.auxTags {
		if bytes.Compare(r.auxTags[i][:2], tag) == 0 {
			return r.auxTags[i], true
		}
	}
	return
}

// Tags returns all Aux tags for the aligment.
func (r *Record) Tags() []Aux {
	return r.auxTags
}

// Start returns the lower-coordinate end of the alignment.
func (r *Record) Start() int {
	return r.pos
}

// Len returns the length of the alignment template.
func (r *Record) Len() int {
	return r.tLen
}

// End returns the higher-coordinate end of the alignment.
// This is the start plus the sum of CigarMatch lengths.
func (r *Record) End() int {
	end := r.pos
	for i, co := range r.cigar {
		if t := co.Type(); t == CigarBack {
			if i == len(r.cigar)-1 {
				break
			}
			var j, forw, delta int
			back := co.Len()
			for j = i - 1; j >= 0; j-- {
				x := r.cigar[j]
				tx, lx := x.Type(), x.Len()
				if consume[tx].query {
					if forw+lx >= back {
						if consume[tx].ref {
							delta += back - forw
						}
						break
					} else {
						forw += lx
					}
				}
				if consume[t].ref {
					delta += lx
				}
			}
			if j < 0 {
				end = r.pos
			} else {
				end -= delta
			}
		} else if consume[t].ref {
			end += co.Len()
		}
	}
	return end
}

// Score returns the quality of the alignment.
func (r *Record) Score() byte {
	return r.mapQ
}

// Flags returns the SAM flags for the alignment record.
func (r *Record) Flags() Flags {
	return r.flag
}

// SetFlags sets the SAM flags for the alignment record.
func (r *Record) SetFlags(fl Flags) {
	r.flag = fl
}

// Strand returns an int8 indicating the strand of the alignment. A positive return indicates
// alignment in the forward orientation, a negative returns indicates alignemnt in the reverse
// orientation.
func (r *Record) Strand() int8 {
	if r.flag&Reverse == Reverse {
		return -1
	}
	return 1
}

// NextRefID returns the reference ID of the next segment/mate.
func (r *Record) MateReference() *Reference {
	return r.mateRef
}

// NextStart returns the start position of the next segment/mate.
func (r *Record) MateStart() int {
	return r.matePos
}

// String returns a string representation of the Record.
func (r *Record) String() string {
	end := r.End()
	return fmt.Sprintf("%s %v %v %d %s:%d..%d %d %s:%d %d %s %v %v",
		r.name,
		r.flag,
		r.cigar,
		r.mapQ,
		r.ref.Name(),
		r.pos,
		end,
		end-r.pos,
		r.mateRef.Name(),
		r.matePos,
		r.tLen,
		r.seq.expand(len(r.qual)),
		r.qual,
		r.auxTags,
	)
}

// BAM record types
type bamRecordFixed struct {
	BlockSize int32
	RefID     int32
	Pos       int32
	NLen      uint8
	MapQ      uint8
	Bin       uint16
	NCigar    uint16
	Flag      Flags
	LSeq      int32
	NextRefID int32
	NextPos   int32
	TLen      int32
}

type bamRecord struct {
	bamRecordFixed
	readName []byte
	cigar    []CigarOp
	seq      []nybblePair
	qual     []byte
	auxTags  []byte
}

var (
	lenFieldSize      = binary.Size(bamRecordFixed{}.BlockSize)
	bamFixedRemainder = binary.Size(bamRecordFixed{}) - lenFieldSize
)

func (br *bamRecord) unmarshal(h *Header) *Record {
	fixed := br.bamRecordFixed
	return &Record{
		name:    string(br.readName[:len(br.readName)-1]), // The BAM spec indicates name is null terminated.
		ref:     h.Refs()[fixed.RefID],
		pos:     int(fixed.Pos),
		mapQ:    fixed.MapQ,
		cigar:   br.cigar,
		flag:    fixed.Flag,
		seq:     br.seq,
		qual:    br.qual,
		tLen:    int(fixed.TLen),
		mateRef: h.Refs()[fixed.NextRefID],
		matePos: int(fixed.NextPos),
		auxTags: parseAux(br.auxTags),
	}

}

func (br *bamRecord) readFrom(r io.Reader) error {
	h := &br.bamRecordFixed
	err := binary.Read(r, Endian, h)
	n := int(br.BlockSize) - bamFixedRemainder

	br.readName = make([]byte, h.NLen)
	nf, err := r.Read(br.readName)
	if err != nil {
		return err
	}
	if nf != int(h.NLen) {
		return errors.New("bam: truncated record name")
	}
	n -= nf

	br.cigar, nf, err = readCigarOps(r, h.NCigar)
	if err != nil {
		return err
	}
	n -= nf

	seq := make([]byte, h.LSeq>>1)
	nf, err = r.Read(seq)
	if err != nil {
		return err
	}
	if nf != int((h.LSeq+1)>>1) {
		return errors.New("bam: truncated sequence")
	}
	br.seq = *(*[]nybblePair)(unsafe.Pointer(&seq))
	n -= nf

	br.qual = make([]byte, h.LSeq)
	nf, err = r.Read(br.qual)
	if err != nil {
		return err
	}
	if nf != int(h.LSeq) {
		return errors.New("bam: truncated quality")
	}
	n -= nf

	br.auxTags = make([]byte, n)
	nf, err = r.Read(br.auxTags)
	if err != nil {
		return err
	}
	if n != nf {
		return errors.New("bam: truncated auxilliary data")
	}

	return nil
}

func readCigarOps(r io.Reader, n uint16) (co []CigarOp, nf int, err error) {
	co = make([]CigarOp, n)
	size := binary.Size(CigarOp(0))
	for i := range co {
		err = binary.Read(r, Endian, &co[i])
		if err != nil {
			return nil, nf, err
		}
		nf += size
	}
	return
}

func (r *Record) marshal(br *bamRecord) int {
	tags := buildAux(r.auxTags)
	recLen := bamFixedRemainder +
		len(r.name) + 1 + // Null terminated.
		len(r.cigar)<<2 + // CigarOps are 4 bytes.
		len(r.seq) +
		len(r.qual) +
		len(tags)
	*br = bamRecord{
		bamRecordFixed: bamRecordFixed{
			BlockSize: int32(recLen),
			RefID:     r.ref.id,
			Pos:       int32(r.pos),
			NLen:      byte(len(r.name) + 1),
			MapQ:      r.mapQ,
			Bin:       r.bin,
			NCigar:    uint16(len(r.cigar)),
			Flag:      r.flag,
			LSeq:      int32(len(r.qual)),
			NextRefID: int32(r.mateRef.id),
			NextPos:   int32(r.matePos),
			TLen:      int32(r.tLen),
		},
		readName: append([]byte(r.name), 0),
		cigar:    r.cigar,
		seq:      r.seq,
		qual:     r.qual,
		auxTags:  tags,
	}
	return recLen
}

func (br *bamRecord) writeTo(w io.Writer) error {
	err := binary.Write(w, Endian, br.bamRecordFixed)

	_, err = w.Write(br.readName)
	if err != nil {
		return err
	}

	err = writeCigarOps(w, br.cigar)
	if err != nil {
		return err
	}

	_, err = w.Write(*(*[]byte)(unsafe.Pointer(&br.seq)))
	if err != nil {
		return err
	}

	_, err = w.Write(br.qual)
	if err != nil {
		return err
	}

	_, err = w.Write(br.auxTags)
	if err != nil {
		return err
	}

	return nil
}

func writeCigarOps(w io.Writer, co []CigarOp) (err error) {
	for _, o := range co {
		err = binary.Write(w, Endian, o)
		if err != nil {
			return err
		}
	}
	return
}

type nybblePair byte

type nybbleSeq []nybblePair

var (
	n16TableRev = [16]byte{'=', 'A', 'C', 'M', 'G', 'R', 'S', 'V', 'T', 'W', 'Y', 'H', 'K', 'D', 'B', 'N'}
	n16Table    = [256]nybblePair{
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0x1, 0x2, 0x4, 0x8, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0x0, 0xf, 0xf,
		0xf, 0x1, 0xe, 0x2, 0xd, 0xf, 0xf, 0x4, 0xb, 0xf, 0xf, 0xc, 0xf, 0x3, 0xf, 0xf,
		0xf, 0xf, 0x5, 0x6, 0x8, 0xf, 0x7, 0x9, 0xf, 0xa, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0x1, 0xe, 0x2, 0xd, 0xf, 0xf, 0x4, 0xb, 0xf, 0xf, 0xc, 0xf, 0x3, 0xf, 0xf,
		0xf, 0xf, 0x5, 0x6, 0x8, 0xf, 0x7, 0x9, 0xf, 0xa, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
		0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf, 0xf,
	}
)

func (ns nybbleSeq) expand(l int) []byte {
	s := make([]byte, l)
	for i := range s {
		if i&1 == 0 {
			s[i] = n16TableRev[ns[i>>1]>>4]
		} else {
			s[i] = n16TableRev[ns[i>>1]&0xf]
		}
	}

	return s
}

func contract(s []byte) nybbleSeq {
	ns := make(nybbleSeq, (len(s)+1)>>1)
	var np nybblePair
	for i, b := range s {
		if i&1 == 0 {
			np = n16Table[b] << 4
		} else {
			ns[i>>1] = np | n16Table[b]
		}
	}
	return ns
}
