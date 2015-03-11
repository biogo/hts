// Copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sam

// A Flags represents a BAM record's alignment FLAG field.
type Flags uint16

const (
	Paired        Flags = 1 << iota // The read is paired in sequencing, no matter whether it is mapped in a pair.
	ProperPair                      // The read is mapped in a proper pair.
	Unmapped                        // The read itself is unmapped; conflictive with ProperPair.
	MateUnmapped                    // The mate is unmapped.
	Reverse                         // The read is mapped to the reverse strand.
	MateReverse                     // The mate is mapped to the reverse strand.
	Read1                           // This is read1.
	Read2                           // This is read2.
	Secondary                       // Not primary alignment.
	QCFail                          // QC failure.
	Duplicate                       // Optical or PCR duplicate.
	Supplementary                   // Supplementary alignment, indicates alignment is part of a chimeric alignment.
)

// String representation of BAM alignment flags:
//  0x001 - p - Paired
//  0x002 - P - ProperPair
//  0x004 - u - Unmapped
//  0x008 - U - MateUnmapped
//  0x010 - r - Reverse
//  0x020 - R - MateReverse
//  0x040 - 1 - Read1
//  0x080 - 2 - Read2
//  0x100 - s - Secondary
//  0x200 - f - QCFail
//  0x400 - d - Duplicate
//  0x800 - S - Supplementary
//
// Note that flag bits are represented high order to the right.
func (f Flags) String() string {
	// If 0x01 is unset, no assumptions can be made about 0x02, 0x08, 0x20, 0x40 and 0x80
	const pairedMask = ProperPair | MateUnmapped | MateReverse | MateReverse | Read1 | Read2
	if f&1 == 0 {
		f &^= pairedMask
	}

	const flags = "pPuUrR12sfdS"

	b := make([]byte, len(flags))
	for i, c := range flags {
		if f&(1<<uint(i)) != 0 {
			b[i] = byte(c)
		} else {
			b[i] = '-'
		}
	}

	return string(b)
}
