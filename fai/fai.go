// Copyright ©2013 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package fai implements FAI fasta sequence file index handling.
package fai

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
)

const (
	nameField = iota
	lengthField
	startField
	basesField
	bytesField
)

var ErrNonUnique = errors.New("non-unique record name")

// Index is an FAI index.
type Index map[string]Record

// NewIndex returns a new Index constructed from the FASTA sequence
// in the provided io.Reader.
func NewIndex(fasta io.Reader) (Index, error) {
	sc := bufio.NewScanner(fasta)
	sc.Split(func(data []byte, atEOF bool) (advance int, token []byte, err error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			return i + 1, data[:i+1], nil
		}
		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	idx := make(Index)
	var (
		rec          Record
		offset       int64
		wantDescLine bool
	)
	for sc.Scan() {
		b := bytes.TrimSpace(sc.Bytes())
		if len(b) == 0 {
			continue
		}
		if b[0] == '>' {
			if rec.Name != "" {
				idx[rec.Name] = rec
				rec = Record{}
			}
			rec.Name = string(bytes.SplitN(b[1:], []byte{' '}, 2)[0])
			if _, exists := idx[rec.Name]; exists {
				return nil, fmt.Errorf("fai: duplicate sequence identifier %s at %d", rec.Name, offset)
			}
			rec.Start = offset + int64(len(sc.Bytes()))
			wantDescLine = false
		} else {
			if wantDescLine {
				return nil, fmt.Errorf("fai: unexpected short line before offset %d", offset)
			}
			switch {
			case rec.BytesPerLine == 0:
				rec.BytesPerLine = len(sc.Bytes())
			case len(sc.Bytes()) > rec.BytesPerLine:
				return nil, fmt.Errorf("fai: unexpected long line at offset %d", offset)
			case len(sc.Bytes()) < rec.BytesPerLine:
				wantDescLine = true
			}
			switch {
			case len(b) == 0:
				// Do nothing.
			case rec.BasesPerLine == 0:
				rec.BasesPerLine = len(b)
			case len(b) > rec.BasesPerLine:
				return nil, fmt.Errorf("fai: unexpected long line at offset %d", offset)
			case len(b) < rec.BasesPerLine:
				wantDescLine = true
			}
			rec.Length += len(b)
		}
		offset += int64(len(sc.Bytes()))
	}
	if rec.Name != "" {
		idx[rec.Name] = rec
		rec = Record{}
	}
	return idx, sc.Err()
}

// Record is a single FAI index record.
type Record struct {
	// Name is the name of the sequence.
	Name string
	// Length is the length of the sequence.
	Length int
	// Start is the starting seek offset of
	// the sequence.
	Start int64
	// BasesPerLine is the number of sequences
	// bases per line.
	BasesPerLine int
	// BytesPerLine is the number of bytes
	// used to represent each line.
	BytesPerLine int
}

// Position returns the seek offset of the sequence position p for the
// given Record.
func (r Record) Position(p int) int64 {
	if p < 0 || r.Length <= p {
		panic("fai: index out of range")
	}
	return r.position(p)
}

func (r Record) position(p int) int64 {
	return r.Start + int64(p/r.BasesPerLine*r.BytesPerLine+p%r.BasesPerLine)
}

// endOfLineOffset returns the number of bytes until the end of the line
// holding position p.
func (r Record) endOfLineOffset(p int) int {
	if p/r.BasesPerLine == r.Length/r.BasesPerLine {
		return r.Length - p
	}
	return r.BasesPerLine - p%r.BasesPerLine
}

func mustAtoi(fields []string, index, line int) int {
	i, err := strconv.ParseInt(fields[index], 10, 0)
	if err != nil {
		panic(parseError(line, index, err))
	}
	return int(i)
}

func mustAtoi64(fields []string, index, line int) int64 {
	i, err := strconv.ParseInt(fields[index], 10, 64)
	if err != nil {
		panic(parseError(line, index, err))
	}
	return i
}

// ReadFrom returns an Index from the stream provided by an io.Reader or an error. If the input
// contains non-unique records the error is a csv.ParseError identifying the second non-unique
// record.
func ReadFrom(r io.Reader) (idx Index, err error) {
	tr := csv.NewReader(r)
	tr.Comma = '\t'
	tr.FieldsPerRecord = 5
	defer func() {
		r := recover()
		if r != nil {
			e, ok := r.(error)
			if !ok {
				panic(r)
			}
			if _, ok = r.(*csv.ParseError); !ok {
				panic(r)
			}
			err = e
			idx = nil
		}
	}()
	for line := 1; ; line++ {
		rec, err := tr.Read()
		if err == io.EOF {
			return idx, nil
		}
		if err != nil {
			return nil, err
		}
		if idx == nil {
			idx = make(Index)
		} else if _, exists := idx[rec[nameField]]; exists {
			return nil, parseError(line, 0, ErrNonUnique)
		}
		idx[rec[nameField]] = Record{
			Name:         rec[nameField],
			Length:       mustAtoi(rec, lengthField, line),
			Start:        mustAtoi64(rec, startField, line),
			BasesPerLine: mustAtoi(rec, basesField, line),
			BytesPerLine: mustAtoi(rec, bytesField, line),
		}
	}
}

func parseError(line, column int, err error) *csv.ParseError {
	return &csv.ParseError{
		StartLine: line,
		Line:      line,
		Column:    column,
		Err:       err,
	}
}

// WriteTo writes the the given index to w in order of ascending start position.
func WriteTo(w io.Writer, idx Index) error {
	recs := make([]Record, 0, len(idx))
	for _, r := range idx {
		recs = append(recs, r)
	}
	sort.Sort(byStart(recs))
	for _, r := range recs {
		_, err := fmt.Fprintf(w, "%s\t%d\t%d\t%d\t%d\n", r.Name, r.Length, r.Start, r.BasesPerLine, r.BytesPerLine)
		if err != nil {
			return err
		}
	}
	return nil
}

type byStart []Record

func (r byStart) Len() int           { return len(r) }
func (r byStart) Less(i, j int) bool { return r[i].Start < r[j].Start }
func (r byStart) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }
