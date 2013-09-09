// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the Go LICENSE file.

// Changes copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bytes"
	"flag"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

var conc = flag.Int("conc", 1, "sets the level of concurrency for compression")

type countWriter struct {
	bytes int64
	w     io.Writer
}

func (cw *countWriter) Write(p []byte) (n int, err error) {
	n, err = cw.w.Write(p)
	cw.bytes += int64(n)
	return
}

// TestEmpty tests that an empty payload still forms a valid GZIP stream.
func TestEmpty(t *testing.T) {
	buf := new(bytes.Buffer)

	if err := NewWriter(buf, *conc).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	r, err := NewReader(buf, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if len(b) != 0 {
		t.Fatalf("got %d bytes, want 0", len(b))
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
}

// TestEOF tests CheckEOF can find the EOF magic block.
func TestEOF(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "bgzf_EOF_test_")
	if err != nil {
		t.Fatalf("Create temp file: %v", err)
	}
	fname := f.Name()

	if err := NewWriter(f, *conc).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	f, err = os.Open(fname)
	if err != nil {
		t.Fatalf("Open temp file: %v", err)
	}
	ok, err := CheckEOF(f)
	if err != nil {
		t.Fatalf("CheckEOF: %v", err)
	}
	if !ok {
		t.Fatal("Expected EOF: not found.")
	}

	os.Remove(fname)
}

// TestRoundTrip tests that bgzipping and then bgunzipping is the identity
// function.
func TestRoundTrip(t *testing.T) {
	buf := new(bytes.Buffer)

	w := NewWriter(buf, *conc)
	w.Comment = "comment"
	w.Extra = []byte("extra")
	w.ModTime = time.Unix(1e8, 0)
	w.Name = "name"
	if _, err := w.Write([]byte("payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	wbl := buf.Len()
	r, err := NewReader(buf, false)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("payload is %q, want %q", string(b), "payload")
	}
	if r.Comment != "comment" {
		t.Fatalf("comment is %q, want %q", r.Comment, "comment")
	}
	if bl, err := r.CurrBlockSize(); err != nil || bl != len(magicBlock) {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, wbl)
	}
	blEnc := string([]byte{byte(wbl - 1), byte((wbl - 1) >> 8)})
	if string(r.Extra) != magicBlock[12:18] {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
	}
	if r.ModTime.Unix() != 0 {
		t.Fatalf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Fatalf("name is %q, want %q", r.Name, "name")
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Reader.Close: %v", err)
	}
}

// TestRoundTripMulti tests that bgzipping and then bgunzipping is the identity
// function for a multiple member bgzf.
func TestRoundTripMulti(t *testing.T) {
	var wbl [2]int
	buf := new(bytes.Buffer)

	w := NewWriter(buf, *conc)
	w.Comment = "comment"
	w.Extra = []byte("extra")
	w.ModTime = time.Unix(1e8, 0)
	w.Name = "name"
	if _, err := w.Write([]byte("payload1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	wbl[0] = buf.Len()
	o := int64(buf.Len())
	if _, err := w.Write([]byte("payloadTwo")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	wbl[1] = buf.Len() - wbl[0] - len(magicBlock)

	var (
		b     []byte
		bl, n int
		err   error
	)
	r, err := NewReader(buf, true)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Comment != "comment" {
		t.Fatalf("comment is %q, want %q", r.Comment, "comment")
	}
	blEnc := string([]byte{byte(wbl[0] - 1), byte((wbl[0] - 1) >> 8)})
	if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Fatalf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Fatalf("name is %q, want %q", r.Name, "name")
	}

	bl, err = r.CurrBlockSize()
	if err != nil || bl != wbl[0] {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, wbl[0])
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil && err != NewBlock {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
	}

	bl, err = r.CurrBlockSize()
	if err != nil || bl != wbl[1] {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, wbl[1])
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil && err != NewBlock {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payloadTwo" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payloadTwo")
	}

	if r.Seek(Offset{o, 0}, 1) == nil {
		t.Fatal("expected seek failure")
	}
	n, err = r.Read(b)
	if err == nil {
		t.Fatal("expected read failure")
	}
}

// TestRoundTripMultiSeek tests that bgzipping and then bgunzipping is the identity
// function for a multiple member bgzf with an underlying Seeker.
func TestRoundTripMultiSeek(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "bgzf_test_")
	if err != nil {
		t.Fatalf("Create temp file: %v", err)
	}
	fname := f.Name()

	var wbl [2]int
	cw := &countWriter{w: f}
	w := NewWriter(cw, *conc)
	w.Comment = "comment"
	w.Extra = []byte("extra")
	w.ModTime = time.Unix(1e8, 0)
	w.Name = "name"
	if _, err := w.Write([]byte("payload1")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := w.Wait(); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	offset := cw.bytes
	wbl[0] = int(offset)
	if _, err := w.Write([]byte("payloadTwo")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	wbl[1] = int(cw.bytes-offset) - len(magicBlock)

	var (
		b     []byte
		bl, n int
	)

	f, err = os.Open(fname)
	if err != nil {
		t.Fatalf("Reopen temp file: %v", err)
	}
	r, err := NewReader(f, true)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Comment != "comment" {
		t.Fatalf("comment is %q, want %q", r.Comment, "comment")
	}
	blEnc := string([]byte{byte(wbl[0] - 1), byte((wbl[0] - 1) >> 8)})
	if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00\x08\x00extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Fatalf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Fatalf("name is %q, want %q", r.Name, "name")
	}
	bl, err = r.CurrBlockSize()
	if err != nil || bl != wbl[0] {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, wbl[0])
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil && err != NewBlock {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
	}
	if err := r.Seek(Offset{}, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	n, err = r.Read(b)
	if err != nil && err != NewBlock {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
	}
	if err := r.Seek(Offset{File: offset}, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	bl, err = r.CurrBlockSize()
	if err != nil || bl != wbl[1] {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, wbl[1])
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil && err != NewBlock {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payloadTwo" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payloadTwo")
	}
	os.Remove(fname)
}

func BenchmarkWrite(b *testing.B) {
	bg := NewWriter(ioutil.Discard, *conc)
	block := bytes.Repeat([]byte("repeated"), 50)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			bg.Write(block)
		}
	}
}
