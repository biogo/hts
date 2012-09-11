// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"
)

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

	if err := NewWriter(buf).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	r, err := NewReader(buf)
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

// TestRoundTrip tests that bgzipping and then bgunzipping is the identity
// function.
func TestRoundTrip(t *testing.T) {
	buf := new(bytes.Buffer)

	w := NewWriter(buf)
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

	r, err := NewReader(buf)
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
	if bl, err := r.CurrBlockSize(); err != nil || bl != 7 {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, 7)
	}
	if string(r.Extra) != "BC\x02\x00\x07\x00extra" {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00\x07\x00extra")
	}
	if r.ModTime.Unix() != 1e8 {
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
	buf := new(bytes.Buffer)

	w := NewWriter(buf)
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
	o := int64(buf.Len())
	if _, err := w.Write([]byte("payloadTwo")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	var (
		b     []byte
		bl, n int
		err   error
	)
	r, err := NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Comment != "comment" {
		t.Fatalf("comment is %q, want %q", r.Comment, "comment")
	}
	if string(r.Extra) != "BC\x02\x00\x08\x00extra" {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00\x08\x00extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Fatalf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Fatalf("name is %q, want %q", r.Name, "name")
	}
	bl, err = r.CurrBlockSize()
	if err != nil || bl != 8 {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, 8)
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
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

	cw := &countWriter{w: f}
	w := NewWriter(cw)
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
	offset := cw.bytes
	if _, err := w.Write([]byte("payloadTwo")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	var (
		b     []byte
		bl, n int
	)

	f, err = os.Open(fname)
	if err != nil {
		t.Fatalf("Reopen temp file: %v", err)
	}
	r, err := NewReader(f)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	if r.Comment != "comment" {
		t.Fatalf("comment is %q, want %q", r.Comment, "comment")
	}
	if string(r.Extra) != "BC\x02\x00\x08\x00extra" {
		t.Fatalf("extra is %q, want %q", r.Extra, "BC\x02\x00\x08\x00extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Fatalf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Fatalf("name is %q, want %q", r.Name, "name")
	}
	bl, err = r.CurrBlockSize()
	if err != nil || bl != 8 {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, 8)
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
	}
	if err := r.Seek(Offset{}, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	n, err = r.Read(b)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payload1" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payload1")
	}
	if err := r.Seek(Offset{File: offset}, 0); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	bl, err = r.CurrBlockSize()
	if err != nil || bl != len("payloadTwo") {
		t.Fatalf("CurrBlockSize() is %d, want %d", bl, len("payloadTwo"))
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(b[:n]) != "payloadTwo" {
		t.Fatalf("payload is %q, want %q", string(b[:n]), "payloadTwo")
	}
	os.Remove(fname)
}
