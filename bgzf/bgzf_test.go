// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the Go LICENSE file.

// Changes copyright ©2012 The bíogo.bam Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf_test

import (
	. "code.google.com/p/biogo.bam/bgzf"
	"code.google.com/p/biogo.bam/bgzf/cache"
	"errors"

	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
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

	r, err := NewReader(buf, *conc)
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

type crippledReaderAt struct {
	r *bytes.Reader
}

func (r crippledReaderAt) ReadAt(b []byte, off int64) (int, error) {
	return r.r.ReadAt(b, off)
}

// TestEOF tests HasEOF can find the EOF magic block.
func TestEOF(t *testing.T) {
	// os.File cases
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
	ok, err := HasEOF(f)
	if err != nil {
		t.Errorf("HasEOF: %v", err)
	}
	if !ok {
		t.Error("Expected EOF in os.File: not found.")
	}

	os.Remove(fname)

	f, err = os.Open(os.TempDir())
	if err != nil {
		t.Fatalf("Open temp dir: %v", err)
	}
	ok, err = HasEOF(f)
	if want := "read " + os.TempDir() + ": is a directory"; err.Error() != want {
		t.Errorf("Expected error:%s got:%v", want, err)
	}
	if ok {
		t.Error("Unexpected EOF in os.File IsDir: found.")
	}

	// {bytes,strings}.Reader cases
	var buf bytes.Buffer
	if err := NewWriter(&buf, *conc).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	ok, err = HasEOF(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Errorf("HasEOF: %v", err)
	}
	if !ok {
		t.Error("Expected EOF in []byte: not found.")
	}

	ok, err = HasEOF(strings.NewReader(buf.String()))
	if err != nil {
		t.Errorf("HasEOF: %v", err)
	}
	if !ok {
		t.Error("Expected EOF in string: not found.")
	}

	ok, err = HasEOF(crippledReaderAt{bytes.NewReader(buf.Bytes())})
	if err != ErrNoEnd {
		t.Errorf("Expected error:%s got:%v", ErrNoEnd, err)
	}
	if ok {
		t.Error("Unexpected EOF in crippled ReaderAt: found.")
	}
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
	// FIXME(kortschak) The magic block is written on close,
	// so we need to discount that until we have the capacity
	// to see every header again.
	wbl := buf.Len() - len(MagicBlock)

	r, err := NewReader(buf, *conc)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Insert a HasEOF to ensure it does not corrupt subsequent reads.
	HasEOF(bytes.NewReader(buf.Bytes()))

	if bl := ExpectedMemberSize(r.Header); bl != wbl {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl)
	}
	blEnc := string([]byte{byte(wbl - 1), byte((wbl - 1) >> 8)})
	if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
		t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
	}
	b, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(b) != "payload" {
		t.Fatalf("payload is %q, want %q", string(b), "payload")
	}
	if r.Comment != "comment" {
		t.Errorf("comment is %q, want %q", r.Comment, "comment")
	}
	if bl := ExpectedMemberSize(r.Header); bl != len(MagicBlock) {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl)
	}
	if string(r.Extra) != "BC\x02\x00\x1b\x00" {
		t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00\x1b\x00")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Errorf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Errorf("name is %q, want %q", r.Name, "name")
	}
	if err := r.Close(); err != nil {
		t.Errorf("Reader.Close: %v", err)
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
	if _, err := w.Write([]byte("payloadTwo")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}
	wbl[1] = buf.Len() - wbl[0] - len(MagicBlock)

	var (
		b     []byte
		bl, n int
		err   error
	)
	r, err := NewReader(buf, *conc)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Insert a HasEOF to ensure it does not corrupt subsequent reads.
	HasEOF(bytes.NewReader(buf.Bytes()))

	if r.Comment != "comment" {
		t.Errorf("comment is %q, want %q", r.Comment, "comment")
	}
	blEnc := string([]byte{byte(wbl[0] - 1), byte((wbl[0] - 1) >> 8)})
	if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
		t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Errorf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Errorf("name is %q, want %q", r.Name, "name")
	}

	bl = ExpectedMemberSize(r.Header)
	if bl != wbl[0] {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl[0])
	}
	b = make([]byte, len("payload1payloadTwo"))
	n, err = r.Read(b)
	if string(b[:n]) != "payload1payloadTwo" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "payload1payloadTwo")
	}
	if err != nil {
		t.Errorf("Read: %v", err)
	}

	bl = ExpectedMemberSize(r.Header)
	if bl != wbl[1] {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl[1])
	}
	b = make([]byte, 1)
	n, err = r.Read(b)
	if string(b[:n]) != "" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "")
	}
	if err != io.EOF {
		t.Errorf("Read: %v", err)
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
	if err := f.Close(); err != nil {
		t.Fatalf("os.File.Close: %v", err)
	}
	wbl[1] = int(cw.bytes-offset) - len(MagicBlock)

	var (
		b     []byte
		bl, n int
	)

	f, err = os.Open(fname)
	if err != nil {
		t.Fatalf("Reopen temp file: %v", err)
	}
	r, err := NewReader(f, *conc)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	// Insert a HasEOF to ensure it does not corrupt subsequent reads.
	HasEOF(f)

	if r.Comment != "comment" {
		t.Errorf("comment is %q, want %q", r.Comment, "comment")
	}
	blEnc := string([]byte{byte(wbl[0] - 1), byte((wbl[0] - 1) >> 8)})
	if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
		t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
	}
	if r.ModTime.Unix() != 1e8 {
		t.Errorf("mtime is %d, want %d", r.ModTime.Unix(), uint32(1e8))
	}
	if r.Name != "name" {
		t.Errorf("name is %q, want %q", r.Name, "name")
	}
	bl = ExpectedMemberSize(r.Header)
	if bl != wbl[0] {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl[0])
	}
	b = make([]byte, len("payload1payloadTwo")+1)
	n, err = r.Read(b)
	if err != io.EOF {
		t.Errorf("Read: %v", err)
	}
	if bl := ExpectedMemberSize(r.Header); bl != len(MagicBlock) {
		t.Errorf("expectedMemberSize is %d, want %d", bl, len(MagicBlock))
	}
	if string(r.Extra) != "BC\x02\x00\x1b\x00" {
		t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00\x1b\x00")
	}
	if string(b[:n]) != "payload1payloadTwo" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "payload1payloadTwo")
	}
	if err := r.Seek(Offset{}); err != nil {
		t.Errorf("Seek: %v", err)
	}
	n, err = r.Read(b)
	if err != io.EOF {
		t.Errorf("Read: %v", err)
	}
	if string(b[:n]) != "payload1payloadTwo" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "payload1payloadTwo")
	}
	if err := r.Seek(Offset{File: offset}); err != nil {
		t.Fatalf("Seek: %v", err)
	}
	bl = ExpectedMemberSize(r.Header)
	if bl != wbl[1] {
		t.Errorf("expectedMemberSize is %d, want %d", bl, wbl[1])
	}
	b = make([]byte, bl+1)
	n, err = r.Read(b)
	if err != io.EOF {
		t.Errorf("Read: %v", err)
	}
	if string(b[:n]) != "payloadTwo" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "payloadTwo")
	}
	os.Remove(fname)
}

type errorReadSeeker struct {
	r   io.ReadSeeker
	err error
}

func (r errorReadSeeker) Read(p []byte) (int, error) {
	n, err := r.r.Read(p)
	if err == nil && r.err != nil {
		err = r.err
	}
	return n, err
}

func (r errorReadSeeker) Seek(offset int64, whence int) (int64, error) {
	n, err := r.r.Seek(offset, whence)
	if r.err != nil {
		err = r.err
	}
	return n, err
}

func TestSeekErrorDeadlock(t *testing.T) {
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
	e := &errorReadSeeker{r: bytes.NewReader(buf.Bytes())}
	r, err := NewReader(e, *conc)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	r.Seek(Offset{File: 0})
	e.err = errors.New("bad seek error")
	err = r.Seek(Offset{File: 1})
	if err == nil {
		t.Error("Expected error.", err)
	}

}

type countReadSeeker struct {
	r       io.ReadSeeker
	didSeek bool
	n       int64
}

func (r *countReadSeeker) Read(p []byte) (int, error) {
	r.didSeek = false
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *countReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.didSeek = true
	return r.r.Seek(offset, whence)
}

func TestSeekFast(t *testing.T) {
	const (
		infix  = "payload"
		blocks = 10
	)

	// Use different caches.
	for _, cache := range []Cache{
		nil, // Explicitly nil.

		cache.NewLRU(0), // Functionally nil.
		cache.NewLRU(1),
		cache.NewLRU(blocks / 2),
		cache.NewLRU(blocks),
		cache.NewLRU(blocks + 1),

		cache.NewRandom(0), // Functionally nil.
		cache.NewRandom(1),
		cache.NewRandom(blocks / 2),
		cache.NewRandom(blocks),
		cache.NewRandom(blocks + 1),
	} {
		var (
			buf     bytes.Buffer
			offsets = []int{0}
		)
		w := NewWriter(&buf, 1)
		for i := 0; i < blocks; i++ {
			if _, err := fmt.Fprintf(w, "%d%[2]s%[1]d", i, infix); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if err := w.Wait(); err != nil {
				t.Fatalf("Wait: %v", err)
			}
			offsets = append(offsets, buf.Len())
		}
		offsets = offsets[:len(offsets)-1]

		c := &countReadSeeker{r: bytes.NewReader(buf.Bytes())}
		r, err := NewReader(c, *conc)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}

		// Insert a HasEOF to ensure it does not corrupt subsequent reads.
		HasEOF(bytes.NewReader(buf.Bytes()))

		r.Cache = cache
		p := make([]byte, len(infix)+2)

		func() {
			defer func() {
				r := recover()
				if r != nil {
					t.Fatalf("Seek on unread reader panicked: %v", r)
				}
			}()
			err := r.Seek(Offset{})
			if err != nil {
				t.Fatalf("Seek: %v", err)
			}
		}()

		// Standard read through of the data.
		for i := range offsets {
			n, err := r.Read(p)
			if n != len(p) {
				t.Fatalf("Unexpected read length: got:%d want:%d", n, len(p))
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			got := string(p)
			want := fmt.Sprintf("%d%[2]s%[1]d", i, infix)
			if got != want {
				t.Errorf("Unexpected result: got:%q want:%q", got, want)
			}
		}

		// Seek to each block in turn
		for i, o := range offsets {
			err := r.Seek(Offset{File: int64(o)})
			if err != nil {
				t.Fatalf("Seek: %v", err)
			}
			n, err := r.Read(p)
			if n != len(p) {
				t.Errorf("Unexpected read length: got:%d want:%d", n, len(p))
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			got := string(p)
			want := fmt.Sprintf("%d%[2]s%[1]d", i, infix)
			if got != want {
				t.Errorf("Unexpected result: got:%q want:%q", got, want)
			}
		}

		// Seek to each block in turn, but read the infix and then the first 2 bytes.
		for i, o := range offsets {
			if err := r.Seek(Offset{File: int64(o), Block: 1}); err != nil {
				t.Fatalf("Seek: %v", err)
			}
			p = p[:len(infix)]
			n, err := r.Read(p)
			if n != len(p) {
				t.Fatalf("Unexpected read length: got:%d want:%d", n, len(p))
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			got := string(p)
			want := infix
			if got != want {
				t.Fatalf("Unexpected result: got:%q want:%q", got, want)
			}

			// Check whether the underlying reader was seeked or read.
			hasRead := c.n
			if err = r.Seek(Offset{File: int64(o), Block: 0}); err != nil {
				t.Fatalf("Seek: %v", err)
			}
			if b := c.n - hasRead; b != 0 {
				t.Errorf("Seek performed unexpected read: %d bytes", b)
			}
			if c.didSeek {
				t.Error("Seek caused underlying Seek.")
			}

			p = p[:2]
			n, err = r.Read(p)
			if n != len(p) {
				t.Fatalf("Unexpected read length: got:%d want:%d", n, len(p))
			}
			if err != nil {
				t.Fatalf("Read: %v", err)
			}
			got = string(p)
			want = fmt.Sprintf("%dp", i)
			if got != want {
				t.Fatalf("Unexpected result: got:%q want:%q", got, want)
			}
		}
	}
}

func TestCache(t *testing.T) {
	const (
		infix  = "payload"
		blocks = 10
	)

	// Each pattern is a series of seek-and-read (when the element >= 0)
	// or read (when the element < 0). Each read is exactly one block
	// worth of data.
	type opPair struct{ seekBlock, blockID int }
	patterns := []struct {
		ops []opPair

		// One for each cache case below. If new caches are added to the
		// test list, stats must be added here.
		expectedStats []cache.Stats
	}{
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +0, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: -1, blockID: 3},
				{seekBlock: -1, blockID: 4},
			},
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache: LRU(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // LRU(1)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // LRU(5)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // LRU(10)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // FIFO(1)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 4, Evictions: 0}, // FIFO(5)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 4, Evictions: 0}, // FIFO(10)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 4, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // Random(1)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // Random(5)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // Random(10)
				{Gets: 7, Misses: 4, Puts: 7, Retains: 7, Evictions: 0}, // Random(11)
			},
		},
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: -1, blockID: 3},
				{seekBlock: -1, blockID: 4},
				{seekBlock: -1, blockID: 5},
			},
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache.
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 4}, // LRU(1)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // LRU(5)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // LRU(10)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 6}, // FIFO(1)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 5, Evictions: 0}, // FIFO(5)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 5, Evictions: 0}, // FIFO(10)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 5, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 4}, // Random(1)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // Random(5)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // Random(10)
				{Gets: 7, Misses: 5, Puts: 7, Retains: 7, Evictions: 0}, // Random(11)
			},
		},
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +2, blockID: 2},
				{seekBlock: -1, blockID: 3},
				{seekBlock: -1, blockID: 4},
				{seekBlock: -1, blockID: 5},
				{seekBlock: -1, blockID: 6},
			},
			// Re-reading the same block avoids a cache look-up.
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache.
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 5}, // LRU(1)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 1}, // LRU(5)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // LRU(10)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 5}, // FIFO(1)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 1}, // FIFO(5)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // FIFO(10)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 5}, // Random(1)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 1}, // Random(5)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // Random(10)
				{Gets: 6, Misses: 6, Puts: 6, Retains: 6, Evictions: 0}, // Random(11)
			},
		},
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +3, blockID: 3},
				{seekBlock: -1, blockID: 4},
				{seekBlock: -1, blockID: 5},
				{seekBlock: -1, blockID: 6},
				{seekBlock: -1, blockID: 7},
			},
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache.
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // LRU(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // LRU(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // LRU(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // FIFO(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // FIFO(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // FIFO(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // Random(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // Random(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // Random(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // Random(11)
			},
		},
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +4, blockID: 4},
				{seekBlock: -1, blockID: 5},
				{seekBlock: -1, blockID: 6},
				{seekBlock: -1, blockID: 7},
				{seekBlock: -1, blockID: 8},
			},
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache.
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // LRU(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // LRU(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // LRU(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // FIFO(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // FIFO(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // FIFO(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 6}, // Random(1)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 2}, // Random(5)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // Random(10)
				{Gets: 7, Misses: 7, Puts: 7, Retains: 7, Evictions: 0}, // Random(11)
			},
		},
		{
			ops: []opPair{
				{seekBlock: -1, blockID: 0},
				{seekBlock: -1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +1, blockID: 1},
				{seekBlock: +2, blockID: 2},
				{seekBlock: +1, blockID: 1},
				{seekBlock: +1, blockID: 1},
				{seekBlock: -1, blockID: 2},
				{seekBlock: +7, blockID: 7},
				{seekBlock: -1, blockID: 8},
				{seekBlock: -1, blockID: 9},
			},
			expectedStats: []cache.Stats{
				{}, // nil cache.
				{}, // nil cache.
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 4}, // LRU(1)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // LRU(5)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // LRU(10)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // LRU(11)
				{}, // nil cache: FIFO(0)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 8}, // FIFO(1)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 5, Evictions: 0}, // FIFO(5)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 5, Evictions: 0}, // FIFO(10)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 5, Evictions: 0}, // FIFO(11)
				{}, // nil cache: Random(0)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 4}, // Random(1)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // Random(5)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // Random(10)
				{Gets: 9, Misses: 5, Puts: 9, Retains: 9, Evictions: 0}, // Random(11)
			},
		},
	}

	for k, pat := range patterns {
		// Use different caches.
		for j, s := range []Cache{
			nil, // Explicitly nil.

			cache.NewLRU(0), // Functionally nil.
			cache.NewLRU(1),
			cache.NewLRU(blocks / 2),
			cache.NewLRU(blocks),
			cache.NewLRU(blocks + 1),

			cache.NewFIFO(0), // Functionally nil.
			cache.NewFIFO(1),
			cache.NewFIFO(blocks / 2),
			cache.NewFIFO(blocks),
			cache.NewFIFO(blocks + 1),

			cache.NewRandom(0), // Functionally nil.
			cache.NewRandom(1),
			cache.NewRandom(blocks / 2),
			cache.NewRandom(blocks),
			cache.NewRandom(blocks + 1),
		} {
			var (
				buf     bytes.Buffer
				offsets = []int{0}
			)
			w := NewWriter(&buf, 1)
			for i := 0; i < blocks; i++ {
				if _, err := fmt.Fprintf(w, "%d%[2]s%[1]d", i, infix); err != nil {
					t.Fatalf("Write: %v", err)
				}
				if err := w.Flush(); err != nil {
					t.Fatalf("Flush: %v", err)
				}
				if err := w.Wait(); err != nil {
					t.Fatalf("Wait: %v", err)
				}
				offsets = append(offsets, buf.Len())
			}
			offsets = offsets[:len(offsets)-1]

			r, err := NewReader(bytes.NewReader(buf.Bytes()), *conc)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			var stats *cache.StatsRecorder
			if s != nil {
				stats = &cache.StatsRecorder{Cache: s}
				s = stats
			}
			r.Cache = s
			p := make([]byte, len(infix)+2)

			// Insert a HasEOF to ensure it does not corrupt subsequent reads.
			HasEOF(bytes.NewReader(buf.Bytes()))

			for _, op := range pat.ops {
				if op.seekBlock >= 0 {
					err := r.Seek(Offset{File: int64(offsets[op.seekBlock])})
					if err != nil {
						t.Fatalf("Seek: %v", err)
					}
				}
				n, err := r.Read(p)
				if n != len(p) {
					t.Errorf("Unexpected read length: got:%d want:%d", n, len(p))
				}
				if err != nil {
					t.Fatalf("Read: %v", err)
				}
				got := string(p)
				want := fmt.Sprintf("%d%[2]s%[1]d", op.blockID, infix)
				if got != want {
					t.Errorf("Unexpected result: got:%q want:%q", got, want)
				}
			}
			if stats != nil && stats.Stats() != pat.expectedStats[j] {
				t.Errorf("Unexpected result for cache %d pattern %d: got:%+v want:%+v", j, k, stats.Stats(), pat.expectedStats[j])
			}
		}
	}
}

type zero struct{}

func (z zero) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

func TestWriteByteCount(t *testing.T) {
	cw, _ := NewWriterLevel(ioutil.Discard, gzip.BestCompression, 4)
	defer cw.Close()
	n, err := io.Copy(cw, &io.LimitedReader{R: new(zero), N: 100000})
	if n != 100000 {
		t.Errorf("Unexpected number of bytes, got:%d, want:%d", n, 100000)
	}
	if err != nil {
		t.Errorf("Unexpected error got:%v", err)
	}
}

func BenchmarkWrite(b *testing.B) {
	bg := NewWriter(ioutil.Discard, *conc)
	block := bytes.Repeat([]byte("repeated"), 50)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			bg.Write(block)
		}
		bg.Wait()
	}
}
