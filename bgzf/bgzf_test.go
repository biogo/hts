// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the Go LICENSE file.

// Changes copyright ©2012 The bíogo Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package bgzf_test

import (
	"bytes"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/biogo/hts/bgzf"
	"github.com/biogo/hts/bgzf/cache"
)

var (
	conc = flag.Int("conc", 1, "sets the level of concurrency for compression")
	file = flag.String("bench.file", "", "bgzf file to read for benchmarking decompression")
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

	if err := NewWriter(buf, *conc).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	r, err := NewReader(buf, *conc)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	b, err := io.ReadAll(r)
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
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("Create temp file: %v", err)
	}
	fname := f.Name()

	if err := NewWriter(f, *conc).Close(); err != nil {
		t.Fatalf("Writer.Close: %v", err)
	}

	if err := f.Close(); err != nil {
		t.Fatalf("f.Close: %v", err)
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
	if err := f.Close(); err != nil {
		t.Fatalf("f.Close: %v", err)
	}

	if runtime.GOOS != "windows" {
		// NOTE: on Windows, os.Open is not allowed for directories.
		f, err = os.Open(t.TempDir())
		if err != nil {
			t.Fatalf("Open temp dir: %v", err)
		}
		ok, err = HasEOF(f)
		if want := "read " + t.TempDir() + ": is a directory"; err.Error() != want {
			t.Errorf("Expected error:%s got:%v", want, err)
		}
		if ok {
			t.Error("Unexpected EOF in os.File IsDir: found.")
		}
		if err := f.Close(); err != nil {
			t.Fatalf("f.Close: %v", err)
		}
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
	if !errors.Is(err, ErrNoEnd) {
		t.Errorf("Expected error:%s got:%v", ErrNoEnd, err)
	}
	if ok {
		t.Error("Unexpected EOF in crippled ReaderAt: found.")
	}
}

// TestRoundTrip tests that bgzipping and then bgunzipping is the identity
// function.
func TestRoundTrip(t *testing.T) {
	for _, reader := range []struct {
		name    string
		readAll func(*Reader) ([]byte, error)
	}{
		{"io.Reader", readAllWrapper},
		{"io.ByteReader", readAllByByte},
	} {
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

		if bl := ExpectedMemberSize(r.Header); bl != wbl {
			t.Errorf("expectedMemberSize is %d, want %d", bl, wbl)
		}
		blEnc := string([]byte{byte(wbl - 1), byte((wbl - 1) >> 8)})
		if string(r.Extra) != "BC\x02\x00"+blEnc+"extra" {
			t.Errorf("extra is %q, want %q", r.Extra, "BC\x02\x00"+blEnc+"extra")
		}
		b, err := reader.readAll(r)
		if err != nil {
			t.Fatalf("%s readAll: %v", reader.name, err)
		}
		if string(b) != "payload" {
			t.Fatalf("%s payload is %q, want %q", reader.name, string(b), "payload")
		}
		if r.Comment != "comment" {
			t.Errorf("comment is %q, want %q", r.Comment, "comment")
		}
		if bl := ExpectedMemberSize(r.Header); bl != len(MagicBlock) {
			t.Errorf("expectedMemberSize is %d, want %d", bl, len(MagicBlock))
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
		if r.OS != 0xff {
			t.Errorf("os is %x, want %x", r.OS, 0xff)
		}
		if err := r.Close(); err != nil {
			t.Errorf("Reader.Close: %v", err)
		}
	}
}

func readAllWrapper(r *Reader) ([]byte, error) {
	return io.ReadAll(r)
}

func readAllByByte(r *Reader) ([]byte, error) {
	var (
		buf []byte
		err error
		b   byte
	)
	for {
		b, err = r.ReadByte()
		if err != nil {
			break
		}
		buf = append(buf, b)
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	return buf, err
}

// TestRoundTripMulti tests that bgzipping and then bgunzipping is the identity
// function for a multiple member bgzf.
func TestRoundTripMulti(t *testing.T) {
	for _, reader := range []struct {
		name string
		read func(*Reader, []byte) (int, error)
	}{
		{"io.Reader", readWrapper},
		{"io.ByteReader", readByByte},
	} {
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
		n, err = reader.read(r, b)
		if string(b[:n]) != "payload1payloadTwo" {
			t.Errorf("%s payload is %q, want %q", reader.name, string(b[:n]), "payload1payloadTwo")
		}
		if err != nil {
			t.Errorf("%s read: %v", reader.name, err)
		}

		bl = ExpectedMemberSize(r.Header)
		if bl != wbl[1] {
			t.Errorf("expectedMemberSize is %d, want %d", bl, wbl[1])
		}
		b = make([]byte, 1)
		n, err = reader.read(r, b)
		if string(b[:n]) != "" {
			t.Errorf("%s payload is %q, want %q", reader.name, string(b[:n]), "")
		}
		if !errors.Is(err, io.EOF) {
			t.Errorf("%s read: %v", reader.name, err)
		}
		r.Close()
	}
}

func readWrapper(r *Reader, buf []byte) (int, error) {
	return r.Read(buf)
}

func readByByte(r *Reader, buf []byte) (n int, err error) {
	for range buf {
		buf[n], err = r.ReadByte()
		if err != nil {
			break
		}
		n++
	}
	return n, err
}

// See https://github.com/biogo/hts/issues/57
func TestHeaderIssue57(t *testing.T) {
	var stamp time.Time

	var buf bytes.Buffer
	bg := NewWriter(&buf, *conc)
	bg.ModTime = stamp
	bg.OS = 0xff
	err := bg.Close()
	if err != nil {
		t.Fatal("error closing Writer")
	}
	got := buf.Bytes()[:16]
	want := []byte(MagicBlock[:16])
	if !bytes.Equal(got, want) {
		t.Fatalf("unexpected header:\ngot: %0#2v\nwant:%0#2v", got, want)
	}
}

// TestRoundTripMultiSeek tests that bgzipping and then bgunzipping is the identity
// function for a multiple member bgzf with an underlying Seeker.
func TestRoundTripMultiSeek(t *testing.T) {
	f, err := os.Create(filepath.Join(t.TempDir(), "data"))
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
	if !errors.Is(err, io.EOF) {
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
	if !errors.Is(err, io.EOF) {
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
	if !errors.Is(err, io.EOF) {
		t.Errorf("Read: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Errorf("r.Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Errorf("f.Close: %v", err)
	}
	if string(b[:n]) != "payloadTwo" {
		t.Errorf("payload is %q, want %q", string(b[:n]), "payloadTwo")
	}
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
	r.Close()
}

type countReadSeeker struct {
	mu       sync.Mutex
	r        io.ReadSeeker
	_didSeek bool
	n        int64
}

func (r *countReadSeeker) offset() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.n
}

func (r *countReadSeeker) didSeek() bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r._didSeek
}

func (r *countReadSeeker) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r._didSeek = false
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

func (r *countReadSeeker) Seek(offset int64, whence int) (int64, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r._didSeek = true
	return r.r.Seek(offset, whence)
}

func TestSeekFast(t *testing.T) {
	// Under these conditions we cannot guarantee that a worker
	// will not read bytes after a Seek call has been made.
	if *conc != 1 && runtime.GOMAXPROCS(0) > 1 {
		return
	}
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
		w.Close()
		offsets = offsets[:len(offsets)-1]

		c := &countReadSeeker{r: bytes.NewReader(buf.Bytes())}

		// Insert a HasEOF to ensure it does not corrupt subsequent reads.
		HasEOF(bytes.NewReader(buf.Bytes()))

		r, err := NewReader(c, *conc)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}

		r.SetCache(cache)
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
			hasRead := c.offset()
			if err = r.Seek(Offset{File: int64(o), Block: 0}); err != nil {
				t.Fatalf("Seek: %v", err)
			}
			if b := c.offset() - hasRead; b != 0 {
				t.Errorf("Seek performed unexpected read: %d bytes", b)
			}
			if c.didSeek() {
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
		r.Close()
	}
}

func TestCache(t *testing.T) {
	// Under these conditions we cannot guarantee that the order of
	// blocks returned by nextBlock work will not result in additional
	// cache puts.
	if *conc != 1 {
		return
	}
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
			w.Close()
			offsets = offsets[:len(offsets)-1]

			br := bytes.NewReader(buf.Bytes())
			// Insert a HasEOF to ensure it does not corrupt subsequent reads.
			HasEOF(br)

			r, err := NewReader(br, *conc)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			var stats *cache.StatsRecorder
			if s != nil {
				stats = &cache.StatsRecorder{Cache: s}
				s = stats
			}
			r.SetCache(s)
			p := make([]byte, len(infix)+2)

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
			r.Close()
		}
	}
}

func TestBlocked(t *testing.T) {
	const (
		infix  = "payload"
		blocks = 10
	)

	for _, blocked := range []bool{false, true} {
		var (
			buf  bytes.Buffer
			want bytes.Buffer
		)
		w := NewWriter(&buf, 1)
		for i := 0; i < blocks; i++ {
			if _, err := fmt.Fprintf(w, "%d%[2]s%[1]d\n", i, infix); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if err := w.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}
			if _, err := fmt.Fprintf(&want, "%d%[2]s%[1]d\n", i, infix); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		err := w.Close()
		if err != nil {
			t.Fatalf("unexpected error on Close: %v", err)
		}

		r, err := NewReader(bytes.NewReader(buf.Bytes()), *conc)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		r.Blocked = blocked

		p := make([]byte, len(infix))
		var (
			got       []byte
			gotBlocks int
		)
		for {
			n, err := r.Read(p)
			got = append(got, p[:n]...)
			if err != nil {
				if errors.Is(err, io.EOF) && n != 0 {
					gotBlocks++
					continue
				}
				break
			}
		}
		if !blocked && gotBlocks != 1 {
			t.Errorf("unexpected number of blocks:\n\tgot:%d\n\twant:%d", gotBlocks, 1)
		}
		if blocked && gotBlocks != blocks {
			t.Errorf("unexpected number of blocks:\n\tgot:%d\n\twant:%d", gotBlocks, blocks)
		}
		if !bytes.Equal(got, want.Bytes()) {
			t.Errorf("unexpected result:\n\tgot:%q\n\twant:%q", got, want.Bytes())
		}
		r.Close()
	}
}

var fuzzCrashers = []string{
	// Invalid block size.
	"\x1f\x8b\bu0000000\x0000000000" +
		"000000000BC\x02\x000\x0000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"000000000000000000\x00",
	"\x1f\x8b\b\xc4000000V\x0000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"000000000000BC\x02\x00w\x00\x030" +
		"\x00\x00\x00\x00\x00\x00\x00\x00\x1f\x8b\bu000000\x00\x00" +
		"\x1f\x8b\bu000000\b\x00BC\x02\x00\x00\x0000" +
		"\x00",

	// Zero block size.
	"\x1f\x8b\bu000000V\x0000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"00000000000000000000" +
		"000000000000BC\x02\x00k\x0000" +
		"0000000\x00",
	"\x1f\x8b\bu\xe8k\x15k\x00sV\x00bcdefghi" +
		"jklmnxpq\xc49\xbf\x1f\x8b\x0f\a/\x85\xba\xb0Q" +
		"\xef (\x01\xbd\xbf\xefrde\a/\x85fghmjt\x00" +
		"\xff\x00\x00v\x97x\x92zB\x80\x00142261869" +
		"48039093abxdBC\x02\x00i\x00sV" +
		"\xbbghmj\x00\x00",
}

func TestFuzzCrashers(t *testing.T) {
	for i, test := range fuzzCrashers {
		func() {
			i := i
			defer func() {
				r := recover()
				if r != nil {
					t.Errorf("unexpected panic for crasher %d: %v", i, r)
				}
			}()
			r, err := NewReader(strings.NewReader(test), 0)
			if err != nil {
				if errors.Is(err, io.EOF) ||
					errors.Is(err, io.ErrUnexpectedEOF) ||
					errors.Is(err, ErrCorrupt) {
					return
				}
				t.Fatalf("unexpected error creating reader: %v", err)
			}

			tmp := make([]byte, 1024)
			for {
				_, err := r.Read(tmp)
				if err != nil {
					break
				}
			}
		}()
	}
}

func TestZeroNonZero(t *testing.T) {
	const wrote = "second block"
	buf := bytes.NewBuffer([]byte(MagicBlock))
	w := NewWriter(buf, 1)
	_, err := w.Write([]byte(wrote))
	if err != nil {
		w.Close()
		t.Fatalf("unexpected error writing second block: %v", err)
	}
	err = w.Close()
	if err != nil {
		t.Fatalf("unexpected error closing writer: %v", err)
	}
	r, err := NewReader(buf, 1)
	if err != nil {
		t.Fatalf("unexpected error opening reader: %v", err)
	}
	defer r.Close()
	var b [1024]byte
	var got []byte
	for {
		n, err := r.Read(b[:])
		got = append(got, b[:n]...)
		if err != nil {
			break
		}
	}
	if string(got) != wrote {
		t.Errorf("unexpected round trip: got:%q want:%q", got, wrote)
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
	cw, _ := NewWriterLevel(io.Discard, gzip.BestCompression, 4)
	defer cw.Close()
	n, err := io.Copy(cw, &io.LimitedReader{R: new(zero), N: 100000})
	if n != 100000 {
		t.Errorf("Unexpected number of bytes, got:%d, want:%d", n, 100000)
	}
	if err != nil {
		t.Errorf("Unexpected error got:%v", err)
	}
}

func TestSeekCacheReadahead(t *testing.T) {
	// Check that we see the correct behavior when seeking to the block at the
	// head of the readahead queue with caching and readahead enabled.
	// See https://github.com/biogo/hts/issues/159.
	fh, err := os.Open("testdata/dbscSNV_sample.gz")
	if err != nil {
		t.Fatalf("Open() error:%v", err)
	}
	rd, err := NewReader(fh, 2)
	if err != nil {
		t.Fatalf("NewReader() error:%v", err)
	}
	rd.SetCache(cache.NewLRU(32))
	buf := make([]byte, 2)
	// Wait for readahead.
	time.Sleep(100 * time.Millisecond)
	// Seek to the beginning of the second block.
	off := Offset{File: 9395, Block: 0}
	err = rd.Seek(off)
	if err != nil {
		t.Fatalf("Seek() error:%v", err)
	}
	_, err = rd.Read(buf)
	if err != nil {
		t.Fatalf("Read() error:%v", err)
	}
	if !bytes.Equal(buf, []byte{'O', 'X'}) {
		t.Errorf("Expected 'OX', got '%s'", buf)
	}
	if rd.LastChunk().Begin != off {
		t.Errorf("Incorrect position: expected %+v, got %+v", off, rd.LastChunk().Begin)
	}
}

func BenchmarkWrite(b *testing.B) {
	bg := NewWriter(io.Discard, *conc)
	block := bytes.Repeat([]byte("repeated"), 50)
	for i := 0; i < b.N; i++ {
		for j := 0; j < 1000000; j++ {
			bg.Write(block)
		}
		bg.Wait()
	}
}

func BenchmarkRead(b *testing.B) {
	if *file == "" {
		b.Skip("no bgzf file specified")
	}
	f, err := os.Open(*file)
	if err != nil {
		b.Fatalf("file open failed: %v", err)
	}
	defer f.Close()

	buf := make([]byte, 16384)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f.Seek(0, io.SeekStart)
		bg, err := NewReader(f, *conc)
		if err != nil {
			b.Fatalf("bgzf open failed: %v", err)
		}
		for {
			_, err = bg.Read(buf)
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				b.Fatalf("bgzf read failed: %v", err)
			}
		}
		bg.Close()
	}
}
