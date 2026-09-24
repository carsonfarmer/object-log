package main

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"os/exec"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/storage/memory"
	gitbinary "github.com/go-git/go-git/v6/utils/binary"
)

type importStorage struct {
	*memory.Storage
	format format.ObjectFormat
	writes int
}

func newImportStorage(f format.ObjectFormat) *importStorage {
	s := &importStorage{Storage: memory.NewStorage(), format: f}
	_ = s.SetObjectFormat(f)
	return s
}
func (s *importStorage) RawObjectWriter(kind plumbing.ObjectType, size int64) (io.WriteCloser, error) {
	s.writes++
	o := plumbing.NewMemoryObject(plumbing.FromObjectFormat(s.format))
	o.SetType(kind)
	w, _ := o.Writer()
	return &importWriter{Writer: w, s: s, object: o, size: size}, nil
}

type importWriter struct {
	io.Writer
	s      *importStorage
	object plumbing.EncodedObject
	size   int64
}

func (w *importWriter) Close() error {
	if w.object.Size() != w.size {
		return io.ErrUnexpectedEOF
	}
	_, err := w.s.SetEncodedObject(w.object)
	return err
}

type packFixtureEntry struct {
	kind     plumbing.ObjectType
	data     []byte
	ref      plumbing.Hash
	ofs      int // index of base entry
	distance int64
}

func fixturePack(t *testing.T, f format.ObjectFormat, entries []packFixtureEntry) []byte {
	t.Helper()
	var b bytes.Buffer
	b.WriteString("PACK")
	_ = binary.Write(&b, binary.BigEndian, uint32(2))
	_ = binary.Write(&b, binary.BigEndian, uint32(len(entries)))
	var offsets []int
	for _, e := range entries {
		offset := b.Len()
		size := len(e.data)
		first := byte(e.kind)<<4 | byte(size&15)
		size >>= 4
		if size != 0 {
			first |= 128
		}
		b.WriteByte(first)
		for size != 0 {
			next := byte(size & 127)
			size >>= 7
			if size != 0 {
				next |= 128
			}
			b.WriteByte(next)
		}
		if e.kind == plumbing.REFDeltaObject {
			b.Write(e.ref.Bytes())
		}
		if e.kind == plumbing.OFSDeltaObject {
			distance := e.distance
			if distance == 0 {
				distance = int64(offset - offsets[e.ofs])
			}
			if err := gitbinary.WriteVariableWidthInt(&b, distance); err != nil {
				t.Fatal(err)
			}
		}
		offsets = append(offsets, offset)
		z := zlib.NewWriter(&b)
		_, _ = z.Write(e.data)
		if err := z.Close(); err != nil {
			t.Fatal(err)
		}
	}
	var h hash.Hash = sha1.New()
	if f == format.SHA256 {
		h = sha256.New()
	}
	_, _ = h.Write(b.Bytes())
	b.Write(h.Sum(nil))
	return b.Bytes()
}
func blobID(f format.ObjectFormat, data []byte) plumbing.Hash {
	h := plumbing.NewHasher(f, plumbing.BlobObject, int64(len(data)))
	_, _ = h.Write(data)
	return h.Sum()
}
func TestImportPackRoundTrip(t *testing.T) {
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(string(f), func(t *testing.T) {
			base := []byte("abcdefghijklmnopqrst")
			delta := []byte{20, 3, 0x91, 10, 1, 0x90, 1, 0x91, 13, 1}
			for _, mode := range []string{"ref", "ofs", "forward-ref", "thin", "chain", "empty"} {
				t.Run(mode, func(t *testing.T) {
					s := newImportStorage(f)
					entries := []packFixtureEntry{{kind: plumbing.BlobObject, data: base}, {kind: plumbing.REFDeltaObject, data: delta, ref: blobID(f, base)}}
					expected := []byte("kan")
					switch mode {
					case "ofs":
						entries[1].kind = plumbing.OFSDeltaObject
					case "forward-ref":
						entries[0], entries[1] = entries[1], entries[0]
					case "thin":
						w, _ := s.RawObjectWriter(plumbing.BlobObject, int64(len(base)))
						_, _ = w.Write(base)
						_ = w.Close()
						entries = entries[1:]
					case "chain":
						entries = append(entries, packFixtureEntry{kind: plumbing.REFDeltaObject, data: []byte{3, 1, 0x91, 2, 1}, ref: blobID(f, expected)})
						expected = []byte("n")
					case "empty":
						entries = []packFixtureEntry{{kind: plumbing.BlobObject}, {kind: plumbing.REFDeltaObject, data: []byte{0, 0}, ref: blobID(f, nil)}}
						expected = nil
					}
					packed := fixturePack(t, f, entries)
					if err := importPack(context.Background(), bytes.NewReader(packed), s, f, testPackLimits(1024)); err != nil {
						t.Fatal(err)
					}
					o, err := s.EncodedObject(plumbing.BlobObject, blobID(f, expected))
					if err != nil {
						t.Fatal(err)
					}
					r, err := o.Reader()
					if err != nil {
						t.Fatal(err)
					}
					got, err := io.ReadAll(r)
					_ = r.Close()
					if err != nil || !bytes.Equal(got, expected) {
						t.Fatalf("got %q: %v", got, err)
					}
					if mode == "thin" || mode == "empty" {
						return
					}
					// Native Git independently authenticates the same self-contained pack.
					dir := t.TempDir()
					cmd := exec.Command("git", "init", "--bare", "--object-format="+string(f), dir)
					if out, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("git init: %s: %v", out, err)
					}
					cmd = exec.Command("git", "-C", dir, "index-pack", "--stdin")
					cmd.Stdin = bytes.NewReader(packed)
					if out, err := cmd.CombinedOutput(); err != nil {
						t.Fatalf("git index-pack: %s: %v", out, err)
					}
					cmd = exec.Command("git", "-C", dir, "cat-file", "blob", blobID(f, expected).String())
					out, err := cmd.Output()
					if err != nil || !bytes.Equal(out, expected) {
						t.Fatalf("native output %q: %v", out, err)
					}
				})
			}
		})
	}
}
func TestImportPackRejectsInvalid(t *testing.T) {
	f := format.SHA1
	base := []byte("abc")
	for _, tc := range []struct {
		name  string
		delta []byte
	}{
		{"truncated-literal", []byte{3, 3, 3, 'x'}},
		{"truncated-copy", []byte{3, 3, 0x91}},
		{"copy-outside-base", []byte{3, 1, 0x91, 3, 1}},
		{"trailing-instructions", []byte{3, 0, 0}},
		{"oversized-result", []byte{3, 0x81, 8}},
		{"mismatched-base-size", []byte{0x81, 8, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newImportStorage(f)
			packed := fixturePack(t, f, []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: tc.delta, ref: blobID(f, base)}, {kind: plumbing.BlobObject, data: base}})
			if err := importPack(context.Background(), bytes.NewReader(packed), s, f, testPackLimits(1024)); err == nil {
				t.Fatal("accepted malformed delta")
			}
		})
	}
	for _, mode := range []string{"checksum", "trailing", "truncated", "missing-base", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			entries := []packFixtureEntry{{kind: plumbing.BlobObject, data: base}}
			if mode == "missing-base" {
				entries = []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: []byte{3, 0}, ref: blobID(f, base)}}
			}
			p := fixturePack(t, f, entries)
			ctx := context.Background()
			switch mode {
			case "checksum":
				p[len(p)-1] ^= 1
			case "trailing":
				p = append(p, 0)
			case "truncated":
				p = p[:len(p)-1]
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			err := importPack(ctx, bytes.NewReader(p), newImportStorage(f), f, testPackLimits(1024))
			if err == nil {
				t.Fatal("accepted invalid pack")
			}
			if mode == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if mode == "missing-base" && !errors.Is(err, packfile.ErrReferenceDeltaNotFound) {
				t.Fatal(err)
			}
		})
	}
}

// A virtual base exercises failures without storing the object in the fixture.
type repeatedObject struct {
	plumbing.EncodedObject
	size         int64
	actual       int64
	opens, reads int
	openError    error
}

func (o *repeatedObject) Size() int64               { return o.size }
func (o *repeatedObject) Type() plumbing.ObjectType { return plumbing.BlobObject }
func (o *repeatedObject) Reader() (io.ReadCloser, error) {
	o.opens++
	if o.openError != nil {
		return nil, o.openError
	}
	return io.NopCloser(io.LimitReader(&repeatReader{o: o}, o.actual)), nil
}

type repeatReader struct{ o *repeatedObject }

func (r *repeatReader) Read(p []byte) (int, error) {
	r.o.reads++
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

type importSinkStorage struct {
	*memory.Storage
	base      *repeatedObject
	writes    int64
	sinkError error
	cancel    context.CancelFunc
}

func (s *importSinkStorage) EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	return s.base, nil
}
func (s *importSinkStorage) RawObjectWriter(plumbing.ObjectType, int64) (io.WriteCloser, error) {
	return importSink{s}, nil
}

type importSink struct{ s *importSinkStorage }

func (w importSink) Write(p []byte) (int, error) {
	if w.s.sinkError != nil {
		return 0, w.s.sinkError
	}
	if w.s.cancel != nil {
		w.s.cancel()
	}
	w.s.writes += int64(len(p))
	return len(p), nil
}
func (w importSink) Close() error { return nil }
func TestImportPackBaseAndSinkFailures(t *testing.T) {
	for _, mode := range []string{"short-base", "base-open", "sink"} {
		t.Run(mode, func(t *testing.T) {
			injected := errors.New("injected failure")
			s := &importSinkStorage{base: &repeatedObject{size: 3, actual: 3}}
			switch mode {
			case "short-base":
				s.base.actual = 1
			case "base-open":
				s.base.openError = injected
			case "sink":
				s.sinkError = injected
			}
			packed := fixturePack(t, format.SHA1, []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: []byte{3, 3, 0x90, 3}, ref: blobID(format.SHA1, []byte("base"))}})
			err := importPack(context.Background(), bytes.NewReader(packed), s, format.SHA1, testPackLimits(1024))
			if err == nil {
				t.Fatal("accepted failed object read/write")
			}
			if mode == "sink" && !errors.Is(err, injected) {
				t.Fatal(err)
			}
		})
	}
}

func TestImportPackRejectsInvalidOFS(t *testing.T) {
	for _, distance := range []int64{1, 1 << 20} {
		t.Run(fmt.Sprint(distance), func(t *testing.T) {
			packed := fixturePack(t, format.SHA1, []packFixtureEntry{{kind: plumbing.BlobObject, data: []byte("a")}, {kind: plumbing.OFSDeltaObject, data: []byte{1, 0}, distance: distance}})
			if err := importPack(context.Background(), bytes.NewReader(packed), newImportStorage(format.SHA1), format.SHA1, testPackLimits(1024)); err == nil {
				t.Fatal("accepted OFS reference outside entry boundaries")
			}
		})
	}
}
func TestImportPackBoundsDeltaDepth(t *testing.T) {
	entries := []packFixtureEntry{{kind: plumbing.BlobObject, data: []byte("a")}}
	for i := 0; i < 4096; i++ {
		entries = append(entries, packFixtureEntry{kind: plumbing.OFSDeltaObject, data: []byte{1, 1, 0x90, 1}, ofs: i})
	}
	packed := fixturePack(t, format.SHA1, entries)
	if err := importPack(context.Background(), bytes.NewReader(packed), newImportStorage(format.SHA1), format.SHA1, testPackLimits(1024)); !errors.Is(err, packfile.ErrMalformedPackfile) {
		t.Fatalf("depth error: %v", err)
	}
}

func TestImportPackObjectFraming(t *testing.T) {
	for _, mode := range []string{"empty-pack", "long-object", "reserved-kind", "version", "zlib-checksum"} {
		t.Run(mode, func(t *testing.T) {
			f := format.SHA1
			entries := []packFixtureEntry{{kind: plumbing.BlobObject, data: []byte("abc")}}
			if mode == "empty-pack" {
				entries = nil
			}
			packed := fixturePack(t, f, entries)
			switch mode {
			case "long-object":
				packed[12] = 0x32
			case "reserved-kind":
				packed[12] = 0x53
			case "version":
				packed[7] = 4
			case "zlib-checksum":
				packed[len(packed)-f.Size()-1] ^= 1
			}
			digest := sha1.Sum(packed[:len(packed)-f.Size()])
			copy(packed[len(packed)-f.Size():], digest[:])
			err := importPack(context.Background(), bytes.NewReader(packed), newImportStorage(f), f, testPackLimits(1024))
			if (err == nil) != (mode == "empty-pack") {
				t.Fatalf("result: %v", err)
			}
		})
	}
}

func testPackLimits(size int64) requestLimits {
	return requestLimits{objectBytes: size, metadataBytes: size, packObjects: 1_000_000}
}

func TestImportPackRejectsOversizedDeltaBeforeStorage(t *testing.T) {
	for _, objectFormat := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(string(objectFormat), func(t *testing.T) {
			delta := []byte{4, 8, 0x90, 4, 0x90, 4}
			packed := fixturePack(t, objectFormat, []packFixtureEntry{
				{kind: plumbing.BlobObject, data: []byte("abcd")},
				{kind: plumbing.OFSDeltaObject, ofs: 0, data: delta},
			})
			for _, tc := range []struct {
				limit   int64
				allowed bool
			}{
				{6, false},
				{8, true},
			} {
				s := newImportStorage(objectFormat)
				err := importPack(t.Context(), bytes.NewReader(packed), s, objectFormat, testPackLimits(tc.limit))
				if tc.allowed {
					if err != nil || s.writes != 2 {
						t.Fatalf("limit %d: %v; writes=%d", tc.limit, err, s.writes)
					}
				} else if !errors.Is(err, errObjectLimit) || s.writes != 1 {
					t.Fatalf("limit %d: %v; writes=%d", tc.limit, err, s.writes)
				}
			}
			// A delta referencing an external base must be rejected on its
			// declared result size before the parser tries to find the base.
			thin := fixturePack(t, objectFormat, []packFixtureEntry{
				{kind: plumbing.REFDeltaObject, ref: blobID(objectFormat, []byte("abcd")), data: delta},
			})
			if err := importPack(t.Context(), bytes.NewReader(thin), newImportStorage(objectFormat), objectFormat, testPackLimits(6)); !errors.Is(err, errObjectLimit) {
				t.Fatalf("external base: %v", err)
			}
		})
	}
}

func TestPackEntryBounds(t *testing.T) {
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(f.String(), func(t *testing.T) {
			limits := testPackLimits(1024)
			limits.packObjects = 2
			for _, n := range []int{2, 3} {
				entries := make([]packFixtureEntry, n)
				for i := range entries {
					entries[i].kind = plumbing.BlobObject
				}
				s := newImportStorage(f)
				err := importPack(context.Background(), bytes.NewReader(fixturePack(t, f, entries)), s, f, limits)
				if n == 2 {
					if err != nil || s.writes != n {
						t.Fatalf("boundary: %v writes=%d", err, s.writes)
					}
				} else if !errors.Is(err, errObjectLimit) || s.writes != 0 {
					t.Fatalf("count overflow: %v writes=%d", err, s.writes)
				}
			}
			// Reject an excessive declaration before trying to read an entry.
			oversized := []byte("PACK\x00\x00\x00\x02\xff\xff\xff\xff")
			if err := importPack(context.Background(), bytes.NewReader(oversized), newImportStorage(f), f, limits); !errors.Is(err, errObjectLimit) {
				t.Fatalf("header count: %v", err)
			}
		})
	}
}

func (s *importStorage) LowMemoryMode() bool     { return true }
func (s *importSinkStorage) LowMemoryMode() bool { return true }

func TestImportPackRejectsTrailingBytesWithDelta(t *testing.T) {
	t.Parallel()
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(string(f), func(t *testing.T) {
			t.Parallel()
			packed := fixturePack(t, f, []packFixtureEntry{{kind: plumbing.REFDeltaObject, data: []byte{3, 1, 0x90, 1}, ref: blobID(f, []byte("abc"))}})
			s := &importSinkStorage{base: &repeatedObject{size: 3, actual: 3}}
			err := importPack(t.Context(), bytes.NewReader(append(packed, 0)), s, f, testPackLimits(1024))
			if err == nil {
				t.Fatal("accepted trailing bytes")
			}
		})
	}
}

func TestImportPackCancellationDuringInflation(t *testing.T) {
	t.Parallel()
	data := make([]byte, 1<<20)
	var state uint32 = 1
	for i := range data {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		data[i] = byte(state)
	}
	packed := fixturePack(t, format.SHA1, []packFixtureEntry{{kind: plumbing.BlobObject, data: data}})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	s := &importSinkStorage{cancel: cancel}
	err := importPack(ctx, bytes.NewReader(packed), s, format.SHA1, testPackLimits(int64(len(data))))
	if !errors.Is(err, context.Canceled) || s.writes == 0 || s.writes >= int64(len(data)) {
		t.Fatalf("canceled inflation: err=%v, wrote %d/%d bytes", err, s.writes, len(data))
	}
}
