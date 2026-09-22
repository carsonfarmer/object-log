package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
	"io"
	"math"
	"strings"
	"testing"
)

type closeProbe struct {
	*bytes.Reader
	closed   bool
	closeErr error
}

func (r *closeProbe) Close() error { r.closed = true; return r.closeErr }

func TestLooseRoundTripAndValidation(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			for _, data := range [][]byte{nil, bytes.Repeat([]byte("compressible\n"), 100000)} {
				var encoded bytes.Buffer
				w := objfile.NewWriter(&encoded, format)
				if err := w.WriteHeader(plumbing.BlobObject, int64(len(data))); err != nil {
					t.Fatal(err)
				}
				if _, err := w.Write(data); err != nil {
					t.Fatal(err)
				}
				if err := w.Close(); err != nil {
					t.Fatal(err)
				}
				source := &closeProbe{Reader: bytes.NewReader(encoded.Bytes())}
				r, err := readLoose(source, format, plumbing.BlobObject, int64(len(data)), w.Hash())
				if err != nil {
					t.Fatal(err)
				}
				var failure error
				r.(*looseReader).failure = &failure
				got, err := io.ReadAll(r)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("roundtrip: %v", err)
				}
				if err = r.Close(); err != nil || !source.closed {
					t.Fatal("source not closed")
				}
				if failure != nil {
					t.Fatalf("successful read recorded failure: %v", failure)
				}
				for _, test := range []struct {
					name       string
					compressed []byte
					kind       plumbing.ObjectType
					size       int64
					id         plumbing.Hash
				}{
					{"wrong-kind", encoded.Bytes(), plumbing.TreeObject, int64(len(data)), w.Hash()},
					{"wrong-size", encoded.Bytes(), plumbing.BlobObject, int64(len(data)) + 1, w.Hash()},
					{"wrong-id", encoded.Bytes(), plumbing.BlobObject, int64(len(data)), plumbing.NewHash("1234567890123456789012345678901234567890")},
					{"truncated", encoded.Bytes()[:encoded.Len()-2], plumbing.BlobObject, int64(len(data)), w.Hash()},
				} {
					source = &closeProbe{Reader: bytes.NewReader(test.compressed)}
					r, err = readLoose(source, format, test.kind, test.size, test.id)
					if err == nil {
						failure = nil
						r.(*looseReader).failure = &failure
						_, err = io.ReadAll(r)
						_ = r.Close()
						if failure == nil || !errors.Is(failure, err) {
							t.Fatalf("%s: read failure was not retained: %v / %v", test.name, err, failure)
						}
					}
					if err == nil || !source.closed {
						t.Fatalf("%s: accepted or leaked source: %v %v", test.name, err, source.closed)
					}
				}
				if len(data) > 0 && encoded.Len() >= len(data)/100 {
					t.Fatal("fixture did not compress")
				}
			}
		})
	}
}

func TestLooseReaderRecordsCloseFailure(t *testing.T) {
	var encoded bytes.Buffer
	w := objfile.NewWriter(&encoded, config.SHA1)
	if err := w.WriteHeader(plumbing.BlobObject, 0); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	want := errors.New("source close failed")
	source := &closeProbe{Reader: bytes.NewReader(encoded.Bytes()), closeErr: want}
	r, err := readLoose(source, config.SHA1, plumbing.BlobObject, 0, w.Hash())
	if err != nil {
		t.Fatal(err)
	}
	var failure error
	r.(*looseReader).failure = &failure
	if err := r.Close(); !errors.Is(err, want) || !errors.Is(failure, want) {
		t.Fatalf("close failure was not retained: %v / %v", err, failure)
	}
}
func TestLooseRejectsDeclaredBodyMismatch(t *testing.T) {
	for _, actual := range []string{"short", "far too much data"} {
		var encoded bytes.Buffer
		z := zlib.NewWriter(&encoded)
		_, _ = fmt.Fprintf(z, "blob 10%c%s", 0, actual)
		_ = z.Close()
		source := &closeProbe{Reader: bytes.NewReader(encoded.Bytes())}
		r, err := readLoose(source, config.SHA1, plumbing.BlobObject, 10, plumbing.ZeroHash)
		if err == nil {
			_, err = io.ReadAll(r)
			_ = r.Close()
		}
		if err == nil || !source.closed {
			t.Fatalf("accepted wrong length or leaked source: %v", err)
		}
	}
}

func TestInlineLooseObjects(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			data := []byte("small authenticated Git object")
			var encoded bytes.Buffer
			w := objfile.NewWriter(&encoded, format)
			if err := w.WriteHeader(plumbing.BlobObject, int64(len(data))); err != nil {
				t.Fatal(err)
			}
			if _, err := w.Write(data); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			original := objectMeta{ID: w.Hash().String(), Kind: plumbing.BlobObject, Size: int64(len(data)), Encoding: "zlib", StoredSize: int64(encoded.Len()), Inline: encoded.Bytes()}
			r, err := original.readInline(format)
			if err != nil {
				t.Fatal(err)
			}
			got, err := io.ReadAll(r)
			_ = r.Close()
			if err != nil || !bytes.Equal(got, data) {
				t.Fatalf("inline roundtrip: %q %v", got, err)
			}
			for name, mutate := range map[string]func(*objectMeta){
				"truncated":         func(m *objectMeta) { m.Inline = m.Inline[:len(m.Inline)-2]; m.StoredSize -= 2 },
				"checksum":          func(m *objectMeta) { m.Inline[len(m.Inline)-1] ^= 1 },
				"compressed length": func(m *objectMeta) { m.StoredSize++ },
				"decoded length":    func(m *objectMeta) { m.Size++ },
				"type":              func(m *objectMeta) { m.Kind = plumbing.TreeObject },
				"identity":          func(m *objectMeta) { m.ID = strings.Repeat("0", len(m.ID)) },
				"encoding":          func(m *objectMeta) { m.Encoding = "" },
				"oversized":         func(m *objectMeta) { m.Inline = make([]byte, inlineObjectLimit+1); m.StoredSize = int64(len(m.Inline)) },
			} {
				t.Run(name, func(t *testing.T) {
					m := original
					m.Inline = bytes.Clone(original.Inline)
					mutate(&m)
					r, err := m.readInline(format)
					if err == nil {
						_, err = io.ReadAll(r)
						_ = r.Close()
					}
					if err == nil {
						t.Fatal("invalid inline object accepted")
					}
				})
			}
		})
	}
}

func TestInlineMetadataBound(t *testing.T) {
	meta := objectMeta{ID: strings.Repeat("f", 64), Kind: plumbing.BlobObject, Size: math.MaxInt64, Encoding: "zlib", StoredSize: inlineObjectLimit, Inline: make([]byte, inlineObjectLimit)}
	if !meta.validInline() {
		t.Fatal("exact limit rejected")
	}
	meta.Inline = append(meta.Inline, 0)
	meta.StoredSize++
	if meta.validInline() {
		t.Fatal("oversized metadata accepted")
	}
	meta.Inline = meta.Inline[:inlineObjectLimit]
	meta.StoredSize--
	leaf := struct{ Items []objectMeta }{Items: make([]objectMeta, indexLeafSize)}
	for i := range leaf.Items {
		leaf.Items[i] = meta
	}
	data, err := json.Marshal(leaf)
	if err != nil {
		t.Fatal(err)
	}
	// Even worst-case SHA-256 IDs and numeric fields leave over 1 MiB of the
	// 2 MiB node budget for the WAL envelope. Inline entries have no child refs.
	if len(data) >= 1024*1024 {
		t.Fatalf("inline leaf metadata grew to %d bytes", len(data))
	}
}

func TestRepositoryRootAdmission(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			empty := rootMeta{Validated: true, Format: format, Refs: map[string]string{}, Buckets: []string{}}
			data, err := json.Marshal(empty)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodeRoot(data, format, 0); err != nil {
				t.Fatalf("rejected empty root: %v", err)
			}
			id := strings.Repeat("a", format.HexSize())
			valid := rootMeta{Validated: true, Format: format, Head: "refs/heads/main", Refs: map[string]string{"refs/heads/main": id}, Buckets: []string{"0a", "ab"}}
			invalid := map[string]rootMeta{
				"head":            {Validated: true, Format: format, Head: "refs/tags/main", Refs: valid.Refs, Buckets: valid.Buckets},
				"reference":       {Validated: true, Format: format, Head: valid.Head, Refs: map[string]string{"HEAD": id}, Buckets: valid.Buckets},
				"hash":            {Validated: true, Format: format, Head: valid.Head, Refs: map[string]string{"refs/heads/main": strings.ToUpper(id)}, Buckets: valid.Buckets},
				"zero hash":       {Validated: true, Format: format, Head: valid.Head, Refs: map[string]string{"refs/heads/main": strings.Repeat("0", format.HexSize())}, Buckets: valid.Buckets},
				"ref collision":   {Validated: true, Format: format, Head: valid.Head, Refs: map[string]string{"refs/heads/main": id, "refs/heads/main/nested": id}, Buckets: valid.Buckets},
				"bucket order":    {Validated: true, Format: format, Head: valid.Head, Refs: valid.Refs, Buckets: []string{"ab", "0a"}},
				"bucket encoding": {Validated: true, Format: format, Head: valid.Head, Refs: valid.Refs, Buckets: []string{"AZ"}},
			}
			data, err = json.Marshal(valid)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = decodeRoot(data, format, len(valid.Buckets)); err != nil {
				t.Fatalf("rejected valid root: %v", err)
			}
			for name, malformed := range map[string][]byte{
				"unknown field":  []byte(strings.Replace(string(data), `"Buckets":`, `"Unknown":true,"Buckets":`, 1)),
				"trailing value": append(append([]byte(nil), data...), []byte(`{}`)...),
			} {
				t.Run(name, func(t *testing.T) {
					if _, err := decodeRoot(malformed, format, len(valid.Buckets)); err == nil {
						t.Fatal("accepted malformed metadata")
					}
				})
			}
			for name, root := range invalid {
				t.Run(name, func(t *testing.T) {
					data, err := json.Marshal(root)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = decodeRoot(data, format, len(root.Buckets)); err == nil {
						t.Fatal("accepted invalid root")
					}
				})
			}
			for _, test := range []struct {
				name      string
				marker    any
				wantValid bool
			}{
				{"missing", nil, false},
				{"unchecked", false, false},
				{"validated", true, true},
			} {
				t.Run(test.name, func(t *testing.T) {
					root := map[string]any{"Format": format, "Buckets": []string{"ab"}}
					if test.marker != nil {
						root["Validated"] = test.marker
					}
					data, err := json.Marshal(root)
					if err != nil {
						t.Fatal(err)
					}
					_, err = decodeRoot(data, format, 1)
					if (err == nil) != test.wantValid {
						t.Fatalf("root admission: %v", err)
					}
					if _, err = decodeRoot(data, format, 0); err == nil {
						t.Fatal("accepted missing child")
					}
				})
			}
		})
	}
}

func TestCatalogMetadataAdmission(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			id := "ab" + strings.Repeat("1", format.HexSize()-2)
			item := objectMeta{ID: id, Kind: plumbing.BlobObject, Size: 1, Encoding: "zlib", StoredSize: 1}
			if !validObjectMeta(item, format, "ab") {
				t.Fatal("rejected valid object metadata")
			}
			for name, mutate := range map[string]func(*objectMeta){
				"path":      func(m *objectMeta) { m.ID = "ac" + m.ID[2:] },
				"hash":      func(m *objectMeta) { m.ID = strings.ToUpper(m.ID) },
				"zero hash": func(m *objectMeta) { m.ID = strings.Repeat("0", format.HexSize()) },
				"kind":      func(m *objectMeta) { m.Kind = plumbing.REFDeltaObject },
				"size":      func(m *objectMeta) { m.Size = -1 },
				"encoding":  func(m *objectMeta) { m.Encoding = "" },
				"delta base": func(m *objectMeta) {
					m.Delta = &deltaMeta{Base: strings.Repeat("0", format.HexSize()), Size: 2, Data: []byte{1}}
				},
			} {
				t.Run(name, func(t *testing.T) {
					invalid := item
					mutate(&invalid)
					if validObjectMeta(invalid, format, "ab") {
						t.Fatal("accepted invalid object metadata")
					}
				})
			}
			if !validPrefix("ab0", 3, "ab") || validPrefix("ac0", 3, "ab") || validPrefix("AB0", 3, "ab") {
				t.Fatal("catalog prefix validation differs from path")
			}
		})
	}
}
