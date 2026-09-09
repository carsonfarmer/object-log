package main

import (
	"bytes"
	"compress/zlib"
	"encoding/json"
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
	closed bool
}

func (r *closeProbe) Close() error { r.closed = true; return nil }

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
				got, err := io.ReadAll(r)
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("roundtrip: %v", err)
				}
				if err = r.Close(); err != nil || !source.closed {
					t.Fatal("source not closed")
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
						_, err = io.ReadAll(r)
						_ = r.Close()
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
