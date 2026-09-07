package main

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/objfile"
	"io"
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
