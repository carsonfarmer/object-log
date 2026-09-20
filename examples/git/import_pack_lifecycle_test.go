package main

import (
	"bytes"
	"context"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	format "github.com/go-git/go-git/v6/plumbing/format/config"
)

func TestImportPackDuplicateIDsWithREFAndOFSChildren(t *testing.T) {
	for _, f := range []format.ObjectFormat{format.SHA1, format.SHA256} {
		t.Run(string(f), func(t *testing.T) {
			base := []byte("abc")
			a := blobID(f, []byte("a"))
			entries := []packFixtureEntry{
				{kind: plumbing.BlobObject, data: base},
				{kind: plumbing.REFDeltaObject, data: []byte{3, 1, 0x90, 1}, ref: blobID(f, base)},
				{kind: plumbing.OFSDeltaObject, data: []byte{1, 1, 0x90, 1}, ofs: 1},
				{kind: plumbing.REFDeltaObject, data: []byte{1, 1, 1, 'b'}, ref: a},
				{kind: plumbing.OFSDeltaObject, data: []byte{1, 1, 1, 'c'}, ofs: 2},
			}
			s := newImportStorage(f)
			if err := importPack(context.Background(), bytes.NewReader(fixturePack(t, f, entries)), s, f, testPackLimits(1024)); err != nil {
				t.Fatal(err)
			}
			for _, value := range []string{"abc", "a", "b", "c"} {
				if _, err := s.EncodedObject(plumbing.BlobObject, blobID(f, []byte(value))); err != nil {
					t.Fatalf("missing %q: %v", value, err)
				}
			}
		})
	}
}
