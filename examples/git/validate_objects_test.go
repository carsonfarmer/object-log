package main

import (
	"errors"
	"io"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"
)

type validationStore struct {
	*memory.Storage
	reads      map[plumbing.Hash]int
	iterations int
	failure    error
}

type validationObject struct {
	plumbing.EncodedObject
	s *validationStore
}

func (o validationObject) Reader() (io.ReadCloser, error) {
	o.s.reads[o.Hash()]++
	return o.EncodedObject.Reader()
}

func (s *validationStore) EncodedObject(kind plumbing.ObjectType, id plumbing.Hash) (plumbing.EncodedObject, error) {
	if s.failure != nil {
		return nil, s.failure
	}
	o, err := s.Storage.EncodedObject(kind, id)
	if err != nil {
		return nil, err
	}
	return validationObject{o, s}, nil
}

func (s *validationStore) IterEncodedObjects(kind plumbing.ObjectType) (storer.EncodedObjectIter, error) {
	s.iterations++
	return s.Storage.IterEncodedObjects(kind)
}

func validationPut(t *testing.T, s *validationStore, value interface {
	Encode(plumbing.EncodedObject) error
}) plumbing.Hash {
	t.Helper()
	o := s.NewEncodedObject()
	if err := value.Encode(o); err != nil {
		t.Fatal(err)
	}
	id, err := s.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestIncrementalCatalogValidation(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			s := &validationStore{Storage: memory.NewStorage(memory.WithObjectFormat(format)), reads: map[plumbing.Hash]int{}}
			put := func(value interface {
				Encode(plumbing.EncodedObject) error
			}) plumbing.Hash {
				return validationPut(t, s, value)
			}
			tree := put(&object.Tree{})
			var tip plumbing.Hash
			for i := 0; i < 100; i++ {
				commit := &object.Commit{TreeHash: tree}
				if i > 0 {
					commit.ParentHashes = []plumbing.Hash{tip}
				}
				tip = put(commit)
			}
			next := put(&object.Commit{TreeHash: tree, ParentHashes: []plumbing.Hash{tip}, Message: "next"})
			if err := verifyObjects(s, []plumbing.Hash{next}); err != nil {
				t.Fatal(err)
			}
			if s.iterations != 0 || len(s.reads) != 1 || s.reads[next] != 1 {
				t.Fatalf("incremental check reread old history: iterations=%d reads=%v", s.iterations, s.reads)
			}
			failure := errors.New("expired view")
			s.failure = failure
			if err := verifyObjects(s, []plumbing.Hash{next}); !errors.Is(err, failure) {
				t.Fatalf("lost storage failure: %v", err)
			}
		})
	}
}

func TestCatalogValidationRejectsUnreachableMalformedObjects(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, name := range []string{"duplicate tree", "missing tree", "wrong parent kind", "wrong tree entry kind", "missing tag target", "wrong tag target kind"} {
			t.Run(format.String()+"/"+name, func(t *testing.T) {
				s := &validationStore{Storage: memory.NewStorage(memory.WithObjectFormat(format)), reads: map[plumbing.Hash]int{}}
				put := func(value interface {
					Encode(plumbing.EncodedObject) error
				}) plumbing.Hash {
					return validationPut(t, s, value)
				}
				tree := put(&object.Tree{})
				commit := put(&object.Commit{TreeHash: tree})
				var bad plumbing.Hash
				switch name {
				case "duplicate tree":
					o := s.NewEncodedObject()
					o.SetType(plumbing.TreeObject)
					w, _ := o.Writer()
					for i := 0; i < 2; i++ {
						_, _ = io.WriteString(w, "40000 duplicate\x00")
						_, _ = w.Write(tree.Bytes())
					}
					_ = w.Close()
					var err error
					bad, err = s.SetEncodedObject(o)
					if err != nil {
						t.Fatal(err)
					}
				case "missing tree":
					bad = put(&object.Commit{})
				case "wrong parent kind":
					bad = put(&object.Commit{TreeHash: tree, ParentHashes: []plumbing.Hash{tree}})
				case "wrong tree entry kind":
					bad = put(&object.Tree{Entries: []object.TreeEntry{{Name: "wrong", Mode: filemode.Regular, Hash: commit}}})
				case "missing tag target":
					bad = put(&object.Tag{Name: "bad", TargetType: plumbing.CommitObject})
				case "wrong tag target kind":
					bad = put(&object.Tag{Name: "bad", Target: tree, TargetType: plumbing.CommitObject})
				}
				if err := verifyObjects(s, []plumbing.Hash{bad}); err == nil {
					t.Fatal("accepted new malformed object outside ref history")
				}
			})
		}
	}
}
