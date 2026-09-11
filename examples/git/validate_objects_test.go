package main

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

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
			signature := object.Signature{When: time.Unix(0, 0)}
			var tip plumbing.Hash
			for i := 0; i < 100; i++ {
				commit := &object.Commit{TreeHash: tree, Author: signature, Committer: signature}
				if i > 0 {
					commit.ParentHashes = []plumbing.Hash{tip}
				}
				tip = put(commit)
			}
			next := put(&object.Commit{TreeHash: tree, ParentHashes: []plumbing.Hash{tip}, Message: "next", Author: signature, Committer: signature})
			if err := verifyObjects(s, []plumbing.Hash{next}); err != nil {
				t.Fatal(err)
			}
			if s.iterations != 0 || len(s.reads) != 1 || s.reads[next] != 2 {
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

func validationGit(t *testing.T, format config.ObjectFormat) func(string, ...string) (string, error) {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("native Git oracle unavailable")
	}
	dir := t.TempDir()
	git := func(input string, args ...string) (string, error) {
		cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
		cmd.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
		cmd.Stdin = strings.NewReader(input)
		out, err := cmd.CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	if out, err := git("", "init", "--bare", "--object-format="+format.String()); err != nil {
		t.Fatalf("init: %s: %v", out, err)
	}
	return git
}

func validationRaw(t *testing.T, s *validationStore, kind plumbing.ObjectType, data string) plumbing.Hash {
	t.Helper()
	o := s.NewEncodedObject()
	o.SetType(kind)
	w, err := o.Writer()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(w, data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	id, err := s.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestCommitIdentityValidationMatchesGit(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, tc := range []struct {
			name, identity string
			valid          bool
		}{
			{"ordinary", "A <a@b> 1 +0000", true},
			{"empty fields and epoch", " <> 0 +0000", true},
			{"extra spaces", "A <>   1 +0000", true},
			{"tab before date", "A <> \t1 +0000", true},
			{"unusual timezone", "A <> 1 +9999", true},
			{"missing email", "invalid", false},
			{"missing name space", "A<a@b> 1 +0000", false},
			{"missing date space", "A <a@b>1 +0000", false},
			{"carriage return date", "A <> \r1 +0000", false},
			{"vertical tab date", "A <> \v1 +0000", false},
			{"form feed date", "A <> \f1 +0000", false},
			{"bad timezone", "A <> 1 blah", false},
			{"negative date", "A <> -1 +0000", false},
			{"padded date", "A <> 01 +0000", false},
			{"maximum date", "A <> 9223372036854775807 +0000", true},
			{"signed date overflow", "A <> 9223372036854775808 +0000", false},
			{"date overflow", "A <> 18446744073709551616 +0000", false},
			{"missing both", "", false},
			{"bad committer", "", false},
			{"missing author", "", false},
			{"missing committer", "", false},
			{"duplicate author", "", false},
			{"extensions", "A <> 1 +0000", true},
		} {
			t.Run(format.String()+"/"+tc.name, func(t *testing.T) {
				git := validationGit(t, format)
				s := &validationStore{Storage: memory.NewStorage(memory.WithObjectFormat(format)), reads: map[plumbing.Hash]int{}}
				tree := validationPut(t, s, &object.Tree{})
				if out, err := git("", "mktree"); err != nil || out != tree.String() {
					t.Fatalf("tree: %s: %v", out, err)
				}
				headers := "author " + tc.identity + "\ncommitter " + tc.identity + "\n"
				switch tc.name {
				case "missing both":
					headers = ""
				case "bad committer":
					headers = "author A <> 1 +0000\ncommitter invalid\n"
				case "missing author":
					headers = "committer A <> 1 +0000\n"
				case "missing committer":
					headers = "author A <> 1 +0000\n"
				case "duplicate author":
					headers = "author A <> 1 +0000\nauthor A <> 1 +0000\ncommitter A <> 1 +0000\n"
				case "extensions":
					headers += "encoding UTF-8\ngpgsig signature\n continuation\nx-custom arbitrary\n"
				}
				data := "tree " + tree.String() + "\n" + headers + "\nmessage\n"
				id := validationRaw(t, s, plumbing.CommitObject, data)
				if err := verifyObjects(s, []plumbing.Hash{id}); (err == nil) != tc.valid {
					t.Fatalf("validation: %v; want valid=%v", err, tc.valid)
				}
				if out, err := git(data, "hash-object", "-t", "commit", "-w", "--stdin", "--literally"); err != nil || out != id.String() {
					t.Fatalf("hash: %s: %v", out, err)
				}
				if out, err := git("", "fsck", "--full", id.String()); (err == nil) != tc.valid {
					t.Fatalf("native Git: %s: %v; want valid=%v", out, err, tc.valid)
				}
			})
		}
	}
}

func TestTagHeaderValidationMatchesGit(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, tc := range []struct {
			name, headers string
			valid         bool
		}{
			{"ordinary", "type blob\ntag x\ntagger A <> 1 +0000\n", true},
			{"historical missing tagger", "type blob\ntag x\n", true},
			{"empty tag name warning", "type blob\ntag \n", true},
			{"invalid tag name warning", "type blob\ntag ..\n", true},
			{"empty identity", "type blob\ntag x\ntagger  <> 0 +0000\n", true},
			{"malformed tagger", "type blob\ntag x\ntagger invalid\n", false},
			{"bad tagger timestamp", "type blob\ntag x\ntagger A <> nope +0000\n", false},
			{"missing type", "tag x\n", false},
			{"missing tag", "type blob\n", false},
		} {
			t.Run(format.String()+"/"+tc.name, func(t *testing.T) {
				git := validationGit(t, format)
				s := &validationStore{Storage: memory.NewStorage(memory.WithObjectFormat(format)), reads: map[plumbing.Hash]int{}}
				blob := validationRaw(t, s, plumbing.BlobObject, "")
				if out, err := git("", "hash-object", "-w", "--stdin"); err != nil || out != blob.String() {
					t.Fatalf("blob: %s: %v", out, err)
				}
				data := "object " + blob.String() + "\n" + tc.headers + "\nmessage\n"
				id := validationRaw(t, s, plumbing.TagObject, data)
				if err := verifyObjects(s, []plumbing.Hash{id}); (err == nil) != tc.valid {
					t.Fatalf("validation: %v; want valid=%v", err, tc.valid)
				}
				if out, err := git(data, "hash-object", "-t", "tag", "-w", "--stdin", "--literally"); err != nil || out != id.String() {
					t.Fatalf("tag: %s: %v", out, err)
				}
				if out, err := git("", "fsck", "--full", id.String()); (err == nil) != tc.valid {
					t.Fatalf("native Git: %s: %v; want valid=%v", out, err, tc.valid)
				}
			})
		}
	}
}
