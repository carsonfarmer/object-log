package main

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/storage/memory"
)

type policyStore struct {
	*memory.Storage
	reads   int
	failure error
	kinds   map[plumbing.ObjectType]int
}

func (s *policyStore) EncodedObject(kind plumbing.ObjectType, id plumbing.Hash) (plumbing.EncodedObject, error) {
	s.reads++
	if s.failure != nil {
		return nil, s.failure
	}
	o, err := s.Storage.EncodedObject(kind, id)
	if err == nil {
		s.kinds[o.Type()]++
	}
	return o, err
}

func policyFixture(t *testing.T, format config.ObjectFormat) (*policyStore, map[string]plumbing.Hash) {
	t.Helper()
	s := &policyStore{Storage: memory.NewStorage(memory.WithObjectFormat(format)), kinds: map[plumbing.ObjectType]int{}}
	put := func(value interface {
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
	o := s.NewEncodedObject()
	o.SetType(plumbing.BlobObject)
	w, _ := o.Writer()
	_, _ = io.WriteString(w, "visible contents")
	_ = w.Close()
	blob, err := s.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	tree := put(&object.Tree{Entries: []object.TreeEntry{{Name: "file", Mode: filemode.Regular, Hash: blob}}})
	base := put(&object.Commit{TreeHash: tree, Message: "base"})
	tip := put(&object.Commit{TreeHash: tree, ParentHashes: []plumbing.Hash{base}, Message: "tip"})
	dead := put(&object.Commit{TreeHash: tree, Message: "unpublished"})
	tag := put(&object.Tag{Name: "release", Target: tip, TargetType: plumbing.CommitObject})
	outer := put(&object.Tag{Name: "outer", Target: tag, TargetType: plumbing.TagObject})
	missing := plumbing.NewHash("1111111111111111111111111111111111111111")
	return s, map[string]plumbing.Hash{"blob": blob, "tree": tree, "base": base, "tip": tip, "dead": dead, "tag": tag, "outer": outer, "missing": missing}
}

func TestVisibleFetch(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			s, ids := policyFixture(t, format)
			tips := []plumbing.Hash{ids["tip"]}
			if _, err := visibleFetch(s, tips, tips, nil); err != nil || s.reads != 0 {
				t.Fatalf("tip lookup performed reads: %d, %v", s.reads, err)
			}
			haves, err := visibleFetch(s, tips, tips, []plumbing.Hash{ids["base"], ids["dead"], ids["missing"]})
			if err != nil || !reflect.DeepEqual(haves, []plumbing.Hash{ids["base"]}) {
				t.Fatalf("haves: %v, %v", haves, err)
			}
			if s.kinds[plumbing.TreeObject] != 0 || s.kinds[plumbing.BlobObject] != 0 {
				t.Fatal("commit negotiation read trees or blobs")
			}
			for _, name := range []string{"base", "tree", "blob"} {
				if _, err := visibleFetch(s, tips, []plumbing.Hash{ids[name]}, nil); err != nil {
					t.Fatalf("reachable %s: %v", name, err)
				}
			}
			for _, name := range []string{"dead", "missing", "tag"} {
				if _, err := visibleFetch(s, tips, []plumbing.Hash{ids[name]}, nil); err == nil {
					t.Fatalf("accepted unreachable %s", name)
				}
			}
			for _, name := range []string{"tag", "tip", "base", "tree", "blob"} {
				if _, err := visibleFetch(s, []plumbing.Hash{ids["outer"]}, []plumbing.Hash{ids[name]}, nil); err != nil {
					t.Fatalf("tag reachability %s: %v", name, err)
				}
			}
			failure := errors.New("expired view")
			s.failure = failure
			if _, err := visibleFetch(s, tips, tips, []plumbing.Hash{ids["base"]}); !errors.Is(err, failure) {
				t.Fatalf("lost storage error: %v", err)
			}
		})
	}
}

func TestFilterFetchCodecs(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		for _, v2 := range []bool{false, true} {
			s, ids := policyFixture(t, format)
			wants := []plumbing.Hash{ids["tip"]}
			haves := []plumbing.Hash{ids["base"], ids["dead"]}
			var input bytes.Buffer
			if v2 {
				request := packp.CommandRequest{Command: "fetch", Args: &packp.FetchArgs{Wants: wants, Haves: haves, Done: true, NoProgress: true, OFSDelta: true}}
				if err := request.Encode(&input); err != nil {
					t.Fatal(err)
				}
			} else {
				request := packp.UploadRequest{Wants: wants}
				if err := request.Encode(&input); err != nil {
					t.Fatal(err)
				}
				requestHaves := packp.UploadHaves{Haves: haves, Done: true}
				if err := requestHaves.Encode(&input); err != nil {
					t.Fatal(err)
				}
			}
			filtered, err := filterFetch(s, wants, bytes.NewReader(input.Bytes()), v2)
			if err != nil {
				t.Fatal(err)
			}
			var gotWants, gotHaves []plumbing.Hash
			var done bool
			if v2 {
				var args packp.FetchArgs
				request := packp.CommandRequest{Args: &args}
				if err := request.Decode(filtered); err != nil {
					t.Fatal(err)
				}
				gotWants, gotHaves, done = args.Wants, args.Haves, args.Done
				if !args.OFSDelta || !args.NoProgress {
					t.Fatal("lost fetch options")
				}
			} else {
				reader := filtered
				var request packp.UploadRequest
				var requestHaves packp.UploadHaves
				if err := request.Decode(reader); err != nil {
					t.Fatal(err)
				}
				if err := requestHaves.Decode(reader); err != nil {
					t.Fatal(err)
				}
				gotWants, gotHaves, done = request.Wants, requestHaves.Haves, requestHaves.Done
			}
			if !reflect.DeepEqual(gotWants, wants) || !reflect.DeepEqual(gotHaves, []plumbing.Hash{ids["base"]}) || !done {
				t.Fatalf("roundtrip v2=%v: %v %v %v", v2, gotWants, gotHaves, done)
			}
			if _, err := filterFetch(s, nil, bytes.NewReader(input.Bytes()), v2); err == nil {
				t.Fatal("accepted wants without published refs")
			}
		}
	}
}

func TestFilterFetchPreservesOtherCommands(t *testing.T) {
	var input bytes.Buffer
	request := packp.CommandRequest{Command: "ls-refs", Args: &packp.LsRefsArgs{Peel: true, Symrefs: true}}
	if err := request.Encode(&input); err != nil {
		t.Fatal(err)
	}
	got, err := filterFetch(nil, nil, bytes.NewReader(input.Bytes()), true)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := io.ReadAll(got)
	if err != nil || !bytes.Equal(encoded, input.Bytes()) {
		t.Fatalf("ls-refs changed: %v", err)
	}
}
