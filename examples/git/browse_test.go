//go:build git_native_test

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage/memory"
)

func browseFixture(t *testing.T, format config.ObjectFormat) *store {
	t.Helper()
	s := &store{Storage: memory.NewStorage(memory.WithObjectFormat(format))}
	blob := func(text string) plumbing.Hash {
		o := s.NewEncodedObject()
		o.SetType(plumbing.BlobObject)
		w, _ := o.Writer()
		_, _ = w.Write([]byte(text))
		_ = w.Close()
		id, err := s.SetEncodedObject(o)
		if err != nil {
			t.Fatal(err)
		}
		return id
	}
	put := func(value interface {
		Encode(plumbing.EncodedObject) error
	}) plumbing.Hash {
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
	subtree := put(&object.Tree{Entries: []object.TreeEntry{
		{Name: "<hello>.txt", Mode: filemode.Regular, Hash: blob("<script>alert('hello')</script>\n")},
	}})
	tree := put(&object.Tree{Entries: []object.TreeEntry{
		{Name: "binary", Mode: filemode.Regular, Hash: blob("a\x00b")},
		{Name: "docs", Mode: filemode.Dir, Hash: subtree},
		{Name: "large", Mode: filemode.Regular, Hash: blob(strings.Repeat("x", browseFileBytes+1))},
		{Name: "link", Mode: filemode.Symlink, Hash: blob("docs/<hello>.txt")},
	}})
	var tip plumbing.Hash
	for i := range 10 {
		commit := &object.Commit{TreeHash: tree, Message: "Commit " + string(rune('A'+i)) + "\nDetails"}
		if i != 0 {
			commit.ParentHashes = []plumbing.Hash{tip}
		}
		tip = put(commit)
	}
	s.meta = rootMeta{Format: format, Head: "refs/heads/main", Refs: map[string]string{
		"refs/heads/main": tip.String(), "refs/tags/v1": tip.String(),
	}}
	return s
}

func TestBrowseSnapshotUsesBranchTreeAndBoundsHistory(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			s := browseFixture(t, format)
			view, err := browseSnapshot(s, s.meta, url.Values{})
			if err != nil {
				t.Fatal(err)
			}
			if len(view.Branches) != 1 || len(view.History) != 8 || view.History[0].Title != "Commit J" {
				t.Fatalf("unexpected branch/history: %+v", view)
			}
			if len(view.Entries) != 4 || len(view.History[0].ID) != format.HexSize() {
				t.Fatalf("wrong tree or hash width: %+v", view)
			}
			for _, ref := range []string{"refs/tags/v1", view.History[0].ID, "refs/heads/missing"} {
				_, err := browseSnapshot(s, s.meta, url.Values{"ref": {ref}})
				if !errors.Is(err, errBrowseNotFound) {
					t.Fatalf("unselectable ref %q: %v", ref, err)
				}
			}
			for _, path := range []string{"missing", "binary/child"} {
				_, err := browseSnapshot(s, s.meta, url.Values{"path": {path}})
				if !errors.Is(err, errBrowseNotFound) {
					t.Fatalf("missing path %q: %v", path, err)
				}
			}
		})
	}
}

func TestBrowseTextBinaryLargeAndSymlink(t *testing.T) {
	s := browseFixture(t, config.SHA256)
	for _, test := range []struct{ path, state, text string }{
		{path: "docs/<hello>.txt", state: "text", text: "<script>alert('hello')</script>\n"},
		{path: "binary", state: "binary"},
		{path: "large", state: "large"},
		{path: "link", state: "text", text: "docs/<hello>.txt"},
	} {
		view, err := browseSnapshot(s, s.meta, url.Values{"path": {test.path}})
		if err != nil || view.File == nil || view.File.State != test.state || view.File.Text != test.text {
			t.Fatalf("path %q: view=%+v err=%v", test.path, view, err)
		}
	}
}

func TestBrowseRejectsAmbiguousQueriesAndUsesReadPermissions(t *testing.T) {
	s := browseFixture(t, config.SHA1)
	for _, query := range []string{"ref=a&ref=b", "path=../secret", "path=a//b", "path=/etc", "path=a/.", "path=%ZZ", "other=x"} {
		w := httptest.NewRecorder()
		err := serveBrowse(w, httptest.NewRequest(http.MethodGet, "/team/alpha.git/_browse?"+query, nil), s)
		if err != nil || w.Code != http.StatusBadRequest {
			t.Fatalf("query %q: status=%d err=%v", query, w.Code, err)
		}
	}
	route, err := resolveRepository(repositoriesForTest(t), httptest.NewRequest(http.MethodGet, "/team/alpha.git/_browse?path=docs", nil))
	if err != nil || route.Action != gitRead || route.Method != http.MethodGet {
		t.Fatalf("wrong authorization route: %+v %v", route, err)
	}
	_, err = resolveRepository(repositoriesForTest(t), httptest.NewRequest(http.MethodPost, "/team/alpha.git/_browse", nil))
	if !errors.Is(err, errRepositoryMethod) {
		t.Fatalf("browse accepted a mutation method: %v", err)
	}
	w := httptest.NewRecorder()
	if err := serveBrowse(w, httptest.NewRequest(http.MethodGet, "/team/alpha.git/_browse", nil), s); err != nil {
		t.Fatal(err)
	}
	var view browseView
	if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("invalid uncached JSON: %v", err)
	}
}

func TestBrowseEscapedNamesAndDirectoryLimit(t *testing.T) {
	s := browseFixture(t, config.SHA256)
	commit, err := object.GetCommit(s, plumbing.NewHash(s.meta.Refs[s.meta.Head]))
	if err != nil {
		t.Fatal(err)
	}
	tree, err := object.GetTree(s, commit.TreeHash)
	if err != nil {
		t.Fatal(err)
	}
	blob := tree.Entries[0].Hash
	name := "space #?.café\\file"
	entries := []object.TreeEntry{}
	for i := range 501 {
		entries = append(entries, object.TreeEntry{Name: fmt.Sprintf("file%04d", i), Mode: filemode.Regular, Hash: blob})
	}
	entries = append(entries, object.TreeEntry{Name: name, Mode: filemode.Regular, Hash: blob})
	o := s.NewEncodedObject()
	if err := (&object.Tree{Entries: entries}).Encode(o); err != nil {
		t.Fatal(err)
	}
	treeID, err := s.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	o = s.NewEncodedObject()
	if err := (&object.Commit{TreeHash: treeID}).Encode(o); err != nil {
		t.Fatal(err)
	}
	tip, err := s.SetEncodedObject(o)
	if err != nil {
		t.Fatal(err)
	}
	ref := "refs/heads/feature/100%"
	s.meta.Refs[ref] = tip.String()
	view, err := browseSnapshot(s, s.meta, url.Values{"ref": {ref}})
	if err != nil || !view.More || len(view.Entries) != 500 {
		t.Fatalf("directory bound: view=%+v err=%v", view, err)
	}
	query := url.Values{"ref": {ref}, "path": {name}}
	w := httptest.NewRecorder()
	if err := serveBrowse(w, httptest.NewRequest(http.MethodGet, "/showcase.git/_browse?"+query.Encode(), nil), s); err != nil {
		t.Fatal(err)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("escaped path did not round trip: status=%d body=%s", w.Code, w.Body)
	}
}

type failingBrowseObjects struct{ storer.EncodedObjectStorer }

func (failingBrowseObjects) EncodedObject(plumbing.ObjectType, plumbing.Hash) (plumbing.EncodedObject, error) {
	return nil, errExpired
}

func TestBrowseStorageFailuresRemainRetryable(t *testing.T) {
	s := browseFixture(t, config.SHA1)
	_, err := browseSnapshot(failingBrowseObjects{EncodedObjectStorer: s}, s.meta, url.Values{})
	if !errors.Is(err, errExpired) {
		t.Fatalf("storage failure lost: %v", err)
	}
}
