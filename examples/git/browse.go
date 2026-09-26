package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

const browseFileBytes = 256 << 10

var errBrowseNotFound = errors.New("branch or path not found")

type browseView struct {
	Branch   string         `json:"branch"`
	Branches []string       `json:"branches"`
	Path     string         `json:"path"`
	History  []browseCommit `json:"history"`
	Entries  []browseEntry  `json:"entries"`
	More     bool           `json:"more"`
	File     *browseFile    `json:"file,omitempty"`
}

type browseCommit struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Author string `json:"author"`
	Date   string `json:"date"`
}

type browseEntry struct {
	Name string `json:"name"`
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type browseFile struct {
	ID    string `json:"id"`
	Size  int64  `json:"size"`
	State string `json:"state"`
	Text  string `json:"text,omitempty"`
}

func serveBrowse(w http.ResponseWriter, r *http.Request, s *store) error {
	query, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil || !validBrowseQuery(query) {
		http.Error(w, "invalid browse query", http.StatusBadRequest)
		return nil
	}
	view, err := browseSnapshot(s, s.meta, query)
	if errors.Is(err, errBrowseNotFound) {
		http.NotFound(w, r)
		return nil
	}
	if err != nil {
		return err
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	return json.NewEncoder(w).Encode(view)
}

func validBrowseQuery(query url.Values) bool {
	for name, values := range query {
		if (name != "ref" && name != "path") || len(values) != 1 || len(values[0]) > 4096 {
			return false
		}
	}
	path := query.Get("path")
	if path == "" {
		return true
	}
	parts := strings.Split(path, "/")
	if len(parts) > 64 || !utf8.ValidString(path) {
		return false
	}
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsRune(part, 0) {
			return false
		}
	}
	return true
}

// Only configured branch tips are selectable. Object IDs cannot expose objects
// left behind by deleted refs, and paths are traversed within that tip's tree.
func browseSnapshot(s storer.EncodedObjectStorer, meta rootMeta, query url.Values) (*browseView, error) {
	view := &browseView{
		Branch: query.Get("ref"), Path: query.Get("path"),
		Branches: []string{}, History: []browseCommit{}, Entries: []browseEntry{},
	}
	for ref := range meta.Refs {
		if strings.HasPrefix(ref, "refs/heads/") {
			view.Branches = append(view.Branches, ref)
		}
	}
	slices.Sort(view.Branches)
	if view.Branch == "" {
		view.Branch = meta.Head
		if _, exists := meta.Refs[view.Branch]; !exists && len(view.Branches) > 0 {
			view.Branch = view.Branches[0]
		}
	}
	if len(view.Branches) == 0 && query.Get("ref") == "" && view.Path == "" {
		return view, nil
	}
	id, exists := meta.Refs[view.Branch]
	if !exists || !slices.Contains(view.Branches, view.Branch) {
		return nil, errBrowseNotFound
	}
	commit, err := object.GetCommit(s, plumbing.NewHash(id))
	if err != nil {
		return nil, err
	}
	current := commit
	for i := range 8 {
		title := strings.SplitN(current.Message, "\n", 2)[0]
		view.History = append(view.History, browseCommit{
			ID: current.Hash.String(), Title: strings.ToValidUTF8(title[:min(len(title), 512)], "�"),
			Author: current.Author.Name[:min(len(current.Author.Name), 512)], Date: current.Author.When.Format("2006-01-02T15:04:05Z07:00"),
		})
		if len(current.ParentHashes) == 0 || i == 7 {
			break
		}
		current, err = object.GetCommit(s, current.ParentHashes[0])
		if err != nil {
			return nil, err
		}
	}
	tree, err := object.GetTree(s, commit.TreeHash)
	if err != nil {
		return nil, err
	}
	if view.Path != "" {
		parts := strings.Split(view.Path, "/")
		for i, part := range parts {
			index := slices.IndexFunc(tree.Entries, func(e object.TreeEntry) bool { return e.Name == part })
			if index < 0 {
				return nil, errBrowseNotFound
			}
			entry := tree.Entries[index]
			if entry.Mode != filemode.Dir {
				if i != len(parts)-1 {
					return nil, errBrowseNotFound
				}
				view.File, err = browseBlob(s, entry)
				return view, err
			}
			tree, err = object.GetTree(s, entry.Hash)
			if err != nil {
				return nil, err
			}
		}
	}
	view.More = len(tree.Entries) > 500
	for _, entry := range tree.Entries[:min(len(tree.Entries), 500)] {
		view.Entries = append(view.Entries, browseEntry{Name: entry.Name, Kind: browseKind(entry.Mode), ID: entry.Hash.String()})
	}
	return view, nil
}

func browseKind(mode filemode.FileMode) string {
	switch mode {
	case filemode.Dir:
		return "directory"
	case filemode.Submodule:
		return "submodule"
	default:
		return "file"
	}
}

func browseBlob(s storer.EncodedObjectStorer, entry object.TreeEntry) (*browseFile, error) {
	file := &browseFile{ID: entry.Hash.String(), State: "submodule"}
	if entry.Mode == filemode.Submodule {
		return file, nil
	}
	blob, err := object.GetBlob(s, entry.Hash)
	if err != nil {
		return nil, err
	}
	file.Size = blob.Size
	file.State = "large"
	if blob.Size > browseFileBytes {
		return file, nil
	}
	r, err := blob.Reader()
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(io.LimitReader(r, browseFileBytes+1))
	err = errors.Join(err, r.Close())
	if err != nil {
		return nil, err
	}
	if len(data) > browseFileBytes {
		return file, nil
	}
	file.State = "binary"
	if utf8.Valid(data) && !bytes.ContainsRune(data, 0) {
		file.State = "text"
		file.Text = string(data)
	}
	return file, nil
}
