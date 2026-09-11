package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// filterFetch checks against the same published refs and object view used by
// upload-pack. Ref-tip requests need no object reads; ordinary haves only walk
// commits. Explicit noncommit wants also inspect trees until visibility is proven.
func filterFetch(s storer.EncodedObjectStorer, tips []plumbing.Hash, body io.Reader, v2 bool) (io.Reader, error) {
	var output bytes.Buffer
	if v2 {
		var command packp.CommandRequest
		args := fetchCommandArgs{command: &command}
		command.Args = &args
		if err := command.Decode(body); err != nil {
			return nil, err
		}
		if command.Command == "fetch" {
			haves, err := visibleFetch(s, tips, args.Wants, args.Haves)
			if err != nil {
				return nil, err
			}
			args.Haves = haves
		}
		if err := command.Encode(&output); err != nil {
			return nil, err
		}
	} else {
		reader := body
		var request packp.UploadRequest
		var haves packp.UploadHaves
		if err := request.Decode(reader); err != nil {
			return nil, err
		}
		if err := haves.Decode(reader); err != nil {
			return nil, err
		}
		var err error
		haves.Haves, err = visibleFetch(s, tips, request.Wants, haves.Haves)
		if err != nil {
			return nil, err
		}
		if err = request.Encode(&output); err != nil {
			return nil, err
		}
		if err = haves.Encode(&output); err != nil {
			return nil, err
		}
	}
	return &output, nil
}

// CommandRequest decodes the header before calling its argument decoder.
// Both supported commands use the library codecs, without retaining input bytes.
type fetchCommandArgs struct {
	packp.FetchArgs
	command *packp.CommandRequest
	refs    packp.LsRefsArgs
}

func (a *fetchCommandArgs) Decode(r io.Reader) error {
	if a.command.Command == "fetch" {
		return a.FetchArgs.Decode(r)
	}
	if a.command.Command == "ls-refs" {
		return a.refs.Decode(r)
	}
	return nil
}

func (a *fetchCommandArgs) Encode(w io.Writer) error {
	if a.command.Command == "fetch" {
		return a.FetchArgs.Encode(w)
	}
	if a.command.Command == "ls-refs" {
		return a.refs.Encode(w)
	}
	return nil
}

func visibleFetch(s storer.EncodedObjectStorer, tips, wants, haves []plumbing.Hash) ([]plumbing.Hash, error) {
	visible := make(map[plumbing.Hash]bool, len(tips))
	for _, tip := range tips {
		visible[tip] = true
	}
	needed := map[plumbing.Hash]bool{}
	fullWalk := false
	for group, ids := range [][]plumbing.Hash{wants, haves} {
		for _, id := range ids {
			if visible[id] {
				continue
			}
			o, err := s.EncodedObject(plumbing.AnyObject, id)
			if errors.Is(err, plumbing.ErrObjectNotFound) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if group == 0 && o.Type() != plumbing.CommitObject {
				fullWalk = true
			}
			if group == 0 || fullWalk || o.Type() == plumbing.CommitObject {
				needed[id] = true
			}
		}
	}
	seen := map[plumbing.Hash]bool{}
	queue := append([]plumbing.Hash(nil), tips...)
	var parents []plumbing.Hash
	for len(queue) > 0 && len(needed) > 0 {
		id := queue[0]
		queue = queue[1:]
		if !seen[id] {
			seen[id] = true
			o, err := s.EncodedObject(plumbing.AnyObject, id)
			if err != nil {
				return nil, err
			}
			if fullWalk || o.Type() == plumbing.CommitObject {
				visible[id] = true
				delete(needed, id)
			}
			if len(needed) == 0 {
				break
			}
			switch o.Type() {
			case plumbing.CommitObject:
				commit, err := object.DecodeCommit(s, o)
				if err != nil {
					return nil, err
				}
				parents = append(parents, commit.ParentHashes...)
				if fullWalk {
					queue = append(queue, commit.TreeHash)
				}
			case plumbing.TagObject:
				tag, err := object.DecodeTag(s, o)
				if err != nil {
					return nil, err
				}
				queue = append(queue, tag.Target)
			case plumbing.TreeObject:
				if fullWalk {
					tree, err := object.DecodeTree(s, o)
					if err != nil {
						return nil, err
					}
					for _, entry := range tree.Entries {
						if entry.Mode == filemode.Submodule {
							continue
						}
						visible[entry.Hash] = true
						delete(needed, entry.Hash)
						if entry.Mode == filemode.Dir {
							queue = append(queue, entry.Hash)
						}
					}
				}
			}
		}
		// Inspect every tip's trees before reading an older generation of commits.
		if len(queue) == 0 {
			queue, parents = parents, nil
		}
	}
	for _, id := range wants {
		if !visible[id] {
			return nil, fmt.Errorf("requested object is not reachable from published refs")
		}
	}
	valid := make([]plumbing.Hash, 0, len(haves))
	for _, id := range haves {
		if visible[id] {
			valid = append(valid, id)
		}
	}
	return valid, nil
}
