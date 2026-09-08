package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// filterFetch checks against the same published refs and object view used by
// upload-pack. Ref-tip requests need no object reads; ordinary haves only walk
// commits. Explicit noncommit wants may require the library's full graph walk.
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
	commits := map[plumbing.Hash]bool{}
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
			if o.Type() == plumbing.CommitObject {
				commits[id] = true
			} else if group == 0 {
				fullWalk = true
			}
		}
	}
	if fullWalk {
		ids, err := revlist.Objects(s, tips, nil)
		if err != nil {
			return nil, err
		}
		for _, id := range ids {
			visible[id] = true
		}
	} else {
		seen := map[plumbing.Hash]bool{}
		for _, tip := range tips {
			if len(commits) == 0 {
				break
			}
			o, err := object.GetObject(s, tip)
			for err == nil {
				tag, ok := o.(*object.Tag)
				if !ok {
					break
				}
				o, err = tag.Object()
			}
			if err != nil {
				return nil, err
			}
			if commit, ok := o.(*object.Commit); ok {
				iter := object.NewCommitPreorderIter(commit, seen, nil)
				err = iter.ForEach(func(c *object.Commit) error {
					seen[c.Hash], visible[c.Hash] = true, true
					delete(commits, c.Hash)
					if len(commits) == 0 {
						return storer.ErrStop
					}
					return nil
				})
				iter.Close()
				if err != nil {
					return nil, err
				}
			}
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
