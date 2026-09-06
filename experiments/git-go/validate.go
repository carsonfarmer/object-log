package main

import (
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	"github.com/go-git/go-git/v6/storage"
	"strings"
)

func validate(st *store, cmds []*packp.Command) (map[string]string, error) {
	refs := map[string]string{}
	for name, id := range st.meta.Refs {
		refs[name] = id
	}
	seen := map[string]bool{}
	var tips []plumbing.Hash
	for _, cmd := range cmds {
		name := string(cmd.Name)
		if seen[name] || !strings.HasPrefix(name, "refs/") || (cmd.Old.IsZero() && cmd.New.IsZero()) {
			return nil, fmt.Errorf("invalid ref update")
		}
		seen[name] = true
		if e := cmd.Name.Validate(); e != nil {
			return nil, e
		}
		old, exists := st.meta.Refs[name]
		if (cmd.Old.IsZero() && exists) || (!cmd.Old.IsZero() && old != cmd.Old.String()) {
			return nil, fmt.Errorf("stale ref")
		}
		if cmd.New.IsZero() {
			delete(refs, name)
			continue
		}
		if cmd.Name.IsBranch() {
			next, e := object.GetCommit(st, cmd.New)
			if e != nil {
				return nil, e
			}
			if exists {
				previous, e := object.GetCommit(st, cmd.Old)
				if e != nil {
					return nil, e
				}
				ok, e := previous.IsAncestor(next)
				if e != nil {
					return nil, e
				}
				if !ok {
					return nil, fmt.Errorf("non-fast-forward")
				}
			}
		}
		refs[name] = cmd.New.String()
		tips = append(tips, cmd.New)
	}
	for name := range refs {
		for parent := name; ; {
			i := strings.LastIndexByte(parent, '/')
			if i < 0 {
				break
			}
			parent = parent[:i]
			if _, exists := refs[parent]; exists {
				return nil, fmt.Errorf("ref prefix collision")
			}
		}
	}
	return refs, verifyObjects(st, tips)
}

// Upstream revlist selects objects; it is not a connectivity/kind validator.
func verifyObjects(st storage.Storer, tips []plumbing.Hash) error {
	ids, err := revlist.Objects(st, tips, nil)
	if err != nil {
		return err
	}
	check := func(id plumbing.Hash, kind plumbing.ObjectType) error {
		_, err := st.EncodedObject(kind, id)
		return err
	}
	for _, id := range ids {
		o, err := st.EncodedObject(plumbing.AnyObject, id)
		if err != nil {
			return err
		}
		switch o.Type() {
		case plumbing.CommitObject:
			c, e := object.DecodeCommit(st, o)
			if e != nil {
				return e
			}
			if e = check(c.TreeHash, plumbing.TreeObject); e != nil {
				return e
			}
			for _, p := range c.ParentHashes {
				if e = check(p, plumbing.CommitObject); e != nil {
					return e
				}
			}
		case plumbing.TreeObject:
			tree, e := object.DecodeTree(st, o)
			if e != nil {
				return e
			}
			for _, entry := range tree.Entries {
				kind := plumbing.BlobObject
				switch entry.Mode {
				case filemode.Submodule:
					continue
				case filemode.Dir:
					kind = plumbing.TreeObject
				case filemode.Regular, filemode.Deprecated, filemode.Executable, filemode.Symlink:
				default:
					return fmt.Errorf("invalid tree mode")
				}
				if e = check(entry.Hash, kind); e != nil {
					return e
				}
			}
		case plumbing.TagObject:
			tag, e := object.DecodeTag(st, o)
			if e != nil {
				return e
			}
			if e = check(tag.Target, tag.TargetType); e != nil {
				return e
			}
		case plumbing.BlobObject:
		default:
			return fmt.Errorf("invalid object kind")
		}
	}
	return nil
}
