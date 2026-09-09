package main

import (
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/filemode"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/storer"
)

// Check every new object, including objects the incoming refs do not reach.
// Direct edges suffice once all objects in the durable catalog were validated.
func verifyObjects(st storer.EncodedObjectStorer, ids []plumbing.Hash) error {
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
			if e = tree.Validate(); e != nil {
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
