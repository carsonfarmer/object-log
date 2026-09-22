package main

import (
	"fmt"
	"maps"
	"strings"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
)

func validate(st *store, cmds []*packp.Command) (map[string]string, error) {
	refs := map[string]string{}
	maps.Copy(refs, st.meta.Refs)
	seen := map[string]bool{}
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
		if _, e := st.EncodedObject(plumbing.AnyObject, cmd.New); e != nil {
			return nil, e
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
	}
	if err := validateRefs(st.meta.Format, refs); err != nil {
		return nil, err
	}
	ids := make([]plumbing.Hash, 0, len(st.pending))
	for id := range st.pending {
		ids = append(ids, plumbing.NewHash(id))
	}
	if err := verifyObjects(st, ids); err != nil {
		return nil, err
	}
	return refs, nil
}
