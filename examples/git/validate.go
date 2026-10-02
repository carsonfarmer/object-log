package main

import (
	"fmt"
	"maps"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/object"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
)

func (s *receiveStore) validate(st *store, cmds []*packp.Command) (map[string]string, error) {
	refs := map[string]string{}
	maps.Copy(refs, st.meta.Refs)
	for _, cmd := range cmds {
		name := string(cmd.Name)
		if cmd.Old.HexSize() != st.meta.Format.HexSize() || cmd.New.HexSize() != st.meta.Format.HexSize() {
			return nil, fmt.Errorf("invalid object format")
		}
		if cmd.Old.IsZero() && cmd.New.IsZero() {
			return nil, fmt.Errorf("invalid ref update")
		}
		if e := validateRefName(cmd.Name); e != nil {
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
				previous := object.Commit{Hash: cmd.Old}
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
	s.validated = make(map[plumbing.Hash]bool, len(cmds))
	for _, cmd := range cmds {
		if !cmd.New.IsZero() {
			s.validated[cmd.New] = true
		}
	}
	return refs, nil
}
