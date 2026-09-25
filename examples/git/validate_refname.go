package main

import (
	"errors"
	"fmt"
	"maps"
	"slices"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"
)

func validateRefName(refNames *dotgit.DotGit, name plumbing.ReferenceName) error {
	// Ref applies go-git's receive-pack path-component check before lookup.
	// On an empty in-memory filesystem, a safe name returns only "not found".
	if !name.IsUnderRefs() || !name.IsSafe() {
		return fmt.Errorf("invalid ref update")
	}
	if err := name.Validate(); err != nil {
		return err
	}
	if _, err := refNames.Ref(name); !errors.Is(err, plumbing.ErrReferenceNotFound) {
		if err != nil {
			return err
		}
		return fmt.Errorf("unexpected existing ref in name check")
	}
	return nil
}

// Remove only refs admitted by the old catalog rules but refused by receive-pack.
// An invalid default HEAD requires an explicit, already-existing replacement.
func withoutInvalidRefs(meta rootMeta, replacement string) (rootMeta, []string, error) {
	refNames := dotgit.New(memfs.New())
	clean := meta
	clean.Refs = maps.Clone(meta.Refs)
	removed := []string{}
	headInvalid := validateRefName(refNames, plumbing.ReferenceName(meta.Head)) != nil
	for name := range meta.Refs {
		if validateRefName(refNames, plumbing.ReferenceName(name)) != nil {
			delete(clean.Refs, name)
			removed = append(removed, name)
		}
	}
	slices.Sort(removed)
	if headInvalid {
		if !plumbing.ReferenceName(replacement).IsBranch() || clean.Refs[replacement] == "" {
			return rootMeta{}, nil, fmt.Errorf("invalid default HEAD requires an existing valid replacement branch")
		}
		clean.Head = replacement
	} else if replacement != "" {
		return rootMeta{}, nil, fmt.Errorf("replacement HEAD is only allowed when the stored HEAD is invalid")
	}
	return clean, removed, nil
}
