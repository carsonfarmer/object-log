package main

import (
	"errors"
	"fmt"

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
