package main

import (
	"errors"
	"fmt"

	"github.com/go-git/go-billy/v6/memfs"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/storage/filesystem/dotgit"
)

// This filesystem stays empty; Ref delegates go-git's complete path checks.
var refNameChecks = dotgit.New(memfs.New())

func validateRefName(name plumbing.ReferenceName) error {
	if !name.IsUnderRefs() {
		return fmt.Errorf("invalid ref update")
	}
	if err := name.Validate(); err != nil {
		return err
	}
	_, err := refNameChecks.Ref(name)
	if errors.Is(err, plumbing.ErrReferenceNotFound) {
		return nil
	}
	return err
}
