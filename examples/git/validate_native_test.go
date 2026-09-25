//go:build git_native_test

package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/storage/memory"
)

// The native test file list excludes the WASI-backed store implementation.
type store struct {
	*memory.Storage
	meta    rootMeta
	pending map[string]struct{}
}

func TestValidateRejectsUnsafeRefBeforePublication(t *testing.T) {
	for _, format := range []config.ObjectFormat{config.SHA1, config.SHA256} {
		t.Run(format.String(), func(t *testing.T) {
			st := &store{Storage: memory.NewStorage(memory.WithObjectFormat(format)), meta: rootMeta{Format: format, Refs: map[string]string{}}}
			command := &packp.Command{
				Name: plumbing.ReferenceName("refs/heads/\u200c./review-probe"),
				Old:  plumbing.NewHash(strings.Repeat("0", format.HexSize())),
				New:  plumbing.NewHash(strings.Repeat("1", format.HexSize())),
			}
			refs, err := validate(st, []*packp.Command{command})
			if refs != nil || !errors.Is(err, plumbing.ErrInvalidReferenceName) {
				t.Fatalf("validate returned refs=%v err=%v, want invalid reference name before publication", refs, err)
			}
		})
	}
}
