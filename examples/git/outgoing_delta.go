package main

import (
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/packfile"
	"github.com/go-git/go-git/v6/plumbing/storer"
	"github.com/go-git/go-git/v6/storage"
)

const (
	outgoingDeltaObjectBytes    = 1 << 20
	outgoingDeltaCandidateBytes = 16 << 20
	outgoingDeltaWindow         = 2
)

type outgoingObjectSelector struct {
	// Embedding this narrow interface hides optional delta storage on the backing store.
	storer.EncodedObjectStorer
	limits requestLimits
}

func outgoingObjectSelectorFactory(limits requestLimits) func(storage.Storer) packfile.ObjectSelector {
	return func(s storage.Storer) packfile.ObjectSelector {
		return &outgoingObjectSelector{EncodedObjectStorer: s, limits: limits}
	}
}

func (s *outgoingObjectSelector) ObjectsToPack(hashes []plumbing.Hash, _ uint) ([]*packfile.ObjectToPack, error) {
	var candidates [2][]plumbing.Hash // Blobs and trees are selected separately.
	full := make([]*packfile.ObjectToPack, 0, len(hashes))
	for _, hash := range hashes {
		object, err := s.EncodedObject(plumbing.AnyObject, hash)
		if err != nil {
			return nil, err
		}
		size := object.Size()
		group := -1
		switch object.Type() {
		case plumbing.BlobObject:
			group = 0
		case plumbing.TreeObject:
			group = 1
		}
		if group >= 0 && size >= 0 && size <= outgoingDeltaObjectBytes && s.limits.admitDelta(size) {
			candidates[group] = append(candidates[group], hash)
			continue
		}
		full = append(full, &packfile.ObjectToPack{Object: object, Original: object})
	}

	selected := make([]*packfile.ObjectToPack, 0, len(hashes))
	for _, group := range candidates {
		objects, err := packfile.NewDeltaSelector(s).ObjectsToPack(group, outgoingDeltaWindow)
		if err != nil {
			return nil, err
		}
		selected = append(selected, objects...)
	}
	return append(selected, full...), nil
}
