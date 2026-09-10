package main

import (
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Maintenance publishes the reachable catalog as an exact-view checkpoint,
// then completes one fenced deletion batch. Another call safely resumes a
// pending batch; `more` requests another pass, never an unbounded loop.
func (s *store) maintain() (wal.CollectionResult, error) {
	if s.tailEntries > 0 {
		tips := make([]plumbing.Hash, 0, len(s.meta.Refs))
		for _, id := range s.meta.Refs {
			tips = append(tips, plumbing.NewHash(id))
		}
		ids, err := revlist.Objects(s, tips, nil)
		if err != nil {
			return wal.CollectionResult{}, err
		}
		live := make(map[string]bool, len(ids))
		for _, id := range ids {
			// revlist returns leaf hashes without checking whether their objects exist.
			if err = s.HasEncodedObject(id); err != nil {
				return wal.CollectionResult{}, err
			}
			live[id.String()] = true
		}
		for prefix, root := range s.buckets {
			replacement, exists, err := filterRadix(root, func(id string) bool { return live[id] }, s.loadBucket, s.saveBucket)
			if err != nil {
				return wal.CollectionResult{}, err
			}
			if exists {
				s.buckets[prefix] = replacement
			} else {
				delete(s.buckets, prefix)
			}
		}
		root, err := s.stageRoot(s.meta.Refs)
		if err != nil {
			return wal.CollectionResult{}, err
		}
		if err := s.ctx.Err(); err != nil {
			return wal.CollectionResult{}, err
		}
		state, err := unwrap(s.session.Checkpoint(nil, []*wal.Object{root}))
		if err != nil {
			return wal.CollectionResult{}, err
		}
		if state != wal.MaintenanceStateComplete {
			return wal.CollectionResult{State: state}, nil
		}
	}
	// Refresh shares the original transport counters and observes the checkpoint
	// or collection epoch before asking the core to resume/install a fenced plan.
	if err := s.ctx.Err(); err != nil {
		return wal.CollectionResult{}, err
	}
	session, err := unwrap(s.session.Refresh())
	if err != nil {
		return wal.CollectionResult{}, err
	}
	defer session.Drop()
	if err := s.ctx.Err(); err != nil {
		return wal.CollectionResult{}, err
	}
	return unwrap(session.Collect())
}
