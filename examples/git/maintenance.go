package main

import (
	"fmt"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Maintenance publishes the reachable catalog, checkpoints it, then completes
// one fenced deletion batch. An empty tail needs a publish-only first pass;
// `more` requests the next bounded pass.
func (s *store) maintain() (wal.CollectionResult, error) {
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
	changed := false
	for prefix, root := range s.buckets {
		replacement, exists, err := filterRadix(root, func(id string) bool { return live[id] }, s.loadBucket, s.saveBucket)
		if err != nil {
			return wal.CollectionResult{}, err
		}
		if exists {
			changed = changed || replacement != root
			s.buckets[prefix] = replacement
		} else {
			changed = true
			delete(s.buckets, prefix)
		}
	}
	if s.tailEntries == 0 && !changed {
		return s.collect()
	}
	root, err := s.stageRoot(s.meta.Refs)
	if err != nil {
		return wal.CollectionResult{}, err
	}
	if s.tailEntries == 0 {
		outcome, err := s.publishRoot(root)
		if err != nil {
			return wal.CollectionResult{}, err
		}
		switch outcome.Tag() {
		case wal.OutcomeCommitted:
			return wal.CollectionResult{State: wal.MaintenanceStateMore}, nil
		case wal.OutcomeConflict, wal.OutcomeExpired:
			return wal.CollectionResult{State: wal.MaintenanceStateConflict}, nil
		case wal.OutcomePending:
			return wal.CollectionResult{State: wal.MaintenanceStatePending}, nil
		default:
			return wal.CollectionResult{}, fmt.Errorf("unknown publication outcome %d", outcome.Tag())
		}
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
	return s.collect()
}

// checkpointTail checkpoints the authenticated current catalog as-is. Full
// maintenance separately prunes and reclaims unreachable Git objects.
func (s *store) checkpointTail() (wal.MaintenanceState, error) {
	if err := s.ctx.Err(); err != nil {
		return 0, err
	}
	return unwrap(s.session.Checkpoint(nil, []*wal.Object{s.stateRoot}))
}

func (s *store) collect() (wal.CollectionResult, error) {
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
