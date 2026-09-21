package main

import (
	"context"
	"encoding/json"
	"fmt"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"net/http"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/revlist"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Keep ordinary pushes below the WAL tail limit without traversing the Git
// graph. Run before receiving any pack; a checkpoint requires a fresh store.
func (s *store) beforePush() (bool, error) {
	if s.tailEntries < 64 {
		return false, nil
	}
	state, err := s.checkpointTail()
	if err != nil {
		return false, err
	}
	switch state {
	case wal.MaintenanceStateComplete:
		return true, nil
	case wal.MaintenanceStateConflict:
		return false, errMaintenanceConflict
	case wal.MaintenanceStatePending:
		return false, errMaintenancePending
	default:
		return false, fmt.Errorf("unknown maintenance state %d", state)
	}
}

// Maintenance publishes the reachable catalog, checkpoints it, then completes
// one fenced deletion batch. Further passes use /collect without repeating
// the Git traversal. A concurrent publication is always kept when checkpointing.
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
	root := s.stateRoot
	if changed {
		root, err = s.stageRoot(s.meta.Refs)
		if err != nil {
			return wal.CollectionResult{}, err
		}
	}
	checkpointSession := s.session
	if s.tailEntries == 0 {
		outcome, err := s.publishRoot(root)
		if err != nil {
			return wal.CollectionResult{}, err
		}
		switch outcome.Tag() {
		case wal.OutcomeCommitted:
			fresh, err := unwrap(s.session.Refresh)
			if err != nil {
				return wal.CollectionResult{}, err
			}
			defer fresh.Drop()
			current, err := unwrap(fresh.LatestCompleteState)
			if err != nil {
				return wal.CollectionResult{}, err
			}
			if current.Latest.IsNone() || len(current.Latest.Some().Objects) != 1 {
				return wal.CollectionResult{}, fmt.Errorf("invalid published root")
			}
			// Another push may have followed pruning. Checkpoint its winning
			// root, never the stale root we just published.
			root = current.Latest.Some().Objects[0]
			defer root.Drop()
			if current.TailEntries == 0 {
				return s.collect()
			}
			checkpointSession = fresh
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
	state, err := unwrap(func() wt.Result[wal.MaintenanceState, wal.Failure] {
		return checkpointSession.Checkpoint(nil, []*wal.Object{root})
	})
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
	return unwrap(func() wt.Result[wal.MaintenanceState, wal.Failure] {
		return s.session.Checkpoint(nil, []*wal.Object{s.stateRoot})
	})
}

func (s *store) collect() (wal.CollectionResult, error) {
	// Refresh shares the original transport counters and observes the checkpoint
	// or collection epoch before asking the core to resume/install a fenced plan.
	if err := s.ctx.Err(); err != nil {
		return wal.CollectionResult{}, err
	}
	session, err := unwrap(s.session.Refresh)
	if err != nil {
		return wal.CollectionResult{}, err
	}
	defer session.Drop()
	if err := s.ctx.Err(); err != nil {
		return wal.CollectionResult{}, err
	}
	return collectSession(s.ctx, session, s.limits)
}

// Resume an installed deletion plan without loading the Git catalog again.
func collectSession(ctx context.Context, session *wal.Session, limits requestLimits) (wal.CollectionResult, error) {
	if err := ctx.Err(); err != nil {
		return wal.CollectionResult{}, err
	}
	return unwrap(func() wt.Result[wal.CollectionResult, wal.Failure] {
		return session.Collect(uint64(min(limits.collectionCandidates, limits.collectionObjects)))
	})
}

func writeMaintenance(w http.ResponseWriter, report wal.CollectionResult, err error) {
	if err != nil {
		http.Error(w, "maintenance failed: "+err.Error(), operationStatus(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	states := map[wal.MaintenanceState]string{
		wal.MaintenanceStateComplete: "complete", wal.MaintenanceStateMore: "more",
		wal.MaintenanceStateConflict: "conflict", wal.MaintenanceStatePending: "pending",
		wal.MaintenanceStateRetained: "retained",
	}
	_ = json.NewEncoder(w).Encode(struct {
		State   string `json:"state"`
		Objects uint64 `json:"candidate_objects"`
		Bytes   uint64 `json:"candidate_bytes"`
	}{states[report.State], report.Objects, report.Bytes})
}
