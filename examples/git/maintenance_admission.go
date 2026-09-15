package main

import (
	"fmt"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Keep ordinary pushes below the WAL tail limit without traversing the Git
// graph. Run before receiving any pack; a checkpoint requires a fresh store.
func (s *store) beforePush() (reopen bool, err error) {
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
