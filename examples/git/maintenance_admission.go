package main

import (
	"fmt"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

// Keep ordinary pushes below the WAL tail limit. Run before receiving any pack
// or validating updates; a completed checkpoint requires a fresh store afterward.
func (s *store) beforePush() (reopen bool, err error) {
	if s.tailEntries < 64 {
		return false, nil
	}
	result, err := s.maintain()
	if err != nil {
		return false, err
	}
	switch result.State {
	case wal.MaintenanceStateComplete, wal.MaintenanceStateMore, wal.MaintenanceStateRetained:
		return true, nil
	default:
		return false, fmt.Errorf("maintenance has not settled; retry push")
	}
}
