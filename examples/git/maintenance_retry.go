package main

import (
	"errors"
	"fmt"
)

const pushMaintenanceAttempts = 2

var (
	errMaintenanceConflict = errors.New("maintenance conflicted; retry push")
	errMaintenancePending  = errors.New("maintenance remains pending; retry push")
)

// Retry one stale-view maintenance result before the caller receives the pack.
// Each reopen observes whether a pending checkpoint actually committed.
func retryBeforePush(before func() (bool, error), reopen func() error) error {
	for attempt := 1; ; attempt++ {
		needsReopen, err := before()
		if err == nil {
			if needsReopen {
				if err := reopen(); err != nil {
					return fmt.Errorf("reopen after maintenance: %w", err)
				}
			}
			return nil
		}
		retryable := errors.Is(err, errMaintenanceConflict) || errors.Is(err, errMaintenancePending)
		if !retryable || attempt == pushMaintenanceAttempts {
			return err
		}
		if err := reopen(); err != nil {
			return fmt.Errorf("reopen before maintenance retry: %w", err)
		}
	}
}
