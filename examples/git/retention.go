package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"time"

	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

const (
	retentionResolutionAttempts = 16
	// At most 15 waits (1.5 seconds), in addition to the WAL calls.
	retentionCollectionDelay = 100 * time.Millisecond
)

var (
	errCollectionActive    = errors.New("collection is active")
	errRetentionUnresolved = errors.New("retention outcome remains unresolved")
)

type retentionCall func([]byte) (wal.RetentionState, error)

func retentionRecoveryStatus(enabled, recoveryRoute bool) int {
	if enabled && !recoveryRoute {
		return http.StatusServiceUnavailable
	}
	if recoveryRoute && !enabled {
		return http.StatusForbidden
	}
	return 0
}

func retained(ctx context.Context, retain, release retentionCall, run func() error) (err error) {
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return err
	}
	// Release ignores request cancellation so a disconnect does not abandon a
	// retention. Exhaustion remains explicit for drained operator recovery.
	defer func() {
		err = errors.Join(err, resolveRetention(context.Background(), func() (wal.RetentionState, error) { return release(id) }, "release"))
	}()
	if err = resolveRetention(ctx, func() (wal.RetentionState, error) { return retain(id) }, "acquire"); err != nil {
		return err
	}
	return run()
}

func resolveDrainedRecovery(call func() (wal.RetentionState, error)) error {
	return resolveRetention(context.Background(), call, "recovery")
}

func resolveRetention(ctx context.Context, call func() (wal.RetentionState, error), operation string) error {
	for attempt := range retentionResolutionAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := call()
		if err != nil {
			return err
		}
		switch state {
		case wal.RetentionStateApplied:
			return nil
		case wal.RetentionStateConflict, wal.RetentionStatePending:
		case wal.RetentionStateActiveCollection:
			if operation != "acquire" {
				return fmt.Errorf("invalid %s state: active collection", operation)
			}
			if attempt == retentionResolutionAttempts-1 {
				return errCollectionActive
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(retentionCollectionDelay):
			}
		}
	}
	return errRetentionUnresolved
}
