package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"

	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

const retentionResolutionAttempts = 16

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
	defer func() { err = errors.Join(err, resolveRelease(release, id)) }()
	if err = resolveAcquire(ctx, retain, id); err != nil {
		return err
	}
	return run()
}

func resolveAcquire(ctx context.Context, call retentionCall, id []byte) error {
	for range retentionResolutionAttempts {
		if err := ctx.Err(); err != nil {
			return err
		}
		state, err := call(id)
		if err != nil {
			return err
		}
		switch state {
		case wal.RetentionStateApplied:
			return nil
		case wal.RetentionStateActiveCollection:
			return errCollectionActive
		case wal.RetentionStateConflict:
			// The bridge advances the session to the returned head. Repeat with
			// the same ID so an acquisition race has one durable identity.
		case wal.RetentionStatePending:
		}
	}
	return errRetentionUnresolved
}

// Release ignores the request context: a disconnect must not abandon a
// retention. Exhaustion remains explicit for drained operator recovery.
func resolveRelease(call retentionCall, id []byte) error {
	return resolveRetention(func() (wal.RetentionState, error) { return call(id) }, "release")
}

func resolveDrainedRecovery(call func() (wal.RetentionState, error)) error {
	return resolveRetention(call, "recovery")
}

func resolveRetention(call func() (wal.RetentionState, error), operation string) error {
	for range retentionResolutionAttempts {
		state, err := call()
		if err != nil {
			return err
		}
		switch state {
		case wal.RetentionStateApplied:
			return nil
		case wal.RetentionStateConflict, wal.RetentionStatePending:
		case wal.RetentionStateActiveCollection:
			return fmt.Errorf("invalid %s state: active collection", operation)
		}
	}
	return errRetentionUnresolved
}
