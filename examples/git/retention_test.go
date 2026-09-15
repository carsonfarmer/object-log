package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"

	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

func TestRetentionResolvesUncertainOutcomesAndDisconnect(t *testing.T) {
	acquire := []wal.RetentionState{wal.RetentionStatePending, wal.RetentionStateConflict, wal.RetentionStateApplied}
	release := []wal.RetentionState{wal.RetentionStateConflict, wal.RetentionStatePending, wal.RetentionStateApplied}
	var ids [][]byte
	next := func(states *[]wal.RetentionState) retentionCall {
		return func(id []byte) (wal.RetentionState, error) {
			ids = append(ids, append([]byte(nil), id...))
			state := (*states)[0]
			*states = (*states)[1:]
			return state, nil
		}
	}
	err := retained(context.Background(), next(&acquire), next(&release), func() error {
		return io.ErrClosedPipe
	})
	if !errors.Is(err, io.ErrClosedPipe) || len(acquire) != 0 || len(release) != 0 || len(ids) != 6 {
		t.Fatalf("outcomes were not resolved: acquire=%v release=%v ids=%d err=%v", acquire, release, len(ids), err)
	}
	for _, id := range ids {
		if len(id) != 16 || !bytes.Equal(id, ids[0]) {
			t.Fatal("retention outcome retried with a different ID")
		}
	}
}

func TestActiveCollectionPreventsOutput(t *testing.T) {
	released, ran := false, false
	err := retained(context.Background(), func([]byte) (wal.RetentionState, error) {
		return wal.RetentionStateActiveCollection, nil
	}, func([]byte) (wal.RetentionState, error) {
		released = true
		return wal.RetentionStateApplied, nil
	}, func() error {
		ran = true
		return nil
	})
	if !errors.Is(err, errCollectionActive) || ran || !released {
		t.Fatalf("collection handling: ran=%v released=%v err=%v", ran, released, err)
	}
}

func TestDrainedRecoveryResolvesHeadRaces(t *testing.T) {
	states := []wal.RetentionState{wal.RetentionStatePending, wal.RetentionStateConflict, wal.RetentionStateApplied}
	calls := 0
	err := resolveDrainedRecovery(func() (wal.RetentionState, error) {
		calls++
		state := states[0]
		states = states[1:]
		return state, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("recovery calls=%d error=%v", calls, err)
	}
}

func TestDrainedRecoveryModeIsExclusive(t *testing.T) {
	for _, test := range []struct {
		enabled, recoveryRoute bool
		want                   int
	}{
		{false, false, 0},
		{false, true, http.StatusForbidden},
		{true, false, http.StatusServiceUnavailable},
		{true, true, 0},
	} {
		if got := retentionRecoveryStatus(test.enabled, test.recoveryRoute); got != test.want {
			t.Fatalf("enabled=%v recovery=%v: got %d, want %d", test.enabled, test.recoveryRoute, got, test.want)
		}
	}
}

func TestActiveCollectionIsRetryable(t *testing.T) {
	if got := operationStatus(errCollectionActive); got != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", got, http.StatusServiceUnavailable)
	}
}
