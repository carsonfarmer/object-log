package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

func TestRetentionResolvesUncertainOutcomesAndDisconnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		acquire := []wal.RetentionState{
			wal.RetentionStatePending, wal.RetentionStateActiveCollection,
			wal.RetentionStateConflict, wal.RetentionStateActiveCollection, wal.RetentionStateApplied,
		}
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
		start := time.Now()
		err := retained(t.Context(), next(&acquire), next(&release), func() error {
			return io.ErrClosedPipe
		})
		if !errors.Is(err, io.ErrClosedPipe) || len(acquire) != 0 || len(release) != 0 || len(ids) != 8 {
			t.Fatalf("outcomes were not resolved: acquire=%v release=%v ids=%d err=%v", acquire, release, len(ids), err)
		}
		for _, id := range ids {
			if len(id) != 16 || !bytes.Equal(id, ids[0]) {
				t.Fatal("retention outcome retried with a different ID")
			}
		}
		if elapsed := time.Since(start); elapsed != 200*time.Millisecond {
			t.Fatalf("collection wait = %v, want 200ms", elapsed)
		}
	})
}

func TestActiveCollectionPreventsOutput(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		released, ran := false, false
		calls := 0
		start := time.Now()
		err := retained(t.Context(), func([]byte) (wal.RetentionState, error) {
			calls++
			return wal.RetentionStateActiveCollection, nil
		}, func([]byte) (wal.RetentionState, error) {
			released = true
			return wal.RetentionStateApplied, nil
		}, func() error {
			ran = true
			return nil
		})
		if !errors.Is(err, errCollectionActive) || ran || !released || calls != retentionResolutionAttempts {
			t.Fatalf("collection handling: calls=%d ran=%v released=%v err=%v", calls, ran, released, err)
		}
		if elapsed := time.Since(start); elapsed != 1500*time.Millisecond {
			t.Fatalf("collection wait = %v, want 1.5s", elapsed)
		}
	})
}

func TestRetentionCollectionWaitIsCancelable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()
		released, ran := false, false
		calls := 0
		start := time.Now()
		err := retained(ctx, func([]byte) (wal.RetentionState, error) {
			calls++
			return wal.RetentionStateActiveCollection, nil
		}, func([]byte) (wal.RetentionState, error) {
			released = true
			return wal.RetentionStateApplied, nil
		}, func() error {
			ran = true
			return nil
		})
		if !errors.Is(err, context.Canceled) || ran || !released || calls != 1 {
			t.Fatalf("canceled collection wait: calls=%d ran=%v released=%v err=%v", calls, ran, released, err)
		}
		if elapsed := time.Since(start); elapsed != 50*time.Millisecond {
			t.Fatalf("canceled collection wait = %v, want 50ms", elapsed)
		}
	})
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
