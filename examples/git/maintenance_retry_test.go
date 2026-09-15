package main

import (
	"errors"
	"testing"
)

func TestRetryBeforePush(t *testing.T) {
	type result struct {
		reopen bool
		err    error
	}
	fatal := errors.New("fatal maintenance failure")
	refreshFailure := errors.New("refresh failure")
	for _, test := range []struct {
		name        string
		results     []result
		reopenErr   error
		wantErr     error
		wantCalls   int
		wantReopens int
	}{
		{name: "success", results: []result{{reopen: true}}, wantCalls: 1, wantReopens: 1},
		{name: "retry", results: []result{{err: errMaintenanceConflict}, {reopen: true}}, wantCalls: 2, wantReopens: 2},
		{name: "pending resolved", results: []result{{err: errMaintenancePending}, {}}, wantCalls: 2, wantReopens: 1},
		{name: "bound", results: []result{{err: errMaintenancePending}, {err: errMaintenancePending}}, wantErr: errMaintenancePending, wantCalls: pushMaintenanceAttempts, wantReopens: pushMaintenanceAttempts - 1},
		{name: "fatal maintenance", results: []result{{err: fatal}}, wantErr: fatal, wantCalls: 1},
		{name: "fatal reopen", results: []result{{err: errMaintenanceConflict}}, reopenErr: refreshFailure, wantErr: refreshFailure, wantCalls: 1, wantReopens: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls, reopens := 0, 0
			err := retryBeforePush(func() (bool, error) {
				result := test.results[calls]
				calls++
				return result.reopen, result.err
			}, func() error {
				reopens++
				return test.reopenErr
			})
			if !errors.Is(err, test.wantErr) || calls != test.wantCalls || reopens != test.wantReopens {
				t.Fatalf("err=%v calls=%d reopens=%d", err, calls, reopens)
			}
		})
	}
}
