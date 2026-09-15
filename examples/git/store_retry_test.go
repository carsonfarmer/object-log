package main

import (
	"errors"
	"testing"
)

func TestRetryOpenStore(t *testing.T) {
	fatal := errors.New("invalid root record")
	refreshFailure := errors.New("refresh failure")
	for _, test := range []struct {
		name          string
		openErrors    []error
		refreshError  error
		wantError     error
		wantOpens     int
		wantRefreshes int
	}{
		{name: "success", openErrors: []error{nil}, wantOpens: 1},
		{name: "expired", openErrors: []error{errExpired, nil}, wantOpens: 2, wantRefreshes: 1},
		{name: "expires again", openErrors: []error{errExpired, errExpired}, wantError: errExpired, wantOpens: 2, wantRefreshes: 1},
		{name: "fatal", openErrors: []error{fatal}, wantError: fatal, wantOpens: 1},
		{name: "refresh failure", openErrors: []error{errExpired}, refreshError: refreshFailure, wantError: refreshFailure, wantOpens: 1, wantRefreshes: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			opens, refreshes := 0, 0
			s, err := retryOpenStore(func() (*int, error) {
				err := test.openErrors[opens]
				opens++
				if err != nil {
					return nil, err
				}
				return new(int), nil
			}, func() error {
				refreshes++
				return test.refreshError
			})
			if !errors.Is(err, test.wantError) || opens != test.wantOpens || refreshes != test.wantRefreshes {
				t.Fatalf("err=%v opens=%d refreshes=%d", err, opens, refreshes)
			}
			if (s != nil) != (test.wantError == nil) {
				t.Fatalf("store=%v err=%v", s, err)
			}
		})
	}
}
