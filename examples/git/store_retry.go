package main

import (
	"errors"
	"fmt"
)

// Retry an expired request view once, before Git can consume the request body
// or emit a response. The caller's refresh keeps the request transport meter,
// and both opens receive copies of the same request limits.
func retryOpenStore[T any](open func() (T, error), refresh func() error) (T, error) {
	s, err := open()
	if !errors.Is(err, errExpired) {
		return s, err
	}
	if err := refresh(); err != nil {
		var zero T
		return zero, fmt.Errorf("refresh expired store: %w", err)
	}
	s, err = open()
	if err != nil {
		var zero T
		return zero, fmt.Errorf("open refreshed store: %w", err)
	}
	return s, nil
}
