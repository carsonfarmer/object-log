package main

import "testing"

func TestTargetIDMatchesQualificationRunner(t *testing.T) {
	values := map[string]string{
		"WAL_ENDPOINT": "https://s3.us-west-2.amazonaws.com",
		"WAL_BUCKET":   "test-bucket",
		"WAL_REGION":   "us-west-2",
		"WAL_PREFIX":   "qualification/test/git",
	}
	const want = "fecbc72836d1036e6d4fea7eb24c1602ef6e599c18b252dd5d95b4dbda1d14a5"
	if got := targetID(func(name string) string { return values[name] }); got != want {
		t.Fatalf("target ID = %s, want %s", got, want)
	}
}
