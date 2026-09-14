package main

import "testing"

func TestSessionToken(t *testing.T) {
	for _, test := range []struct {
		name, value string
		wantSome    bool
	}{
		{name: "unset"},
		{name: "temporary", value: "token", wantSome: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := sessionToken(func(name string) string {
				if name != "WAL_SESSION_TOKEN" {
					t.Fatalf("unexpected environment lookup %q", name)
				}
				return test.value
			})
			if got.IsSome() != test.wantSome {
				t.Fatalf("IsSome() = %t, want %t", got.IsSome(), test.wantSome)
			}
			if got.IsSome() && got.Some() != test.value {
				t.Fatalf("Some() = %q, want %q", got.Some(), test.value)
			}
		})
	}
}
