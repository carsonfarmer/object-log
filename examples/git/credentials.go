package main

import cm "go.bytecodealliance.org/pkg/wit/types"

func sessionToken(getenv func(string) string) cm.Option[string] {
	if token := getenv("WAL_SESSION_TOKEN"); token != "" {
		return cm.Some(token)
	}
	return cm.None[string]()
}
