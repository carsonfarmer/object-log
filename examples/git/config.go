package main

import (
	"strings"

	config "object-log-git-proof/bindings/wasi_config_store"
)

func getConfig(name string) string {
	imports.Lock()
	defer finishImports()
	result := config.Get(strings.ToLower(name))
	if !result.IsOk() {
		panic("configuration unavailable")
	}
	if value := result.Ok(); value.IsSome() {
		return value.Some()
	}
	return ""
}
