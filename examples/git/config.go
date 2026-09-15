package main

import (
	"strings"

	config "object-log-git-proof/bindings/wasi_config_0_2_0_draft_2024_09_27_store"
)

func getConfig(name string) string {
	result := config.Get(strings.ToLower(name))
	if !result.IsOk() {
		panic("configuration unavailable")
	}
	if value := result.Ok(); value.IsSome() {
		return value.Some()
	}
	return ""
}
