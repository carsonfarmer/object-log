package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
)

func targetID(getenv func(string) string) string {
	hash := sha256.New()
	for _, name := range []string{"WAL_ENDPOINT", "WAL_BUCKET", "WAL_REGION", "WAL_PREFIX"} {
		_, _ = io.WriteString(hash, getenv(name))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
