package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	cm "go.bytecodealliance.org/pkg/wit/types"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
)

const (
	walTransportCalls = 1024 + 24*1040
	walTransportBytes = 24*(1040<<20) + 8<<20
)

func walLogLimits(collectionObjects uint64) wal.LogLimits {
	return wal.LogLimits{
		MaxTailEntries: 1024, ResolutionWindow: 1024,
		MaxInlineOperationBytes: 64 << 10, MaxInlineResultBytes: 4 << 10,
		MaxObjectRefs: 1024, MaxObjectBytes: 2 << 20,
		MaxCommitBytes: 1 << 20, MaxHeadBytes: 256 << 10,
		MaxCheckpointBytes: 16 << 20, MaxRetentionIds: 1024,
		MaxCollectionObjects: collectionObjects, MaxCollectionPlanBytes: 16 << 20,
	}
}

func walTransportLimits() wal.TransportLimits {
	return wal.TransportLimits{MaxCalls: walTransportCalls, MaxBytes: walTransportBytes}
}

func sessionToken(getenv func(string) string) cm.Option[string] {
	if token := getenv("WAL_SESSION_TOKEN"); token != "" {
		return cm.Some(token)
	}
	return cm.None[string]()
}

func walSettings(getenv func(string) string, logID string, limits requestLimits) (wal.Config, error) {
	mode := wal.CredentialModeStaticCredentials
	switch getenv("WAL_CREDENTIAL_MODE") {
	case "static":
	case "instance-role":
		mode = wal.CredentialModeInstanceRole
	default:
		return wal.Config{}, fmt.Errorf("WAL_CREDENTIAL_MODE must be static or instance-role")
	}
	return wal.Config{
		Endpoint: getenv("WAL_ENDPOINT"), Bucket: getenv("WAL_BUCKET"), Region: getenv("WAL_REGION"),
		CredentialMode: mode, AccessKey: getenv("WAL_ACCESS_KEY"), SecretKey: getenv("WAL_SECRET_KEY"),
		SessionToken: sessionToken(getenv), Prefix: getenv("WAL_PREFIX"), LogId: logID,
		LogLimits: walLogLimits(uint64(limits.collectionObjects)), TransportLimits: walTransportLimits(),
	}, nil
}

func targetID(getenv func(string) string) string {
	hash := sha256.New()
	for _, name := range []string{"WAL_ENDPOINT", "WAL_BUCKET", "WAL_REGION", "WAL_PREFIX"} {
		_, _ = io.WriteString(hash, getenv(name))
		_, _ = hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}
