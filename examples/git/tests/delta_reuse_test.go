package tests

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRetainedDeltas(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			branch := fmt.Sprintf("reuse-%d", time.Now().UnixNano())
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			data := make([]byte, 16<<20)
			_, _ = rand.New(rand.NewSource(42)).Read(data)
			for version := range 4 {
				data[version*1024]++
				write(t, filepath.Join(source, "content"), data)
				git(t, nil, "-C", source, "add", ".")
				git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(version))
			}
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			for pass := range 2 {
				if pass == 1 {
					finishGitMaintenance(t, url)
				}
				clone := filepath.Join(t.TempDir(), "clone")
				git(t, nil, "clone", "--single-branch", "--branch", branch, url, clone)
				git(t, nil, "-C", clone, "fsck", "--full", "--strict")
				got, err := os.ReadFile(filepath.Join(clone, "content"))
				if err != nil || !bytes.Equal(got, data) {
					t.Fatalf("clone differs: %v", err)
				}
				packs, err := filepath.Glob(filepath.Join(clone, ".git", "objects", "pack", "*.pack"))
				if err != nil || len(packs) != 1 {
					t.Fatalf("clone pack files: %v %v", packs, err)
				}
				info, err := os.Stat(packs[0])
				if err != nil {
					t.Fatal(err)
				}
				if info.Size() >= int64(len(data))*2 {
					t.Fatalf("four related revisions used %d pack bytes", info.Size())
				}
				t.Logf("maintenance=%t four16MiB revisions clone=%d bytes", pass == 1, info.Size())
			}
		})
	}
}
