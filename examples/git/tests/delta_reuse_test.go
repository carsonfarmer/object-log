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
		for _, shape := range []struct{ size, edit int }{{1536, 16}, {16 << 20, 1 << 20}} {
			t.Run(fmt.Sprintf("%s/bytes=%d", format, shape.size), func(t *testing.T) {
				branch := fmt.Sprintf("reuse-%d", time.Now().UnixNano())
				url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
				source := filepath.Join(t.TempDir(), "source")
				git(t, nil, "init", "--object-format="+format, "-b", branch, source)
				data := make([]byte, shape.size)
				_, _ = rand.New(rand.NewSource(42)).Read(data)
				for version := range 4 {
					_, _ = rand.New(rand.NewSource(int64(version) + 43)).Read(data[version*shape.edit : (version+1)*shape.edit])
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
					deltas := 0
					for _, line := range strings.Split(string(git(t, nil, "-C", clone, "verify-pack", "-v", strings.TrimSuffix(packs[0], ".pack")+".idx")), "\n") {
						if fields := strings.Fields(line); len(fields) == 7 && fields[1] == "blob" {
							deltas++
						}
					}
					if deltas == 0 {
						t.Fatal("cold clone did not reuse any blob delta")
					}
					t.Logf("maintenance=%t four %d-byte revisions with %d-byte edits clone=%d bytes deltas=%d", pass == 1, shape.size, shape.edit, info.Size(), deltas)
				}
			})
		}
	}
}
