package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestShallowAndTags(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			branch := fmt.Sprintf("shallow-%d", time.Now().UnixNano())
			tag := branch + "-tag"
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			for i := 0; i < 3; i++ {
				write(t, filepath.Join(source, "value"), []byte(fmt.Sprint(i)))
				git(t, nil, "-C", source, "add", ".")
				git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(i))
			}
			git(t, nil, "-C", source, "tag", "-a", tag, "-m", "annotated release")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch, "refs/tags/"+tag)
			cold := filepath.Join(t.TempDir(), "cold")
			git(t, nil, "-c", "protocol.version=2", "clone", "--no-tags", "--depth=1", "--branch", branch, url, cold)
			count := func(want string) {
				t.Helper()
				if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-list", "--count", "HEAD"))); got != want {
					t.Fatalf("history count %s, want %s", got, want)
				}
			}
			count("1")
			git(t, nil, "-C", cold, "-c", "protocol.version=2", "fetch", "--deepen=1", "origin")
			count("2")
			git(t, nil, "-C", cold, "-c", "protocol.version=2", "fetch", "--unshallow", "origin")
			count("3")
			git(t, nil, "-C", cold, "fsck", "--full")
			want := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", "HEAD"))); got != want {
				t.Fatal("shallow operations changed tip")
			}
			git(t, nil, "-C", cold, "fetch", "origin", "refs/tags/"+tag+":refs/tags/"+tag)
			if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "cat-file", "-t", "refs/tags/"+tag))); got != "tag" {
				t.Fatalf("annotated tag became %q", got)
			}
			if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", tag+"^{}"))); got != want {
				t.Fatal("tag peeled to wrong commit")
			}
			git(t, nil, "-C", cold, "fsck", "--full")
		})
	}
}
