package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRepeatedPushes(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" || os.Getenv("GIT_REPEATED_PUSHES") == "" {
		t.Skip("set GIT_PROBE_URL and GIT_REPEATED_PUSHES=1 for local repeated-push coverage")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			branch := fmt.Sprintf("repeated-%d", time.Now().UnixNano())
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			for i := 0; i < 129; i++ {
				write(t, filepath.Join(source, "value"), []byte(fmt.Sprint(i)))
				git(t, nil, "-C", source, "add", ".")
				git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(i))
				git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			}
			cold := filepath.Join(t.TempDir(), "cold")
			git(t, nil, "clone", "--single-branch", "--branch", branch, url, cold)
			git(t, nil, "-C", cold, "fsck", "--full")
			want := string(git(t, nil, "-C", source, "rev-list", "HEAD"))
			if got := string(git(t, nil, "-C", cold, "rev-list", "HEAD")); got != want {
				t.Fatal("repeated pushes lost history")
			}
			data, err := os.ReadFile(filepath.Join(cold, "value"))
			if err != nil || string(data) != "128" {
				t.Fatalf("wrong latest content: %q %v", data, err)
			}
		})
	}
}
