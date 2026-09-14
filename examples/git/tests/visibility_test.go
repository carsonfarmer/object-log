package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Deleting a ref must stop serving its now-unreachable history immediately,
// without requiring maintenance to remove the stored objects first.
func TestFetchVisibility(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			branch := fmt.Sprintf("visibility-%d", time.Now().UnixNano())
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", branch, source)
			write(t, filepath.Join(source, "private"), []byte(branch))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", branch)
			id := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+branch)
			for _, version := range []string{"0", "2"} {
				client := filepath.Join(t.TempDir(), "visible")
				git(t, nil, "init", "--object-format="+format, client)
				git(t, nil, "-C", client, "-c", "protocol.version="+version, "fetch", url, id)
			}
			git(t, nil, "-C", source, "push", url, ":refs/heads/"+branch)
			for _, version := range []string{"0", "2"} {
				client := filepath.Join(t.TempDir(), "hidden")
				git(t, nil, "init", "--object-format="+format, client)
				command := gitCommand("-C", client, "-c", "protocol.version="+version, "fetch", url, id)
				command.Env = append(command.Env, "GIT_TERMINAL_PROMPT=0")
				if output, err := command.CombinedOutput(); err == nil {
					t.Fatalf("protocol %s fetched unreachable commit before cleanup: %s", version, output)
				}
			}
		})
	}
}
