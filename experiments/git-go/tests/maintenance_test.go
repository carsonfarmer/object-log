package tests

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestMaintenance(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			source := filepath.Join(t.TempDir(), "source")
			git(t, nil, "init", "--object-format="+format, "-b", "cleanup-live", source)
			write(t, filepath.Join(source, "keep"), []byte("maintenance live content"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "live")
			live := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/cleanup-live")
			git(t, nil, "-C", source, "checkout", "--orphan", "cleanup-dead")
			git(t, nil, "-C", source, "rm", "-rf", ".")
			write(t, filepath.Join(source, "discard"), []byte("maintenance unreachable content"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "dead")
			dead := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/cleanup-dead")
			git(t, nil, "-C", source, "push", url, ":refs/heads/cleanup-dead")
			var candidates uint64
			complete := false
			for attempt := 0; attempt < 10; attempt++ {
				r, err := http.NewRequest(http.MethodPost, url+"/maintenance", nil)
				if err != nil {
					t.Fatal(err)
				}
				if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
					r.SetBasicAuth("git", password)
				}
				response, err := http.DefaultClient.Do(r)
				if err != nil {
					t.Fatal(err)
				}
				var result struct {
					State   string `json:"state"`
					Objects uint64 `json:"candidate_objects"`
				}
				err = json.NewDecoder(response.Body).Decode(&result)
				_ = response.Body.Close()
				if err != nil || response.StatusCode != 200 {
					t.Fatalf("maintenance HTTP %d: %v", response.StatusCode, err)
				}
				candidates += result.Objects
				if result.State == "complete" {
					complete = true
					break
				}
				if result.State != "more" {
					t.Fatalf("unexpected maintenance outcome: %s", result.State)
				}
			}
			if !complete || candidates == 0 {
				t.Fatalf("cleanup did not finish a positive plan: %v/%d", complete, candidates)
			}
			cold := filepath.Join(t.TempDir(), "cold")
			git(t, nil, "clone", "--branch", "cleanup-live", url, cold)
			git(t, nil, "-C", cold, "fsck", "--full")
			if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", "HEAD"))); got != live {
				t.Fatal("maintenance changed live tip")
			}
			args := []string{"-C", cold}
			if password := os.Getenv("GIT_PROBE_PASSWORD"); password != "" {
				args = append(args, "-c", "http.extraHeader=Authorization: Basic "+base64.StdEncoding.EncodeToString([]byte("git:"+password)))
			}
			args = append(args, "fetch", url, dead)
			command := exec.Command("git", args...)
			command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/dev/null")
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("unreachable commit survived pruning: %s", output)
			}
		})
	}
}
