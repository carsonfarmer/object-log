package tests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type maintenanceResult struct {
	State   string `json:"state"`
	Objects uint64 `json:"candidate_objects"`
}

func TestMaintenance(t *testing.T) {
	endpoint := os.Getenv("GIT_PROBE_URL")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL to an isolated local Spin/MinIO instance")
	}
	for _, format := range []string{"sha1", "sha256"} {
		t.Run(format, func(t *testing.T) {
			url := strings.TrimRight(endpoint, "/") + "/" + format + ".git"
			finishGitMaintenance(t, url)
			source := filepath.Join(t.TempDir(), "source")
			liveBranch := fmt.Sprintf("cleanup-live-%d", time.Now().UnixNano())
			deadBranch := liveBranch + "-dead"
			git(t, nil, "init", "--object-format="+format, "-b", liveBranch, source)
			git(t, nil, "-C", source, "config", "maintenance.autoDetach", "false")
			write(t, filepath.Join(source, "keep"), []byte("maintenance live content"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "live")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+liveBranch)
			git(t, nil, "-C", source, "checkout", "--orphan", deadBranch)
			git(t, nil, "-C", source, "rm", "-rf", ".")
			write(t, filepath.Join(source, "discard"), []byte("maintenance unreachable content"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "dead")
			dead := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+deadBranch)
			git(t, nil, "-C", source, "push", url, ":refs/heads/"+deadBranch)
			git(t, nil, "-C", source, "checkout", liveBranch)
			// Begin from tail zero, then leave exactly 64 durable updates behind.
			for i := range 61 {
				write(t, filepath.Join(source, "keep"), []byte(fmt.Sprintf("maintenance live content %d", i)))
				git(t, nil, "-C", source, "add", ".")
				git(t, nil, "-C", source, "commit", "-m", fmt.Sprint(i))
				git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+liveBranch)
			}
			live := strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			// The empty receive checkpoints the as-is catalog without appending.
			post(t, url+"/git-receive-pack", "git-receive-pack", []byte("0000"))
			first := requestGitMaintenance(t, url)
			if first.State != "more" || first.Objects != 0 {
				t.Fatalf("tail-zero pruning did not publish before collection: %+v", first)
			}
			write(t, filepath.Join(source, "keep"), []byte("intervening live update"))
			git(t, nil, "-C", source, "add", ".")
			git(t, nil, "-C", source, "commit", "-m", "intervening update")
			git(t, nil, "-C", source, "push", url, "HEAD:refs/heads/"+liveBranch)
			live = strings.TrimSpace(string(git(t, nil, "-C", source, "rev-parse", "HEAD")))
			if candidates := finishGitMaintenance(t, url); candidates == 0 {
				t.Fatal("cleanup did not finish a positive plan")
			}
			if settled := requestGitMaintenance(t, url); settled.State != "complete" || settled.Objects != 0 {
				t.Fatalf("settled maintenance created more work: %+v", settled)
			}
			cold := filepath.Join(t.TempDir(), "cold")
			git(t, nil, "clone", "--branch", liveBranch, url, cold)
			git(t, nil, "-C", cold, "fsck", "--full")
			if got := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", "HEAD"))); got != live {
				t.Fatal("maintenance changed live tip")
			}
			command := gitCommand("-C", cold, "fetch", url, dead)
			if output, err := command.CombinedOutput(); err == nil {
				t.Fatalf("unreachable commit survived pruning: %s", output)
			}
		})
	}
}

func requestGitMaintenance(t *testing.T, url string) maintenanceResult {
	t.Helper()
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
	defer response.Body.Close()
	var result maintenanceResult
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("maintenance HTTP %d: %v", response.StatusCode, err)
	}
	return result
}

func finishGitMaintenance(t *testing.T, url string) uint64 {
	t.Helper()
	var candidates uint64
	for range 16 {
		result := requestGitMaintenance(t, url)
		candidates += result.Objects
		if result.State == "complete" {
			return candidates
		}
		if result.State != "more" {
			t.Fatalf("unexpected maintenance outcome: %s", result.State)
		}
	}
	t.Fatal("maintenance did not complete")
	return 0
}
