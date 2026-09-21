package tests

import (
	"bytes"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Configure these three names with distinct log IDs on a fresh storage prefix,
// password/token authentication, and default branch main. The test uses the
// supplied server as-is; it does not rebuild or relaunch Spin.
func TestRepositoryIsolation(t *testing.T) {
	if os.Getenv("GIT_MULTI_REPOSITORIES") != "1" {
		t.Skip("set GIT_MULTI_REPOSITORIES=1 for configured repository isolation")
	}
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" || os.Getenv("GIT_PROBE_PASSWORD") == "" {
		t.Fatal("GIT_PROBE_URL and GIT_PROBE_PASSWORD are required")
	}
	repositories := []struct {
		name, format, source, url, tip, blob, refs string
	}{
		{name: "alpha/project.git", format: "sha1"},
		{name: "beta/project.git", format: "sha1"},
		{name: "hash256/project.git", format: "sha256"},
	}
	for i := range repositories {
		repo := &repositories[i]
		repo.url = endpoint + "/" + repo.name
		read := repo.url + "/info/refs?service=git-upload-pack"
		write := repo.url + "/info/refs?service=git-receive-pack"
		repositoryStatus(t, read, os.Getenv("GIT_PROBE_PASSWORD"), http.StatusNotFound)
		repositoryStatus(t, write, "invalid-token", http.StatusUnauthorized)
		repositoryStatus(t, read, os.Getenv("GIT_PROBE_PASSWORD"), http.StatusNotFound)
		repositoryStatus(t, write, os.Getenv("GIT_PROBE_PASSWORD"), http.StatusOK)
		repositoryStatus(t, read, os.Getenv("GIT_PROBE_PASSWORD"), http.StatusOK)
	}
	for _, path := range []string{
		"unknown/project.git", "alpha/project.git/extra", "alpha//project.git",
	} {
		repositoryStatus(t, endpoint+"/"+path+"/info/refs?service=git-receive-pack",
			os.Getenv("GIT_PROBE_PASSWORD"), http.StatusNotFound)
	}
	for i := range repositories {
		repo := &repositories[i]
		repo.source = filepath.Join(t.TempDir(), "source")
		git(t, nil, "init", "--object-format="+repo.format, "-b", "main", repo.source)
		write(t, filepath.Join(repo.source, "identity"), []byte(repo.name))
		git(t, nil, "-C", repo.source, "add", ".")
		git(t, nil, "-C", repo.source, "commit", "-m", repo.name)
		git(t, nil, "-C", repo.source, "push", repo.url, "HEAD:refs/heads/main")
		repo.tip = strings.TrimSpace(string(git(t, nil, "-C", repo.source, "rev-parse", "HEAD")))
		repo.blob = strings.TrimSpace(string(git(t, nil, "-C", repo.source, "rev-parse", "HEAD:identity")))
		repo.refs = string(git(t, nil, "ls-remote", "--refs", repo.url))
		if repo.refs != repo.tip+"\trefs/heads/main\n" {
			t.Fatalf("%s published unexpected refs: %s", repo.name, repo.refs)
		}
	}

	// Delete an orphan branch in one repository. Collection must reclaim it
	// without affecting either another SHA-1 repository or the SHA-256 one.
	first := repositories[0]
	git(t, nil, "-C", first.source, "checkout", "--orphan", "discard")
	git(t, nil, "-C", first.source, "rm", "-rf", ".")
	write(t, filepath.Join(first.source, "discard"), []byte("unreachable alpha content"))
	git(t, nil, "-C", first.source, "add", ".")
	git(t, nil, "-C", first.source, "commit", "-m", "discard")
	dead := strings.TrimSpace(string(git(t, nil, "-C", first.source, "rev-parse", "HEAD")))
	git(t, nil, "-C", first.source, "push", first.url, "HEAD:refs/heads/discard")
	git(t, nil, "-C", first.source, "push", first.url, ":refs/heads/discard")
	if finishGitMaintenance(t, first.url) == 0 {
		t.Fatal("isolated collection did not reclaim candidates")
	}

	for i, repo := range repositories {
		if refs := string(git(t, nil, "ls-remote", "--refs", repo.url)); refs != repo.refs {
			t.Fatalf("collection changed %s refs: %s", repo.name, refs)
		}
		cold := filepath.Join(t.TempDir(), "cold")
		git(t, nil, "clone", repo.url, cold)
		git(t, nil, "-C", cold, "fsck", "--strict")
		if head := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", "HEAD"))); head != repo.tip {
			t.Fatalf("%s lost its acknowledged tip", repo.name)
		}
		if branch := strings.TrimSpace(string(git(t, nil, "-C", cold, "symbolic-ref", "--short", "HEAD"))); branch != "main" {
			t.Fatalf("%s default branch is %s", repo.name, branch)
		}
		if format := strings.TrimSpace(string(git(t, nil, "-C", cold, "rev-parse", "--show-object-format"))); format != repo.format {
			t.Fatalf("%s object format is %s", repo.name, format)
		}
		if content := git(t, nil, "-C", cold, "cat-file", "blob", repo.blob); !bytes.Equal(content, []byte(repo.name)) {
			t.Fatalf("%s object content crossed repository boundaries", repo.name)
		}
		if i == 0 {
			rejectedObjectFetch(t, cold, repo.url, dead)
		}
		for j, other := range repositories {
			if i != j && repo.format == other.format {
				rejectedObjectFetch(t, cold, repo.url, other.blob)
			}
		}
	}
}

func repositoryStatus(t *testing.T, url, password string, want int) {
	t.Helper()
	request, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth("git", password)
	response, err := (&http.Client{Timeout: time.Minute}).Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != want {
		t.Fatalf("%s: HTTP %d, want %d", request.URL.Path, response.StatusCode, want)
	}
}

func rejectedObjectFetch(t *testing.T, source, url, id string) {
	t.Helper()
	if output, err := gitCommand("-C", source, "fetch", url, id).CombinedOutput(); err == nil {
		t.Fatalf("repository served unavailable object %s: %s", id, output)
	}
	if output, err := gitCommand("-C", source, "cat-file", "-e", id).CombinedOutput(); err == nil {
		t.Fatalf("unavailable object %s appeared in clone: %s", id, output)
	}
}
