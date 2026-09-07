package tests

import (
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
)

func TestAccess(t *testing.T) {
	endpoint := strings.TrimRight(os.Getenv("GIT_PROBE_URL"), "/")
	if endpoint == "" {
		t.Skip("set GIT_PROBE_URL")
	}
	password := os.Getenv("GIT_PROBE_PASSWORD")
	receiveStatus := http.StatusOK
	if os.Getenv("GIT_PROBE_READ_ONLY") == "true" {
		receiveStatus = http.StatusForbidden
	}
	type request struct {
		method, path, password string
		status                 int
	}
	cases := []request{
		{"GET", "/sha1.git/info/refs?service=git-upload-pack", password, 200},
		{"GET", "/sha1.git/info/refs?service=git-receive-pack", password, receiveStatus},
		{"POST", "/sha1.git/info/refs?service=git-upload-pack", password, 405},
		{"GET", "/sha1.git/git-upload-pack", password, 405},
		{"GET", "/unknown.git/info/refs?service=git-upload-pack", password, 404},
		{"GET", "/sha256.git-extra/info/refs?service=git-upload-pack", password, 404},
		{"GET", "/sha1.git/info/refs?service=unknown", password, 404},
	}
	if password != "" {
		cases = append(cases, request{"GET", "/sha1.git/info/refs?service=git-upload-pack", "wrong", 401})
	}
	for _, c := range cases {
		r, err := http.NewRequest(c.method, endpoint+c.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		r.SetBasicAuth("git", c.password)
		response, err := http.DefaultClient.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		_, err = io.Copy(io.Discard, response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != c.status {
			t.Errorf("%s %s: got %d, want %d", c.method, c.path, response.StatusCode, c.status)
		}
	}
}

// Run again after restarting Spin with a different default-branch setting.
func TestPersistedHead(t *testing.T) {
	endpoint, branch := os.Getenv("GIT_PROBE_URL"), os.Getenv("GIT_PROBE_BRANCH")
	if endpoint == "" || branch == "" || os.Getenv("GIT_PROBE_PERSISTED_HEAD") != "true" {
		t.Skip("set GIT_PROBE_URL and GIT_PROBE_BRANCH for an initialized repository")
	}
	for _, format := range []string{"sha1", "sha256"} {
		refs := string(git(t, nil, "ls-remote", "--symref", strings.TrimRight(endpoint, "/")+"/"+format+".git", "HEAD"))
		if !strings.Contains(refs, "ref: refs/heads/"+branch+"\tHEAD") {
			t.Fatalf("%s default branch changed: %s", format, refs)
		}
	}
}
