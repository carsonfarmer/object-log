package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/transport"
)

const repositoryTestConfig = `{
	"team/alpha.git": {
		"log_id": "alpha", "format": "sha1", "default_branch": "release/v1",
		"read_groups": ["readers"], "write_groups": ["writers"], "admin_groups": ["operators"]
	},
	"team/beta.git": {"log_id": "beta", "format": "sha1", "read_groups": ["beta-readers"]},
	"R&D/Lib+client@v2.git": {"log_id": "third", "format": "sha256"}
}`

func repositoriesFromText(text string) (map[string]repositoryConfig, error) {
	return loadRepositories(func(name string) string {
		if name == "GIT_REPOSITORIES" {
			return text
		}
		return ""
	})
}

func repositoriesForTest(t *testing.T) map[string]repositoryConfig {
	t.Helper()
	repositories, err := repositoriesFromText(repositoryTestConfig)
	if err != nil {
		t.Fatal(err)
	}
	return repositories
}

func TestLoadRepositoriesIsolatesSameFormatRepositories(t *testing.T) {
	t.Parallel()
	repositories := repositoriesForTest(t)
	alpha, beta := repositories["team/alpha.git"], repositories["team/beta.git"]
	if alpha.Format != config.SHA1 || beta.Format != config.SHA1 || alpha.LogID == beta.LogID {
		t.Fatal("same-format repositories did not retain independent identities")
	}
	if alpha.DefaultBranch != "release/v1" || beta.DefaultBranch != "" {
		t.Fatal("per-repository default branches were not preserved")
	}
	if repositories["R&D/Lib+client@v2.git"].Format != config.SHA256 {
		t.Fatal("canonical URL-safe punctuation or SHA-256 was rejected")
	}
	if _, exists := repositories["sha1.git"]; exists {
		t.Fatal("configuration introduced an implicit repository")
	}
}

func TestLoadRepositoriesRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		text string
	}{
		{name: "not object", text: `[]`},
		{name: "null root", text: `null`},
		{name: "null entry", text: `{"r.git":null}`},
		{name: "malformed", text: `{"r.git":`},
		{name: "trailing document", text: `{} {}`},
		{name: "duplicate name", text: `{"r.git":{},"r.git":{}}`},
		{name: "escaped duplicate name", text: `{"r.git":{},"\u0072.git":{}}`},
		{name: "duplicate field", text: `{"r.git":{"log_id":"one","log_id":"two","format":"sha1"}}`},
		{name: "escaped duplicate field", text: `{"r.git":{"format":"sha1","\u0066ormat":"sha256"}}`},
		{name: "case alias field", text: `{"r.git":{"log_id":"one","LOG_ID":"two","format":"sha1"}}`},
		{name: "unknown field", text: `{"r.git":{"log_id":"one","format":"sha1","public":true}}`},
		{name: "duplicate identity", text: `{
			"a.git":{"log_id":"same","format":"sha1"},
			"b.git":{"log_id":"same","format":"sha256"}
		}`},
		{name: "missing identity", text: `{"r.git":{"format":"sha1"}}`},
		{name: "missing format", text: `{"r.git":{"log_id":"one"}}`},
		{name: "invalid format", text: `{"r.git":{"log_id":"one","format":"SHA1"}}`},
		{name: "null format", text: `{"r.git":{"log_id":"one","format":null}}`},
		{name: "path identity", text: `{"r.git":{"log_id":"a/b","format":"sha1"}}`},
		{name: "dot identity", text: `{"r.git":{"log_id":"..","format":"sha1"}}`},
		{name: "long identity", text: `{"r.git":{"log_id":"` + strings.Repeat("a", 129) + `","format":"sha1"}}`},
		{name: "invalid branch", text: `{"r.git":{"log_id":"one","format":"sha1","default_branch":"a..b"}}`},
		{name: "HEAD branch", text: `{"r.git":{"log_id":"one","format":"sha1","default_branch":"HEAD"}}`},
		{name: "hyphen branch", text: `{"r.git":{"log_id":"one","format":"sha1","default_branch":"-bad"}}`},
		{name: "null branch", text: `{"r.git":{"log_id":"one","format":"sha1","default_branch":null}}`},
		{name: "string groups", text: `{"r.git":{"log_id":"one","format":"sha1","read_groups":"everyone"}}`},
		{name: "null groups", text: `{"r.git":{"log_id":"one","format":"sha1","read_groups":null}}`},
		{name: "wrong group type", text: `{"r.git":{"log_id":"one","format":"sha1","read_groups":[true]}}`},
		{name: "null group element", text: `{"r.git":{"log_id":"one","format":"sha1","read_groups":[null]}}`},
		{name: "size bound", text: strings.Repeat(" ", repositoriesConfigBytes+1)},
		{name: "invalid UTF-8", text: string([]byte{0xff})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if _, err := repositoriesFromText(tt.text); err == nil {
				t.Fatal("invalid configuration was accepted")
			}
		})
	}
}

func TestLoadRepositoriesRejectsNoncanonicalNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"", "r", ".git", "team/.git", "/r.git", "r.git/", "team//r.git", "./r.git",
		"team/./r.git", "team/../r.git", "../r.git", "r%2egit", `team\r.git`, "r?.git", "r#.git",
		"r space.git", "résumé.git",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(map[string]repositoryConfig{
				name: {LogID: "one", Format: config.SHA1, repositoryAccess: repositoryAccess{
					ReadGroups: []string{}, WriteGroups: []string{}, AdminGroups: []string{},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repositoriesFromText(string(encoded)); err == nil {
				t.Fatal("noncanonical name was accepted")
			}
		})
	}
}

func TestLoadRepositoriesEmptyConfigDeniesAll(t *testing.T) {
	t.Parallel()
	for _, text := range []string{"", " ", "{}"} {
		t.Run("config="+text, func(t *testing.T) {
			t.Parallel()
			repositories, err := repositoriesFromText(text)
			if err != nil || len(repositories) != 0 {
				t.Fatalf("repositories=%v error=%v", repositories, err)
			}
			request := httptest.NewRequest(http.MethodGet, "/sha1.git/info/refs?service=git-upload-pack", nil)
			if _, err := resolveRepository(repositories, request); !errors.Is(err, errRepositoryNotFound) {
				t.Fatalf("empty configuration route error = %v", err)
			}
		})
	}
}

func TestAddRepositoryRequiresOnlyConfiguration(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodGet, "/new/nested.git/info/refs?service=git-upload-pack", nil)
	if _, err := resolveRepository(repositoriesForTest(t), request); !errors.Is(err, errRepositoryNotFound) {
		t.Fatalf("unprovisioned repository route error = %v", err)
	}
	text := strings.TrimSuffix(repositoryTestConfig, "}") + `,
		"new/nested.git":{"log_id":"new-stable-id","format":"sha1"}
	}`
	repositories, err := repositoriesFromText(text)
	if err != nil {
		t.Fatal(err)
	}
	route, err := resolveRepository(repositories, request)
	if err != nil || route.Repository.LogID != "new-stable-id" {
		t.Fatalf("configured route=%+v error=%v", route, err)
	}
}

func TestResolveRepositorySelectsServiceAndAction(t *testing.T) {
	t.Parallel()
	repositories := repositoriesForTest(t)
	tests := []struct {
		name    string
		method  string
		path    string
		service string
		action  gitAction
	}{
		{name: "read discovery", method: http.MethodGet, path: "info/refs?service=git-upload-pack",
			service: transport.UploadPackService, action: gitRead},
		{name: "write discovery", method: http.MethodGet, path: "info/refs?service=git-receive-pack",
			service: transport.ReceivePackService, action: gitWrite},
		{name: "read RPC", method: http.MethodPost, path: "git-upload-pack",
			service: transport.UploadPackService, action: gitRead},
		{name: "write RPC", method: http.MethodPost, path: "git-receive-pack",
			service: transport.ReceivePackService, action: gitWrite},
		{name: "logical maintenance", method: http.MethodPost, path: "maintenance", service: "maintenance", action: gitAdmin},
		{name: "legacy ref cleanup", method: http.MethodPost, path: "prune-invalid-refs", service: "prune-invalid-refs", action: gitAdmin},
		{name: "physical collection", method: http.MethodPost, path: "collect", service: "collect", action: gitAdmin},
		{name: "retention recovery", method: http.MethodPost, path: "recover-retentions-after-drain",
			service: "recover-retentions-after-drain", action: gitAdmin},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			route, err := resolveRepository(repositories, httptest.NewRequest(tt.method, "/team/alpha.git/"+tt.path, nil))
			if err != nil {
				t.Fatal(err)
			}
			if route.Name != "team/alpha.git" || route.Repository.LogID != "alpha" {
				t.Fatalf("wrong repository: %+v", route)
			}
			if route.Service != tt.service || route.Method != tt.method || route.Action != tt.action {
				t.Fatalf("wrong operation: %+v", route)
			}
		})
	}
}

func TestResolveRepositoryRejectsAliasesAndUnknownRoutes(t *testing.T) {
	t.Parallel()
	repositories := repositoriesForTest(t)
	for _, path := range []string{
		"/missing.git/git-upload-pack", "/sha1.git/git-upload-pack", "/team/alpha.git",
		"/team/alpha.git/unknown", "/team/alpha.git/git-upload-pack/", "/team/alpha.git//git-upload-pack",
		"//team/alpha.git/git-upload-pack", "/team/./alpha.git/git-upload-pack",
		"/team/other/../alpha.git/git-upload-pack", "/team%2falpha.git/git-upload-pack",
		"/team/%61lpha.git/git-upload-pack", "/team/alpha%2egit/git-upload-pack",
		"/team/alpha.git/git-upload-pack?", "/team/alpha.git/git-upload-pack?service=git-receive-pack",
		"/team/alpha.git/info/refs", "/team/alpha.git/info/refs?service=unknown",
		"/team/alpha.git/info/refs?service=git-upload-pack&service=git-receive-pack",
		"/team/alpha.git/info/refs?service=git-upload-pack&extra=1",
		"/team/alpha.git/info/refs?service=git-upload-pack&bad=%ZZ",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			if _, err := resolveRepository(repositories, httptest.NewRequest(http.MethodPost, path, nil)); !errors.Is(err, errRepositoryNotFound) {
				t.Fatalf("route error = %v, want not found", err)
			}
		})
	}
}

func TestResolveRepositoryMethodErrorPreservesWriteDiscoveryAction(t *testing.T) {
	t.Parallel()
	request := httptest.NewRequest(http.MethodHead, "/team/alpha.git/info/refs?service=git-receive-pack", nil)
	route, err := resolveRepository(repositoriesForTest(t), request)
	if !errors.Is(err, errRepositoryMethod) || route.Method != http.MethodGet || route.Action != gitWrite {
		t.Fatalf("route=%+v error=%v", route, err)
	}
}

func TestRepositoryRoutePolicyIsIndependentPerRepositoryAndAction(t *testing.T) {
	t.Parallel()
	repositories := repositoriesForTest(t)
	tests := []struct {
		name   string
		group  string
		path   string
		method string
		want   bool
	}{
		{name: "reader alpha", group: "readers", path: "team/alpha.git/git-upload-pack", want: true},
		{name: "reader beta denied", group: "readers", path: "team/beta.git/git-upload-pack"},
		{name: "writer discovery", group: "writers", path: "team/alpha.git/info/refs?service=git-receive-pack",
			method: http.MethodGet, want: true},
		{name: "reader cannot discover write", group: "readers", path: "team/alpha.git/info/refs?service=git-receive-pack",
			method: http.MethodGet},
		{name: "writer receive", group: "writers", path: "team/alpha.git/git-receive-pack", want: true},
		{name: "writer read denied", group: "writers", path: "team/alpha.git/git-upload-pack"},
		{name: "writer admin denied", group: "writers", path: "team/alpha.git/maintenance"},
		{name: "admin maintenance", group: "operators", path: "team/alpha.git/maintenance", want: true},
		{name: "admin ref cleanup", group: "operators", path: "team/alpha.git/prune-invalid-refs", want: true},
		{name: "admin collection", group: "operators", path: "team/alpha.git/collect", want: true},
		{name: "admin recovery", group: "operators", path: "team/alpha.git/recover-retentions-after-drain", want: true},
		{name: "admin read denied", group: "operators", path: "team/alpha.git/git-upload-pack"},
		{name: "admin beta denied", group: "operators", path: "team/beta.git/maintenance"},
		{name: "empty policy denied", group: "readers", path: "R&D/Lib+client@v2.git/git-upload-pack"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			method := tt.method
			if method == "" {
				method = http.MethodPost
			}
			route, err := resolveRepository(repositories, httptest.NewRequest(method, "/"+tt.path, nil))
			if err != nil {
				t.Fatal(err)
			}
			principal := gitPrincipal{subject: "person", groups: []string{tt.group}}
			if got := principal.Allows(route.Repository.repositoryAccess, route.Action); got != tt.want {
				t.Fatalf("permission = %v, want %v", got, tt.want)
			}
		})
	}
}
