package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-git/go-git/v6/plumbing/transport"
)

const repositoryTestConfig = `{
	"team/alpha.git": {
		"read_groups": ["readers"], "write_groups": ["writers"], "admin_groups": ["operators"]
	},
	"team/beta.git": {"read_groups": ["beta-readers"]},
	"R&D/Lib+client@v2.git": {}
}`

func repositoriesFromText(text string) (map[string]repositoryAccess, error) {
	return loadRepositories(func(name string) string {
		if name == "GIT_REPOSITORIES" {
			return text
		}
		return ""
	})
}

func repositoriesForTest(t *testing.T) map[string]repositoryAccess {
	t.Helper()
	repositories, err := repositoriesFromText(repositoryTestConfig)
	if err != nil {
		t.Fatal(err)
	}
	return repositories
}

func TestRepositoryNamesDeriveIndependentIdentitiesAndDeferPushMetadata(t *testing.T) {
	t.Parallel()
	repositories := repositoriesForTest(t)
	alpha, err := resolveRepository(repositories, httptest.NewRequest(http.MethodGet, "/team/alpha.git/authorize-read", nil))
	if err != nil {
		t.Fatal(err)
	}
	beta, err := resolveRepository(repositories, httptest.NewRequest(http.MethodGet, "/team/beta.git/authorize-read", nil))
	if err != nil {
		t.Fatal(err)
	}
	if alpha.LogID != automaticRepositoryID("team/alpha.git") ||
		beta.LogID != automaticRepositoryID("team/beta.git") || alpha.LogID == beta.LogID {
		t.Fatal("repository names did not retain independent storage identities")
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
		{name: "duplicate field", text: `{"r.git":{"read_groups":["one"],"read_groups":["two"]}}`},
		{name: "escaped duplicate field", text: `{"r.git":{"read_groups":[],"\u0072ead_groups":["reader"]}}`},
		{name: "case alias field", text: `{"r.git":{"read_groups":[],"READ_GROUPS":["reader"]}}`},
		{name: "unknown field", text: `{"r.git":{"public":true}}`},
		{name: "string groups", text: `{"r.git":{"read_groups":"everyone"}}`},
		{name: "null groups", text: `{"r.git":{"read_groups":null}}`},
		{name: "wrong group type", text: `{"r.git":{"read_groups":[true]}}`},
		{name: "null group element", text: `{"r.git":{"read_groups":[null]}}`},
		{name: "empty read group", text: `{"r.git":{"read_groups":[""]}}`},
		{name: "empty write group", text: `{"r.git":{"write_groups":[""]}}`},
		{name: "empty admin group", text: `{"r.git":{"admin_groups":[""]}}`},
		{name: "size bound", text: strings.Repeat(" ", repositoriesConfigBytes+1)},
		{name: "invalid UTF-8", text: string([]byte{0xff})},
		{name: "invalid UTF-8 group", text: `{"r.git":{"read_groups":["` + string([]byte{0xff}) + `"]}}`},
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

func TestLoadRepositoriesRejectsDiscardedMetadataFields(t *testing.T) {
	t.Parallel()
	for _, field := range []string{`"log_id":"custom"`, `"format":"sha1"`, `"default_branch":"release"`} {
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			if _, err := repositoriesFromText(`{"r.git":{` + field + `}}`); err == nil {
				t.Fatal("discarded configuration field was silently accepted")
			}
		})
	}
}

func TestLoadRepositoriesRejectsNoncanonicalNames(t *testing.T) {
	t.Parallel()
	for _, name := range []string{
		"", ".", ".git", "team/.git", "/r.git", "r.git/", "team//r.git", "./r.git",
		"team/./r.git", "team/../r.git", "../r.git", "r%2egit", `team\r.git`, "r?.git", "r#.git",
		"r space.git", "résumé.git",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			encoded, err := json.Marshal(map[string]repositoryAccess{
				name: {ReadGroups: []string{}, WriteGroups: []string{}, AdminGroups: []string{}},
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

func TestRepositoryNameBoundaries(t *testing.T) {
	for _, name := range []string{"...git", "team/...git", "team/a:b+c&d=e.git", strings.Repeat("a", 4092), strings.Repeat("a", 4092) + ".git"} {
		if !validRepositoryName(name) {
			t.Fatalf("rejected canonical repository name %q", name)
		}
	}
	for _, name := range []string{strings.Repeat("a", 4093), strings.Repeat("a", 4093) + ".git", "team/../r", "team/./r", "team\x00r"} {
		if validRepositoryName(name) {
			t.Fatalf("accepted noncanonical or oversized name %q", name)
		}
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
		"new/nested.git":{}
	}`
	repositories, err := repositoriesFromText(text)
	if err != nil {
		t.Fatal(err)
	}
	route, err := resolveRepository(repositories, request)
	if err != nil || route.LogID != automaticRepositoryID("new/nested.git") {
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
		{name: "read permission", method: http.MethodGet, path: "authorize-read", service: "authorize-read", action: gitRead},
		{name: "read RPC", method: http.MethodPost, path: "git-upload-pack",
			service: transport.UploadPackService, action: gitRead},
		{name: "write RPC", method: http.MethodPost, path: "git-receive-pack",
			service: transport.ReceivePackService, action: gitWrite},
		{name: "logical maintenance", method: http.MethodPost, path: "maintenance", service: "maintenance", action: gitAdmin},
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
			if route.Name != "team/alpha.git" || route.LogID != automaticRepositoryID("team/alpha.git") {
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
		"/team/alpha.git/unknown", "/team/alpha.git/create", "/team/alpha.git/git-upload-pack/", "/team/alpha.git//git-upload-pack",
		"//team/alpha.git/git-upload-pack", "/team/./alpha.git/git-upload-pack",
		"/team/other/../alpha.git/git-upload-pack", "/team%2falpha.git/git-upload-pack",
		"/team/%61lpha.git/git-upload-pack", "/team/alpha%2egit/git-upload-pack",
		"/team/alpha.git/git-upload-pack?", "/team/alpha.git/git-upload-pack?service=git-receive-pack",
		"/team/alpha.git/info/refs", "/team/alpha.git/info/refs?service=unknown",
		"/team/alpha.git/info/refs?service=git-upload-pack&service=git-receive-pack",
		"/team/alpha.git/info/refs?service=git-upload-pack&extra=1",
		"/team/alpha.git/info/refs?service=git-upload-pack&bad=%ZZ",
		"/team/alpha.git/authorize-read?", "/team/alpha.git/authorize-read?service=git-upload-pack",
		"/missing.git/authorize-read",
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

func TestReadPermissionMethodErrorPreservesAction(t *testing.T) {
	t.Parallel()
	for _, method := range []string{http.MethodPost, http.MethodHead} {
		route, err := resolveRepository(repositoriesForTest(t), httptest.NewRequest(method, "/team/alpha/authorize-read", nil))
		if !errors.Is(err, errRepositoryMethod) || route.Method != http.MethodGet || route.Action != gitRead || route.Name != "team/alpha.git" {
			t.Fatalf("route=%+v error=%v", route, err)
		}
	}
}

func TestDiscoveredLogMaintenanceRequiresScopeOperator(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"maintenance", "collect"} {
		request := httptest.NewRequest(http.MethodPost, "/_maintenance?log_id=auto-123&operation="+operation, nil)
		route, err := resolveRepository(nil, request)
		if err != nil || route.LogID != "auto-123" || route.Service != operation || route.Action != gitAdmin {
			t.Fatalf("route=%+v error=%v", route, err)
		}
		claims := authTestClaims(time.Now())
		claims.Groups = []string{"operators"}
		for _, subject := range []string{"", "repository-admin"} {
			claims.Subject = subject
			if status, err := authorizeForTest(t, route, claims); status == 0 || err == nil {
				t.Fatal("discovered log bypassed repository policy without a scope operator")
			}
		}
		claims.Subject, claims.ClientID, claims.Scope = "scheduler", "maintenance-client", "git/access git/maintenance"
		if status, err := authorizeForTest(t, route, claims); status != 0 || err != nil {
			t.Fatalf("scope operator cannot maintain a discovered log: status=%d error=%v", status, err)
		}
		request.Method = http.MethodGet
		if route, err := resolveRepository(nil, request); !errors.Is(err, errRepositoryMethod) || route.Action != gitAdmin {
			t.Fatalf("read-only method route=%+v error=%v", route, err)
		}
	}
	for _, query := range []string{
		"", "log_id=auto-123", "log_id=auto-123&operation=read",
		"log_id=auto-123&operation=maintenance&extra=1",
		"log_id=auto-123&log_id=other&operation=maintenance",
		"log_id=auto-123&operation=maintenance&operation=collect",
		"log_id=..&operation=maintenance", "log_id=a%2Fb&operation=collect",
		"log_id=&operation=maintenance", "log_id=%ZZ&operation=maintenance",
	} {
		if _, err := resolveRepository(nil, httptest.NewRequest(http.MethodPost, "/_maintenance?"+query, nil)); !errors.Is(err, errRepositoryNotFound) {
			t.Fatalf("query %q error=%v", query, err)
		}
	}
	// A repository with this name still has ordinary Git routes and permissions.
	route, err := resolveRepository(map[string]repositoryAccess{"_maintenance.git": {}},
		httptest.NewRequest(http.MethodGet, "/_maintenance/info/refs?service=git-upload-pack", nil))
	if err != nil || route.LogID != automaticRepositoryID("_maintenance.git") || route.Action != gitRead {
		t.Fatalf("named repository route=%+v error=%v", route, err)
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
			claims := authTestClaims(time.Now())
			claims.Groups = []string{tt.group}
			status, err := authorizeForTest(t, route, claims)
			if (status == 0 && err == nil) != tt.want {
				t.Fatalf("authorization: status=%d error=%v, want allowed=%v", status, err, tt.want)
			}
		})
	}
}

func TestAutomaticRepositoryPolicyAndIdentity(t *testing.T) {
	if !validRepositoryName(strings.Repeat("a", 4092)) || validRepositoryName(strings.Repeat("a", 4093)) {
		t.Fatal("name limit must include the optional .git suffix")
	}
	repositories, err := repositoriesFromText(`{"*":{"write_groups":["writers"]},"team/private":{}}`)
	if err != nil {
		t.Fatal(err)
	}
	var first repositoryRoute
	for _, name := range []string{"team/project", "team/project.git"} {
		route, err := resolveRepository(repositories, httptest.NewRequest(http.MethodPost, "/"+name+"/git-receive-pack", nil))
		if err != nil {
			t.Fatal(err)
		}
		if route.Name != "team/project.git" || route.Action != gitWrite {
			t.Fatalf("unexpected automatic route: %+v", route)
		}
		if first.Name != "" && first.LogID != route.LogID {
			t.Fatal("URL aliases select different logs")
		}
		first = route
	}
	claims := authTestClaims(time.Now())
	claims.Groups = []string{"writers"}
	if status, err := authorizeForTest(t, first, claims); status != 0 || err != nil {
		t.Fatalf("automatic write policy: status=%d error=%v", status, err)
	}
	read := first
	read.Action = gitRead
	if status, err := authorizeForTest(t, read, claims); status != http.StatusForbidden || err == nil {
		t.Fatalf("automatic policy lost independent permissions: status=%d error=%v", status, err)
	}
	private, err := resolveRepository(repositories, httptest.NewRequest(http.MethodPost, "/team/private.git/git-receive-pack", nil))
	if err != nil {
		t.Fatal(err)
	}
	if status, err := authorizeForTest(t, private, claims); status != http.StatusForbidden || err == nil || private.LogID == first.LogID {
		t.Fatal("exact denial or repository isolation lost")
	}
	for _, name := range []string{"*", "team/../project", "team//project", "%70roject"} {
		if _, err := resolveRepository(repositories, httptest.NewRequest(http.MethodPost, "/"+name+"/git-receive-pack", nil)); err == nil {
			t.Fatalf("wildcard accepted invalid name %q", name)
		}
	}
	// Adding a permission override must preserve the automatically created log.
	override, err := repositoriesFromText(`{"team/project":{"read_groups":["readers"]}}`)
	if err != nil {
		t.Fatal(err)
	}
	overridden, err := resolveRepository(override, httptest.NewRequest(http.MethodGet, "/team/project.git/authorize-read", nil))
	if err != nil || overridden.LogID != first.LogID {
		t.Fatal("permission override moved storage")
	}
	for _, text := range []string{
		`{"*":{"log_id":"shared"}}`,
		`{"project":{},"project.git":{}}`,
		`{"project":{"log_id":"auto-forbidden"}}`,
	} {
		if _, err := repositoriesFromText(text); err == nil {
			t.Fatalf("accepted ambiguous policy: %s", text)
		}
	}
}

func TestBackendValidationIsGlobalAdministration(t *testing.T) {
	for _, path := range []string{"/_validate_backend", "/_validate_backend?", "/_validate_backend?bad=%ZZ", "/%5fvalidate_backend"} {
		for _, method := range []string{http.MethodPost, http.MethodGet, http.MethodHead} {
			route, err := resolveRepository(nil, httptest.NewRequest(method, path, nil))
			want := error(nil)
			if method != http.MethodPost {
				want = errRepositoryMethod
			}
			if !errors.Is(err, want) || route.Service != "validate-backend" || route.Method != http.MethodPost ||
				route.Action != gitAdmin || route.Name != "" || route.LogID != "" {
				t.Fatalf("%s %s route=%+v error=%v", method, path, route, err)
			}
			claims := authTestClaims(time.Now())
			claims.Groups = []string{"operators"}
			if status, err := authorizeForTest(t, route, claims); status != http.StatusForbidden || err == nil {
				t.Fatal("global validation inherited repository permissions")
			}
			claims.ClientID, claims.Scope = "maintenance-client", "git/access git/maintenance"
			if status, err := authorizeForTest(t, route, claims); status != 0 || err != nil {
				t.Fatalf("scope operator cannot validate the backend: status=%d error=%v", status, err)
			}
		}
	}
	for _, path := range []string{"/_validate_backend/", "//_validate_backend", "/_validate_backend/authorize-read"} {
		if _, err := resolveRepository(nil, httptest.NewRequest(http.MethodPost, path, nil)); !errors.Is(err, errRepositoryNotFound) {
			t.Fatalf("accepted validation alias %s: %v", path, err)
		}
	}
}
