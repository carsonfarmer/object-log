package main

import (
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/go-git/go-git/v6/plumbing/transport"
)

const repositoriesConfigBytes = 64 << 10

var (
	errRepositoryNotFound = errors.New("repository or Git endpoint not found")
	errRepositoryMethod   = errors.New("method not allowed")
	// Match the core's LogId contract; IDs are storage identities, not paths.
	repositoryLogID    = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	repositoryEndpoint = regexp.MustCompile(
		`^/(.+)/(info/refs|git-upload-pack|git-receive-pack|authorize-read|` +
			`maintenance|collect|recover-retentions-after-drain)$`,
	)
)

// Configuration sets permissions only. The name determines storage identity;
// the first accepted push establishes the format and default branch.
// Exact policies override "*". Bare names and their .git URLs identify one log.
func loadRepositories(getenv func(string) string) (map[string]repositoryAccess, error) {
	text := getenv("GIT_REPOSITORIES")
	if len(text) > repositoriesConfigBytes {
		return nil, errors.New("GIT_REPOSITORIES exceeds 64 KiB")
	}
	repositories := map[string]repositoryAccess{}
	if strings.TrimSpace(text) == "" {
		return repositories, nil
	}
	entries := map[string]repositoryAccess{}
	err := json.Unmarshal(
		[]byte(text),
		&entries,
		json.RejectUnknownMembers(true),
		json.WithUnmarshalers(
			json.UnmarshalFromFunc(func(dec *jsontext.Decoder, _ any) error {
				if dec.PeekKind() == jsontext.KindNull {
					return errors.New("repository configuration must not contain null")
				}
				return errors.ErrUnsupported
			}),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("GIT_REPOSITORIES: %w", err)
	}
	for name, repository := range entries {
		if name != "*" && !validRepositoryName(name) {
			return nil, fmt.Errorf("GIT_REPOSITORIES: noncanonical repository name %q", name)
		}
		if name != "*" {
			name = canonicalRepositoryName(name)
		}
		if _, exists := repositories[name]; exists {
			return nil, fmt.Errorf("duplicate repository name %q", name)
		}
		for _, groups := range [][]string{repository.ReadGroups, repository.WriteGroups, repository.AdminGroups} {
			if slices.Contains(groups, "") {
				return nil, errors.New("permission groups must be nonempty strings")
			}
		}
		repositories[name] = repository
	}
	return repositories, nil
}

func validRepositoryName(name string) bool {
	path := &url.URL{Path: name}
	return fs.ValidPath(name) && name != "." && name != "*" &&
		len(canonicalRepositoryName(name)) <= 4096 && !strings.Contains(name, `\`) &&
		path.EscapedPath() == name && !strings.HasSuffix("/"+name, "/.git")
}

func canonicalRepositoryName(name string) string {
	if !strings.HasSuffix(name, ".git") {
		return name + ".git"
	}
	return name
}

func automaticRepositoryID(name string) string {
	return fmt.Sprintf("auto-%x", sha256.Sum256([]byte(name)))
}

type repositoryRoute struct {
	Name       string
	LogID      string
	Repository repositoryAccess
	Service    string
	Method     string
	Action     gitAction
}

// Resolve exact permitted names and endpoint suffixes without cleaning or
// decoding path aliases. The caller must authorize Action before opening a WAL.
// A method error preserves Method so the caller can send an Allow header.
func resolveRepository(repositories map[string]repositoryAccess, r *http.Request) (repositoryRoute, error) {
	route := repositoryRoute{Method: http.MethodPost, Action: gitAdmin}
	// Validation retains its decoded path and ignores queries.
	if r.URL.Path == "/_validate_backend" {
		route.Service = "validate-backend"
	} else {
		if r.URL.RawPath != "" || r.URL.EscapedPath() != r.URL.Path || r.URL.Fragment != "" ||
			r.URL.Opaque != "" || !strings.HasPrefix(r.URL.Path, "/") {
			return repositoryRoute{}, errRepositoryNotFound
		}
		query, err := url.ParseQuery(r.URL.RawQuery)
		if err != nil {
			return repositoryRoute{}, errRepositoryNotFound
		}
		if r.URL.Path == "/_maintenance" {
			id := query.Get("log_id")
			route.Service = query.Get("operation")
			if len(query) != 2 || len(query["log_id"]) != 1 || len(query["operation"]) != 1 ||
				!repositoryLogID.MatchString(id) || id == "." || id == ".." ||
				(route.Service != "maintenance" && route.Service != "collect") {
				return repositoryRoute{}, errRepositoryNotFound
			}
			// Discovered IDs have no repository groups; Cognito requires the operator.
			route.LogID = id
		} else {
			endpoint := repositoryEndpoint.FindStringSubmatch(r.URL.Path)
			if endpoint == nil || !validRepositoryName(endpoint[1]) {
				return repositoryRoute{}, errRepositoryNotFound
			}
			route.Name, route.Service = canonicalRepositoryName(endpoint[1]), endpoint[2]
			if route.Service == "info/refs" {
				if len(query) != 1 || len(query["service"]) != 1 {
					return repositoryRoute{}, errRepositoryNotFound
				}
				route.Service = query.Get("service")
				if route.Service != transport.UploadPackService && route.Service != transport.ReceivePackService {
					return repositoryRoute{}, errRepositoryNotFound
				}
				route.Method = http.MethodGet
			} else if r.URL.RawQuery != "" || r.URL.ForceQuery {
				return repositoryRoute{}, errRepositoryNotFound
			}
			if route.Service == "authorize-read" {
				route.Method = http.MethodGet
			}
			switch route.Service {
			case transport.UploadPackService, "authorize-read":
				route.Action = gitRead
			case transport.ReceivePackService:
				route.Action = gitWrite
			}
			var ok bool
			route.Repository, ok = repositories[route.Name]
			if !ok {
				route.Repository, ok = repositories["*"]
			}
			if !ok {
				return repositoryRoute{}, errRepositoryNotFound
			}
			route.LogID = automaticRepositoryID(route.Name)
		}
	}
	if r.Method != route.Method {
		return route, errRepositoryMethod
	}
	return route, nil
}
