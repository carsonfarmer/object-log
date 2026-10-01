package main

import (
	"crypto/sha256"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
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
type repositoryConfig struct {
	LogID string `json:"-"`
	repositoryAccess
}

// Exact policies override "*". Bare names and their .git URLs identify one log.
func loadRepositories(getenv func(string) string) (map[string]repositoryConfig, error) {
	text := getenv("GIT_REPOSITORIES")
	if len(text) > repositoriesConfigBytes {
		return nil, errors.New("GIT_REPOSITORIES exceeds 64 KiB")
	}
	repositories := map[string]repositoryConfig{}
	if strings.TrimSpace(text) == "" {
		return repositories, nil
	}
	entries := map[string]repositoryConfig{}
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
			for _, group := range groups {
				if group == "" {
					return nil, errors.New("permission groups must be nonempty strings")
				}
			}
		}
		if name != "*" {
			repository.LogID = automaticRepositoryID(name)
		}
		repositories[name] = repository
	}
	return repositories, nil
}

func validRepositoryName(name string) bool {
	if len(name) == 0 || len(canonicalRepositoryName(name)) > 4096 || name == "*" || strings.Contains(name, `\`) {
		return false
	}
	path := &url.URL{Path: name}
	if path.EscapedPath() != name {
		return false
	}
	segments := strings.Split(name, "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	return segments[len(segments)-1] != ".git"
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
	Repository repositoryConfig
	Service    string
	Method     string
	Action     gitAction
}

// Resolve exact permitted names and endpoint suffixes without cleaning or
// decoding path aliases. The caller must authorize Action before opening a WAL.
// A method error preserves Method so the caller can send an Allow header.
func resolveRepository(repositories map[string]repositoryConfig, r *http.Request) (repositoryRoute, error) {
	var route repositoryRoute
	// Validation retains its decoded path and ignores queries.
	if r.URL.Path != "/_validate_backend" && (r.URL.RawPath != "" || r.URL.EscapedPath() != r.URL.Path ||
		r.URL.Fragment != "" || r.URL.Opaque != "" || !strings.HasPrefix(r.URL.Path, "/")) {
		return route, errRepositoryNotFound
	}
	switch r.URL.Path {
	case "/_validate_backend":
		route = repositoryRoute{Service: "validate-backend", Method: http.MethodPost, Action: gitAdmin}
	case "/_maintenance":
		query, err := url.ParseQuery(r.URL.RawQuery)
		id, operation := query.Get("log_id"), query.Get("operation")
		if err != nil || len(query) != 2 || len(query["log_id"]) != 1 || len(query["operation"]) != 1 ||
			!repositoryLogID.MatchString(id) || id == "." || id == ".." ||
			(operation != "maintenance" && operation != "collect") {
			return route, errRepositoryNotFound
		}
		// Empty repository groups restrict discovered IDs to the scope-wide
		// operator in Cognito mode.
		route = repositoryRoute{Repository: repositoryConfig{LogID: id}, Service: operation, Method: http.MethodPost, Action: gitAdmin}
	default:
		endpoint := repositoryEndpoint.FindStringSubmatch(r.URL.Path)
		if endpoint == nil {
			return route, errRepositoryNotFound
		}
		route.Name, route.Service = endpoint[1], endpoint[2]
		if !validRepositoryName(route.Name) {
			return repositoryRoute{}, errRepositoryNotFound
		}
		route.Name = canonicalRepositoryName(route.Name)
		repository, ok := repositories[route.Name]
		if !ok {
			repository, ok = repositories["*"]
			repository.LogID = automaticRepositoryID(route.Name)
		}
		if !ok {
			return repositoryRoute{}, errRepositoryNotFound
		}
		route.Repository = repository
		route.Method = http.MethodPost
		if route.Service == "info/refs" {
			query, err := url.ParseQuery(r.URL.RawQuery)
			services := query["service"]
			if err != nil || len(query) != 1 || len(services) != 1 {
				return repositoryRoute{}, errRepositoryNotFound
			}
			route.Service = services[0]
			route.Method = http.MethodGet
			if route.Service != transport.UploadPackService && route.Service != transport.ReceivePackService {
				return repositoryRoute{}, errRepositoryNotFound
			}
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
		default:
			route.Action = gitAdmin
		}
	}
	if r.Method != route.Method {
		return route, errRepositoryMethod
	}
	return route, nil
}
