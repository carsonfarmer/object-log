package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/transport"
)

const repositoriesConfigBytes = 64 << 10

var (
	errRepositoryNotFound = errors.New("repository or Git endpoint not found")
	errRepositoryMethod   = errors.New("method not allowed")
	// Match the core's LogId contract; IDs are storage identities, not paths.
	repositoryLogID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
)

// A map entry provisions a name. Opening or creating storage remains the caller's
// responsibility, after checking the route's action against repositoryAccess.
type repositoryConfig struct {
	LogID         string              `json:"log_id"`
	Format        config.ObjectFormat `json:"format"`
	DefaultBranch string              `json:"default_branch,omitempty"`
	repositoryAccess
}

// GIT_REPOSITORIES is an object keyed by canonical names such as team/project.git.
// Missing configuration provides no repositories. There are no implicit aliases.
func loadRepositories(getenv func(string) string) (map[string]repositoryConfig, error) {
	text := getenv("GIT_REPOSITORIES")
	if len(text) > repositoriesConfigBytes || !utf8.ValidString(text) {
		return nil, errors.New("GIT_REPOSITORIES exceeds 64 KiB or contains invalid UTF-8")
	}
	repositories := map[string]repositoryConfig{}
	if strings.TrimSpace(text) == "" {
		return repositories, nil
	}
	entries, err := uniqueRepositoryObject([]byte(text))
	if err != nil {
		return nil, fmt.Errorf("GIT_REPOSITORIES: %w", err)
	}
	identities := map[string]string{}
	for name, encoded := range entries {
		if !validRepositoryName(name) {
			return nil, fmt.Errorf("GIT_REPOSITORIES: noncanonical repository name %q", name)
		}
		repository, err := parseRepository(encoded)
		if err != nil {
			return nil, fmt.Errorf("GIT_REPOSITORIES: repository %q: %w", name, err)
		}
		if other, exists := identities[repository.LogID]; exists {
			return nil, fmt.Errorf("repositories %q and %q share log_id %q", other, name, repository.LogID)
		}
		identities[repository.LogID] = name
		repositories[name] = repository
	}
	return repositories, nil
}

func parseRepository(encoded []byte) (repositoryConfig, error) {
	var repository repositoryConfig
	fields, err := uniqueRepositoryObject(encoded)
	if err != nil {
		return repository, err
	}
	for name, value := range fields {
		if bytes.Equal(value, []byte("null")) {
			return repository, fmt.Errorf("%s must not be null", name)
		}
		switch name {
		case "log_id", "format", "default_branch", "read_groups", "write_groups", "admin_groups":
		default:
			return repository, fmt.Errorf("unknown field %q", name)
		}
	}
	if err := json.Unmarshal(encoded, &repository); err != nil {
		return repository, err
	}
	validID := repositoryLogID.MatchString(repository.LogID)
	if !validID || repository.LogID == "." || repository.LogID == ".." {
		return repository, errors.New("log_id must satisfy the WAL log identifier contract")
	}
	if repository.Format != config.SHA1 && repository.Format != config.SHA256 {
		return repository, config.ErrInvalidObjectFormat
	}
	if repository.DefaultBranch != "" {
		if err := plumbing.ValidateBranchName(repository.DefaultBranch); err != nil {
			return repository, fmt.Errorf("default_branch: %w", err)
		}
	}
	for _, groups := range [][]string{repository.ReadGroups, repository.WriteGroups, repository.AdminGroups} {
		for _, group := range groups {
			if group == "" {
				return repository, errors.New("permission groups must be nonempty strings")
			}
		}
	}
	return repository, nil
}

// Decode only an object, rejecting duplicate keys before normal JSON decoding
// could silently overwrite them. Both repository names and fields use this path.
func uniqueRepositoryObject(encoded []byte) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("expected a JSON object")
	}
	fields := map[string]json.RawMessage{}
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, err
		}
		name, ok := key.(string)
		if !ok {
			return nil, errors.New("expected an object key")
		}
		if _, exists := fields[name]; exists {
			return nil, fmt.Errorf("duplicate key %q", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		fields[name] = value
	}
	if _, err := decoder.Token(); err != nil {
		return nil, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("unexpected trailing JSON")
	}
	return fields, nil
}

func validRepositoryName(name string) bool {
	if !strings.HasSuffix(name, ".git") || strings.Contains(name, `\`) {
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

type repositoryRoute struct {
	Name       string
	Repository repositoryConfig
	Service    string
	Method     string
	Action     gitAction
}

// Resolve exact provisioned names and endpoint suffixes without cleaning or
// decoding path aliases. The caller must authorize Action before opening a WAL.
// A method error preserves Method so the caller can send an Allow header.
func resolveRepository(repositories map[string]repositoryConfig, r *http.Request) (repositoryRoute, error) {
	var route repositoryRoute
	if r.URL.RawPath != "" || r.URL.EscapedPath() != r.URL.Path {
		return route, errRepositoryNotFound
	}
	if r.URL.Fragment != "" || r.URL.Opaque != "" || !strings.HasPrefix(r.URL.Path, "/") {
		return route, errRepositoryNotFound
	}
	for _, service := range []string{
		"info/refs", transport.UploadPackService, transport.ReceivePackService,
		"_browse",
		"maintenance", "collect", "prune-invalid-refs", "recover-retentions-after-drain",
	} {
		suffix := "/" + service
		if strings.HasSuffix(r.URL.Path, suffix) {
			route.Name = strings.TrimPrefix(strings.TrimSuffix(r.URL.Path, suffix), "/")
			route.Service = service
			break
		}
	}
	repository, ok := repositories[route.Name]
	if !ok || !validRepositoryName(route.Name) {
		return repositoryRoute{}, errRepositoryNotFound
	}
	route.Repository = repository
	route.Method = http.MethodPost
	if route.Service == "_browse" {
		route.Method = http.MethodGet
	} else if route.Service == "info/refs" {
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
	} else if route.Service != "_browse" && (r.URL.RawQuery != "" || r.URL.ForceQuery) {
		return repositoryRoute{}, errRepositoryNotFound
	}
	switch route.Service {
	case transport.UploadPackService, "_browse":
		route.Action = gitRead
	case transport.ReceivePackService:
		route.Action = gitWrite
	default:
		route.Action = gitAdmin
	}
	if r.Method != route.Method {
		return route, errRepositoryMethod
	}
	return route, nil
}
