package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const (
	authTokenBytes   = 16 << 10
	authJWKSBytes    = 64 << 10
	authFetchTimeout = 5 * time.Second
)

var (
	errAuthRequired    = errors.New("access token required as Basic password or Bearer credential")
	errAuthInvalid     = errors.New("invalid or expired access token")
	errAuthScope       = errors.New("access token lacks required scope")
	errAuthUnavailable = errors.New("authentication keys unavailable")
	errAuthConfig      = errors.New("invalid Cognito issuer, client, operator, scope or transport configuration")
)

func authenticateCognito(
	r *http.Request,
	getenv func(string) string,
	transport http.RoundTripper,
	now func() time.Time,
) (gitPrincipal, error) {
	issuer, clientID := getenv("GIT_COGNITO_ISSUER"), getenv("GIT_COGNITO_CLIENT_ID")
	operatorID, scopes := getenv("GIT_COGNITO_OPERATOR_CLIENT_ID"), strings.Fields(getenv("GIT_COGNITO_SCOPE"))
	endpoint, err := url.ParseRequestURI(issuer)
	invalidIssuer := err != nil || endpoint.Scheme != "https" || endpoint.Host == ""
	invalidClient := clientID == "" || operatorID == clientID
	if invalidIssuer || invalidClient || len(scopes) != 1 || transport == nil {
		return gitPrincipal{}, errAuthConfig
	}
	encoded, err := requestAccessToken(r)
	if err != nil {
		return gitPrincipal{}, err
	}
	if r.Context().Err() != nil {
		return gitPrincipal{}, errAuthUnavailable
	}
	token, err := jwt.ParseSigned(encoded, []jose.SignatureAlgorithm{jose.RS256})
	if err != nil {
		return gitPrincipal{}, errAuthInvalid
	}
	fetchContext, cancel := context.WithTimeout(r.Context(), authFetchTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(fetchContext, http.MethodGet, issuer+"/.well-known/jwks.json", nil)
	if err != nil {
		return gitPrincipal{}, errAuthConfig
	}
	// One synchronous request prevents redirects and background key fetches.
	response, err := transport.RoundTrip(request)
	if err != nil {
		return gitPrincipal{}, errAuthUnavailable
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, authJWKSBytes+1))
	var keys jose.JSONWebKeySet
	if err != nil || fetchContext.Err() != nil || response.StatusCode != http.StatusOK ||
		len(body) > authJWKSBytes || json.Unmarshal(body, &keys) != nil {
		return gitPrincipal{}, errAuthUnavailable
	}
	var claims struct {
		jwt.Claims
		ClientID string   `json:"client_id"`
		TokenUse string   `json:"token_use"`
		Scope    string   `json:"scope"`
		Groups   []string `json:"cognito:groups"`
	}
	if token.Claims(keys, &claims) != nil {
		return gitPrincipal{}, errAuthInvalid
	}
	// Cognito access tokens identify the app by client_id, not ID-token aud.
	operator := operatorID != "" && claims.ClientID == operatorID
	validClient := claims.ClientID == clientID || operator
	current := now()
	validTime := claims.Expiry != nil && current.Before(claims.Expiry.Time())
	if !validTime || claims.ValidateWithLeeway(jwt.Expected{Issuer: issuer, Time: current}, 0) != nil {
		return gitPrincipal{}, errAuthInvalid
	}
	if !validClient || claims.TokenUse != "access" || claims.Subject == "" {
		return gitPrincipal{}, errAuthInvalid
	}
	if !slices.Contains(strings.Fields(claims.Scope), scopes[0]) ||
		(operator && !slices.Contains(strings.Fields(claims.Scope), "git/maintenance")) {
		return gitPrincipal{}, errAuthScope
	}
	return gitPrincipal{subject: claims.Subject, groups: claims.Groups, operator: operator}, nil
}

func requestAccessToken(r *http.Request) (string, error) {
	headers := r.Header.Values("Authorization")
	if len(headers) == 0 {
		return "", errAuthRequired
	}
	if len(headers) != 1 || len(headers[0]) > 2*authTokenBytes {
		return "", errAuthInvalid
	}
	var token string
	scheme, value, _ := strings.Cut(headers[0], " ")
	switch {
	case strings.EqualFold(scheme, "Basic"):
		_, password, ok := r.BasicAuth()
		if !ok {
			return "", errAuthInvalid
		}
		token = password
	case strings.EqualFold(scheme, "Bearer"):
		token = value
	default:
		return "", errAuthInvalid
	}
	if token == "" || len(token) > authTokenBytes {
		return "", errAuthInvalid
	}
	return token, nil
}

type gitAction uint8

const (
	gitRead gitAction = iota
	gitWrite
	gitAdmin
)

type repositoryAccess struct {
	ReadGroups  []string `json:"read_groups"`
	WriteGroups []string `json:"write_groups"`
	AdminGroups []string `json:"admin_groups"`
}

type gitPrincipal struct {
	subject  string
	groups   []string
	operator bool
}

// Actions are independent: write and admin do not imply read. Empty lists deny.
func (p gitPrincipal) Allows(policy repositoryAccess, action gitAction) bool {
	if p.subject == "" || action > gitAdmin {
		return false
	}
	if p.operator {
		return action == gitAdmin
	}
	allowed := [...][]string{policy.ReadGroups, policy.WriteGroups, policy.AdminGroups}[action]
	for _, group := range p.groups {
		if group != "" && slices.Contains(allowed, group) {
			return true
		}
	}
	return false
}
