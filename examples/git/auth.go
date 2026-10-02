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

func authorizeCognito(
	r *http.Request,
	route repositoryRoute,
	getenv func(string) string,
	transport http.RoundTripper,
	now func() time.Time,
) (int, error) {
	issuer, clientID := getenv("GIT_COGNITO_ISSUER"), getenv("GIT_COGNITO_CLIENT_ID")
	operatorID, scopes := getenv("GIT_COGNITO_OPERATOR_CLIENT_ID"), strings.Fields(getenv("GIT_COGNITO_SCOPE"))
	endpoint, err := url.ParseRequestURI(issuer)
	invalidIssuer := err != nil || endpoint.Scheme != "https" || endpoint.Host == ""
	invalidClient := clientID == "" || operatorID == clientID
	if invalidIssuer || invalidClient || len(scopes) != 1 || transport == nil {
		return http.StatusInternalServerError, errAuthConfig
	}
	encoded, err := requestAccessToken(r)
	if err != nil {
		return http.StatusUnauthorized, err
	}
	if r.Context().Err() != nil {
		return http.StatusServiceUnavailable, errAuthUnavailable
	}
	token, err := jwt.ParseSigned(encoded, []jose.SignatureAlgorithm{jose.RS256})
	if err != nil {
		return http.StatusUnauthorized, errAuthInvalid
	}
	fetchContext, cancel := context.WithTimeout(r.Context(), authFetchTimeout)
	defer cancel()
	request, err := http.NewRequestWithContext(fetchContext, http.MethodGet, issuer+"/.well-known/jwks.json", nil)
	if err != nil {
		return http.StatusInternalServerError, errAuthConfig
	}
	// One synchronous request prevents redirects and background key fetches.
	response, err := transport.RoundTrip(request)
	if err != nil {
		return http.StatusServiceUnavailable, errAuthUnavailable
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, authJWKSBytes+1))
	var keys jose.JSONWebKeySet
	if err != nil || fetchContext.Err() != nil || response.StatusCode != http.StatusOK ||
		len(body) > authJWKSBytes || json.Unmarshal(body, &keys) != nil {
		return http.StatusServiceUnavailable, errAuthUnavailable
	}
	var claims struct {
		jwt.Claims
		ClientID string   `json:"client_id"`
		TokenUse string   `json:"token_use"`
		Scope    string   `json:"scope"`
		Groups   []string `json:"cognito:groups"`
	}
	if token.Claims(keys, &claims) != nil {
		return http.StatusUnauthorized, errAuthInvalid
	}
	// Cognito access tokens identify the app by client_id, not ID-token aud.
	operator := operatorID != "" && claims.ClientID == operatorID
	current := now()
	if claims.Expiry == nil || !current.Before(claims.Expiry.Time()) ||
		claims.ValidateWithLeeway(jwt.Expected{Issuer: issuer, Time: current}, 0) != nil ||
		(claims.ClientID != clientID && !operator) || claims.TokenUse != "access" || claims.Subject == "" {
		return http.StatusUnauthorized, errAuthInvalid
	}
	if !slices.Contains(strings.Fields(claims.Scope), scopes[0]) ||
		(operator && !slices.Contains(strings.Fields(claims.Scope), "git/maintenance")) {
		return http.StatusForbidden, errAuthScope
	}
	// Actions are independent: write and admin do not imply read. Empty lists deny.
	allowed := [...][]string{
		route.Repository.ReadGroups, route.Repository.WriteGroups, route.Repository.AdminGroups,
	}
	if route.Action > gitAdmin || (operator && route.Action != gitAdmin) ||
		(!operator && !slices.ContainsFunc(claims.Groups, func(group string) bool {
			return group != "" && slices.Contains(allowed[route.Action], group)
		})) {
		return http.StatusForbidden, errors.New("repository access denied")
	}
	return 0, nil
}

func requestAccessToken(r *http.Request) (string, error) {
	headers := r.Header.Values("Authorization")
	if len(headers) == 0 {
		return "", errAuthRequired
	}
	if len(headers) != 1 || len(headers[0]) > 2*authTokenBytes {
		return "", errAuthInvalid
	}
	_, token, basic := r.BasicAuth()
	if !basic {
		scheme, value, _ := strings.Cut(headers[0], " ")
		if !strings.EqualFold(scheme, "Bearer") {
			return "", errAuthInvalid
		}
		token = value
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
