package main

import (
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const (
	authTokenBytes     = 16 << 10
	authJWKSBytes      = 64 << 10
	authFetchTimeout   = 5 * time.Second
	authKeyLifetime    = 15 * time.Minute
	authRefreshBackoff = time.Minute
)

var (
	errAuthRequired    = errors.New("access token required as Basic password or Bearer credential")
	errAuthInvalid     = errors.New("invalid or expired access token")
	errAuthScope       = errors.New("access token lacks required scope")
	errAuthUnavailable = errors.New("authentication keys unavailable")
	cognitoIssuer      = regexp.MustCompile(
		`^https://cognito-idp\.([a-z0-9-]+)\.amazonaws\.com(?:\.cn)?/([a-z0-9-]+)_[A-Za-z0-9]+$`,
	)
)

type cognitoConfig struct {
	issuer        string
	clientID      string
	requiredScope string
}

type cognitoAuthenticator struct {
	config cognitoConfig
	client *http.Client
	now    func() time.Time

	mu           sync.Mutex
	keys         map[string]jose.JSONWebKey
	expires      time.Time
	refreshAfter time.Time
	refreshError error
}

// The transport must bound network waits and close bodies synchronously. The
// WASIp2 integration must also serialize allocating imports through Unpin.
func newCognitoAuthenticator(config cognitoConfig, transport http.RoundTripper) (*cognitoAuthenticator, error) {
	issuer := cognitoIssuer.FindStringSubmatch(config.issuer)
	if issuer == nil || issuer[1] != issuer[2] {
		return nil, errors.New("Cognito issuer must be an HTTPS user-pool URL")
	}
	if config.clientID == "" || strings.TrimSpace(config.clientID) != config.clientID {
		return nil, errors.New("Cognito client ID is required")
	}
	scopes := strings.Fields(config.requiredScope)
	if len(scopes) != 1 || scopes[0] != config.requiredScope {
		return nil, errors.New("one required Cognito scope must be configured")
	}
	if transport == nil {
		return nil, errors.New("authentication HTTP transport is required")
	}
	return &cognitoAuthenticator{
		config: config,
		client: &http.Client{
			Transport: transport,
			Timeout:   authFetchTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		now:  time.Now,
		keys: map[string]jose.JSONWebKey{},
	}, nil
}

type cognitoClaims struct {
	jwt.Claims
	ClientID string   `json:"client_id"`
	TokenUse string   `json:"token_use"`
	Scope    string   `json:"scope"`
	Groups   []string `json:"cognito:groups"`
}

func (a *cognitoAuthenticator) Authenticate(r *http.Request) (gitPrincipal, error) {
	if a == nil || a.client == nil {
		return gitPrincipal{}, errAuthUnavailable
	}
	encoded, err := requestAccessToken(r)
	if err != nil {
		return gitPrincipal{}, err
	}
	token, err := jwt.ParseSigned(encoded, []jose.SignatureAlgorithm{jose.RS256})
	if err != nil || len(token.Headers) != 1 {
		return gitPrincipal{}, errAuthInvalid
	}
	kid := token.Headers[0].KeyID
	if kid == "" || len(kid) > 256 {
		return gitPrincipal{}, errAuthInvalid
	}
	key, err := a.key(r.Context(), kid)
	if err != nil {
		return gitPrincipal{}, err
	}
	var claims cognitoClaims
	if err := token.Claims(key, &claims); err != nil {
		return gitPrincipal{}, errAuthInvalid
	}
	now := a.now()
	if claims.Expiry == nil || !now.Before(claims.Expiry.Time()) {
		return gitPrincipal{}, errAuthInvalid
	}
	expected := jwt.Expected{Issuer: a.config.issuer, Time: now}
	if err := claims.ValidateWithLeeway(expected, 0); err != nil {
		return gitPrincipal{}, errAuthInvalid
	}
	// Cognito access tokens identify the app by client_id, not the ID-token aud.
	validAccess := claims.TokenUse == "access" && claims.ClientID == a.config.clientID
	if !validAccess || claims.Subject == "" {
		return gitPrincipal{}, errAuthInvalid
	}
	if !slices.Contains(strings.Fields(claims.Scope), a.config.requiredScope) {
		return gitPrincipal{}, errAuthScope
	}
	return gitPrincipal{subject: claims.Subject, groups: claims.Groups}, nil
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

// Keys are an instance-local cache. Fetch synchronously, at most once a minute,
// including unknown kids and failures. A cold instance always refetches.
func (a *cognitoAuthenticator) key(ctx context.Context, kid string) (jose.JSONWebKey, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return jose.JSONWebKey{}, errAuthUnavailable
	}
	now := a.now()
	if key, ok := a.keys[kid]; ok && now.Before(a.expires) {
		return key, nil
	}
	if !now.Before(a.refreshAfter) {
		a.refreshAfter = now.Add(authRefreshBackoff)
		keys, err := a.fetchKeys(ctx)
		a.refreshError = err
		if err == nil {
			a.keys = keys
			a.expires = a.now().Add(authKeyLifetime)
		}
	}
	if a.refreshError != nil || !a.now().Before(a.expires) {
		return jose.JSONWebKey{}, errAuthUnavailable
	}
	key, ok := a.keys[kid]
	if !ok {
		return jose.JSONWebKey{}, errAuthInvalid
	}
	return key, nil
}

func (a *cognitoAuthenticator) fetchKeys(ctx context.Context) (map[string]jose.JSONWebKey, error) {
	ctx, cancel := context.WithTimeout(ctx, authFetchTimeout)
	defer cancel()
	// Only the configured issuer supplies the URL; token jku/x5u are never used.
	endpoint := a.config.issuer + "/.well-known/jwks.json"
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		endpoint,
		nil,
	)
	if err != nil {
		return nil, err
	}
	response, err := a.client.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("JWKS HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, authJWKSBytes+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(body) > authJWKSBytes {
		return nil, errors.New("JWKS exceeds byte limit")
	}
	var set jose.JSONWebKeySet
	if err := json.Unmarshal(body, &set); err != nil {
		return nil, err
	}
	if len(set.Keys) == 0 || len(set.Keys) > 16 {
		return nil, errors.New("JWKS key count out of bounds")
	}
	keys := make(map[string]jose.JSONWebKey, len(set.Keys))
	for _, key := range set.Keys {
		public, ok := key.Key.(*rsa.PublicKey)
		if !ok || !key.Valid() {
			return nil, errors.New("JWKS requires RSA public keys")
		}
		if public.N.BitLen() < 2048 || public.N.BitLen() > 4096 {
			return nil, errors.New("JWKS requires RSA public keys of 2048 to 4096 bits")
		}
		validKey := key.Use == "sig" && key.Algorithm == string(jose.RS256)
		if !validKey || key.KeyID == "" || len(key.KeyID) > 256 {
			return nil, errors.New("invalid Cognito signing key")
		}
		if _, exists := keys[key.KeyID]; exists {
			return nil, errors.New("duplicate JWKS key ID")
		}
		keys[key.KeyID] = key
	}
	return keys, nil
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
	subject string
	groups  []string
}

// Actions are independent: write and admin do not imply read. Empty lists deny.
func (p gitPrincipal) Allows(policy repositoryAccess, action gitAction) bool {
	if p.subject == "" {
		return false
	}
	allowed := []string{}
	switch action {
	case gitRead:
		allowed = policy.ReadGroups
	case gitWrite:
		allowed = policy.WriteGroups
	case gitAdmin:
		allowed = policy.AdminGroups
	default:
		return false
	}
	for _, group := range p.groups {
		if group != "" && slices.Contains(allowed, group) {
			return true
		}
	}
	return false
}
