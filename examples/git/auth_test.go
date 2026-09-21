package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const authTestIssuer = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_Example"

var authTestKey = sync.OnceValues(func() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
})

type authTestTransport func(*http.Request) (*http.Response, error)

func (f authTestTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type authTestBody struct {
	io.Reader
	closed bool
}

func (b *authTestBody) Close() error { b.closed = true; return nil }

func authTestConfig() cognitoConfig {
	return cognitoConfig{issuer: authTestIssuer, clientID: "git-client", requiredScope: "git/access"}
}

func authTestClaims(now time.Time) cognitoClaims {
	return cognitoClaims{
		Claims: jwt.Claims{
			Issuer:   authTestIssuer,
			Subject:  "person-123",
			Expiry:   jwt.NewNumericDate(now.Add(time.Hour)),
			IssuedAt: jwt.NewNumericDate(now.Add(-time.Minute)),
		},
		ClientID: "git-client",
		TokenUse: "access",
		Scope:    "openid git/access",
		Groups:   []string{"readers"},
	}
}

func authSignedToken(t *testing.T, claims cognitoClaims, options *jose.SignerOptions) string {
	t.Helper()
	key, err := authTestKey()
	if err != nil {
		t.Fatal(err)
	}
	if options == nil {
		options = (&jose.SignerOptions{}).WithHeader("kid", "key-one")
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key}, options)
	if err != nil {
		t.Fatal(err)
	}
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	return token
}

func authTestJWKS(t *testing.T) jose.JSONWebKeySet {
	t.Helper()
	key, err := authTestKey()
	if err != nil {
		t.Fatal(err)
	}
	return jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key: &key.PublicKey, KeyID: "key-one", Algorithm: string(jose.RS256), Use: "sig",
	}}}
}

func authKeyResponse(t *testing.T, set jose.JSONWebKeySet) *http.Response {
	t.Helper()
	body, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(string(body)))}
}

func authForTest(t *testing.T, transport http.RoundTripper) *cognitoAuthenticator {
	t.Helper()
	auth, err := newCognitoAuthenticator(authTestConfig(), transport)
	if err != nil {
		t.Fatal(err)
	}
	return auth
}

func authRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://git.example/sha1.git/info/refs", nil)
	r.SetBasicAuth("oauth2", token)
	return r
}

func TestNewCognitoAuthenticatorRejectsIncompleteConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*cognitoConfig)
	}{
		{name: "empty issuer", change: func(c *cognitoConfig) { c.issuer = "" }},
		{name: "HTTP issuer", change: func(c *cognitoConfig) { c.issuer = strings.Replace(c.issuer, "https", "http", 1) }},
		{name: "arbitrary host", change: func(c *cognitoConfig) { c.issuer = "https://keys.example/pool" }},
		{name: "issuer query", change: func(c *cognitoConfig) { c.issuer += "?keys=evil" }},
		{name: "pool region mismatch", change: func(c *cognitoConfig) {
			c.issuer = "https://cognito-idp.us-east-1.amazonaws.com/us-west-2_Example"
		}},
		{name: "client missing", change: func(c *cognitoConfig) { c.clientID = "" }},
		{name: "scope missing", change: func(c *cognitoConfig) { c.requiredScope = "" }},
		{name: "multiple scopes", change: func(c *cognitoConfig) { c.requiredScope = "one two" }},
	}
	transport := authTestTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("configuration validation must not fetch keys")
		return nil, nil
	})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			config := authTestConfig()
			tt.change(&config)
			if _, err := newCognitoAuthenticator(config, transport); err == nil {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
	if _, err := newCognitoAuthenticator(authTestConfig(), nil); err == nil {
		t.Fatal("accepted missing transport")
	}
}

func TestCognitoAuthenticateSignedAccessTokens(t *testing.T) {
	t.Parallel()
	now := time.Now().Truncate(time.Second)
	set := authTestJWKS(t)
	tests := []struct {
		name   string
		change func(*cognitoClaims)
		want   error
	}{
		{name: "valid access token", change: func(*cognitoClaims) {}},
		{name: "expired", change: func(c *cognitoClaims) {
			c.Expiry = jwt.NewNumericDate(now.Add(-time.Second))
		}, want: errAuthInvalid},
		{name: "expiry boundary", change: func(c *cognitoClaims) { c.Expiry = jwt.NewNumericDate(now) }, want: errAuthInvalid},
		{name: "missing expiry", change: func(c *cognitoClaims) { c.Expiry = nil }, want: errAuthInvalid},
		{name: "future issue", change: func(c *cognitoClaims) {
			c.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute))
		}, want: errAuthInvalid},
		{name: "not yet valid", change: func(c *cognitoClaims) {
			c.NotBefore = jwt.NewNumericDate(now.Add(time.Minute))
		}, want: errAuthInvalid},
		{name: "wrong issuer", change: func(c *cognitoClaims) { c.Issuer += "Other" }, want: errAuthInvalid},
		{name: "wrong client", change: func(c *cognitoClaims) { c.ClientID = "another-client" }, want: errAuthInvalid},
		{name: "ID token", change: func(c *cognitoClaims) {
			c.TokenUse, c.Audience = "id", jwt.Audience{"git-client"}
		}, want: errAuthInvalid},
		{name: "missing subject", change: func(c *cognitoClaims) { c.Subject = "" }, want: errAuthInvalid},
		{name: "scope missing", change: func(c *cognitoClaims) { c.Scope = "openid" }, want: errAuthScope},
		{name: "scope substring", change: func(c *cognitoClaims) { c.Scope = "git/access-more" }, want: errAuthScope},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
				if r.URL.String() != authTestIssuer+"/.well-known/jwks.json" || r.Header.Get("Authorization") != "" {
					t.Fatal("key fetch was not restricted to the configured issuer without credentials")
				}
				return authKeyResponse(t, set), nil
			}))
			auth.now = func() time.Time { return now }
			claims := authTestClaims(now)
			tt.change(&claims)
			principal, err := auth.Authenticate(authRequest(authSignedToken(t, claims, nil)))
			if !errors.Is(err, tt.want) {
				t.Fatalf("Authenticate error = %v, want %v", err, tt.want)
			}
			if err == nil && !principal.Allows(repositoryAccess{ReadGroups: []string{"readers"}}, gitRead) {
				t.Fatal("verified group did not grant configured read access")
			}
			if err != nil && principal.subject != "" {
				t.Fatal("failed authentication returned a principal")
			}
		})
	}
}

func TestCognitoAuthenticateRejectsSignatureAndHeaderAttacks(t *testing.T) {
	t.Parallel()
	claims := authTestClaims(time.Now())
	valid := authSignedToken(t, claims, nil)
	parts := strings.Split(valid, ".")
	parts[2] = strings.Repeat("A", len(parts[2]))
	hmac, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.HS256, Key: []byte(strings.Repeat("a", 32))}, nil)
	if err != nil {
		t.Fatal(err)
	}
	hmacToken, err := jwt.Signed(hmac).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name  string
		token string
	}{
		{name: "tampered signature", token: strings.Join(parts, ".")},
		{name: "wrong algorithm", token: hmacToken},
		{name: "no signature", token: "eyJhbGciOiJub25lIn0.e30."},
		{name: "missing kid", token: authSignedToken(t, claims, &jose.SignerOptions{})},
		{name: "unknown kid", token: authSignedToken(t, claims, (&jose.SignerOptions{}).WithHeader("kid", "unknown"))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
				return authKeyResponse(t, authTestJWKS(t)), nil
			}))
			if _, err := auth.Authenticate(authRequest(tt.token)); !errors.Is(err, errAuthInvalid) {
				t.Fatalf("Authenticate error = %v, want invalid token", err)
			}
		})
	}
}

func TestCognitoTokenProvidedURLsAreIgnored(t *testing.T) {
	t.Parallel()
	options := (&jose.SignerOptions{}).WithHeader("kid", "key-one").
		WithHeader("jku", "https://attacker.example/keys").WithHeader("x5u", "https://attacker.example/cert")
	token := authSignedToken(t, authTestClaims(time.Now()), options)
	auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != authTestIssuer+"/.well-known/jwks.json" {
			t.Fatal("followed token-provided URL")
		}
		return authKeyResponse(t, authTestJWKS(t)), nil
	}))
	if _, err := auth.Authenticate(authRequest(token)); err != nil {
		t.Fatal(err)
	}
}

func TestRequestAccessToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		header []string
		want   error
	}{
		{name: "missing", want: errAuthRequired},
		{name: "Basic password", header: []string{"Basic b2F1dGgyOnRva2Vu"}},
		{name: "Bearer", header: []string{"Bearer token"}},
		{name: "case insensitive", header: []string{"bearer token"}},
		{name: "malformed Basic", header: []string{"Basic %%%"}, want: errAuthInvalid},
		{name: "empty password", header: []string{"Basic b2F1dGgyOg=="}, want: errAuthInvalid},
		{name: "empty Bearer", header: []string{"Bearer "}, want: errAuthInvalid},
		{name: "unsupported scheme", header: []string{"Digest token"}, want: errAuthInvalid},
		{name: "ambiguous credentials", header: []string{"Bearer token", "Basic b2F1dGgyOnRva2Vu"}, want: errAuthInvalid},
		{name: "oversized token", header: []string{"Bearer " + strings.Repeat("a", authTokenBytes+1)}, want: errAuthInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodGet, "https://git.example", nil)
			r.Header["Authorization"] = tt.header
			token, err := requestAccessToken(r)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
			if err == nil && token != "token" {
				t.Fatalf("token = %q", token)
			}
		})
	}
}

func TestCognitoKeyCacheRotationAndExpiry(t *testing.T) {
	t.Parallel()
	now := time.Now()
	set := authTestJWKS(t)
	calls := 0
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		calls++
		return authKeyResponse(t, set), nil
	}))
	auth.now = func() time.Time { return now }
	if _, err := auth.key(context.Background(), "key-one"); err != nil {
		t.Fatal(err)
	}
	if _, err := auth.key(context.Background(), "key-one"); err != nil || calls != 1 {
		t.Fatalf("cached lookup error=%v calls=%d", err, calls)
	}
	if _, err := auth.key(context.Background(), "unknown"); !errors.Is(err, errAuthInvalid) || calls != 1 {
		t.Fatalf("unknown kid bypassed refresh backoff: error=%v calls=%d", err, calls)
	}
	set.Keys[0].KeyID = "key-two"
	now = now.Add(authRefreshBackoff)
	if _, err := auth.key(context.Background(), "key-two"); err != nil || calls != 2 {
		t.Fatalf("rotated key error=%v calls=%d", err, calls)
	}
	if _, err := auth.key(context.Background(), "key-one"); !errors.Is(err, errAuthInvalid) {
		t.Fatal("removed key remained trusted")
	}
	now = now.Add(authKeyLifetime)
	if _, err := auth.key(context.Background(), "key-two"); err != nil || calls != 3 {
		t.Fatalf("expired cache did not refresh: error=%v calls=%d", err, calls)
	}
}

func TestCognitoKeyFetchFailuresDenyAndBackOff(t *testing.T) {
	t.Parallel()
	valid := authTestJWKS(t)
	tests := []struct {
		name string
		get  func() *http.Response
	}{
		{name: "HTTP failure", get: func() *http.Response {
			return &http.Response{StatusCode: 503, Body: io.NopCloser(strings.NewReader("down"))}
		}},
		{name: "redirect", get: func() *http.Response {
			return &http.Response{
				StatusCode: 302, Header: http.Header{"Location": []string{"https://attacker.example/keys"}},
				Body: io.NopCloser(strings.NewReader("")),
			}
		}},
		{name: "oversized", get: func() *http.Response {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(strings.Repeat(" ", authJWKSBytes+1)))}
		}},
		{name: "malformed", get: func() *http.Response {
			return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader("{"))}
		}},
		{name: "empty keys", get: func() *http.Response { return authKeyResponse(t, jose.JSONWebKeySet{}) }},
		{name: "duplicate kid", get: func() *http.Response {
			return authKeyResponse(t, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{valid.Keys[0], valid.Keys[0]}})
		}},
		{name: "non signing key", get: func() *http.Response {
			key := valid.Keys[0]
			key.Use = "enc"
			return authKeyResponse(t, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key}})
		}},
		{name: "wrong key algorithm", get: func() *http.Response {
			key := valid.Keys[0]
			key.Algorithm = string(jose.RS512)
			return authKeyResponse(t, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key}})
		}},
		{name: "symmetric key", get: func() *http.Response {
			key := valid.Keys[0]
			key.Key = []byte(strings.Repeat("a", 32))
			return authKeyResponse(t, jose.JSONWebKeySet{Keys: []jose.JSONWebKey{key}})
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.String() != authTestIssuer+"/.well-known/jwks.json" {
					t.Fatal("followed redirect")
				}
				return tt.get(), nil
			}))
			for range 2 {
				if _, err := auth.key(context.Background(), "key-one"); !errors.Is(err, errAuthUnavailable) {
					t.Fatalf("key error = %v, want unavailable", err)
				}
			}
			if calls != 1 {
				t.Fatalf("failed fetch retried without backoff: calls=%d", calls)
			}
		})
	}
}

func TestCognitoExpiredKeysFailClosedDuringOutage(t *testing.T) {
	t.Parallel()
	now := time.Now()
	available := true
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		if !available {
			return nil, errors.New("offline")
		}
		return authKeyResponse(t, authTestJWKS(t)), nil
	}))
	auth.now = func() time.Time { return now }
	if _, err := auth.key(context.Background(), "key-one"); err != nil {
		t.Fatal(err)
	}
	available = false
	now = now.Add(authKeyLifetime)
	if _, err := auth.key(context.Background(), "key-one"); !errors.Is(err, errAuthUnavailable) {
		t.Fatalf("stale key accepted during outage: %v", err)
	}
	available = true
	now = now.Add(authRefreshBackoff)
	if _, err := auth.key(context.Background(), "key-one"); err != nil {
		t.Fatalf("recovery after backoff failed: %v", err)
	}
}

func TestCognitoKeyFetchClosesBodiesSynchronously(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		body string
	}{
		{name: "invalid JSON", body: "{"},
		{name: "oversized body", body: strings.Repeat(" ", authJWKSBytes+1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			body := &authTestBody{Reader: strings.NewReader(tt.body)}
			auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: body}, nil
			}))
			if _, err := auth.key(context.Background(), "key-one"); !errors.Is(err, errAuthUnavailable) {
				t.Fatalf("key error = %v", err)
			}
			if !body.closed {
				t.Fatal("returned before closing key response")
			}
		})
	}
}

func TestCognitoKeyFetchDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
			<-r.Context().Done()
			return nil, r.Context().Err()
		}))
		start := time.Now()
		if _, err := auth.key(context.Background(), "key-one"); !errors.Is(err, errAuthUnavailable) {
			t.Fatalf("key error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != authFetchTimeout {
			t.Fatalf("fetch duration = %v, want %v", elapsed, authFetchTimeout)
		}
	})
}

func TestCognitoConcurrentAuthenticationSharesKeys(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return authKeyResponse(t, authTestJWKS(t)), nil
	}))
	token := authSignedToken(t, authTestClaims(time.Now()), nil)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			if _, err := auth.Authenticate(authRequest(token)); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if calls.Load() != 1 {
		t.Fatalf("key fetches = %d, want one", calls.Load())
	}
}

func TestGitPrincipalActionsAreIndependentAndDenyByDefault(t *testing.T) {
	t.Parallel()
	policy := repositoryAccess{
		ReadGroups: []string{"readers"}, WriteGroups: []string{"writers"}, AdminGroups: []string{"admins"},
	}
	tests := []struct {
		name   string
		groups []string
		action gitAction
		want   bool
	}{
		{name: "reader can read", groups: []string{"readers"}, action: gitRead, want: true},
		{name: "reader cannot write", groups: []string{"readers"}, action: gitWrite},
		{name: "writer can write", groups: []string{"writers"}, action: gitWrite, want: true},
		{name: "writer cannot read", groups: []string{"writers"}, action: gitRead},
		{name: "writer cannot administer", groups: []string{"writers"}, action: gitAdmin},
		{name: "admin can administer", groups: []string{"admins"}, action: gitAdmin, want: true},
		{name: "admin cannot write", groups: []string{"admins"}, action: gitWrite},
		{name: "unknown group", groups: []string{"visitors"}, action: gitRead},
		{name: "no group", action: gitRead},
		{name: "unknown action", groups: []string{"admins"}, action: gitAction(255)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			principal := gitPrincipal{subject: "person-123", groups: tt.groups}
			if got := principal.Allows(policy, tt.action); got != tt.want {
				t.Fatalf("Allows = %v, want %v", got, tt.want)
			}
			if principal.Allows(repositoryAccess{}, tt.action) {
				t.Fatal("empty policy granted access")
			}
		})
	}
	if (gitPrincipal{}).Allows(policy, gitRead) {
		t.Fatal("zero principal granted access")
	}
}
