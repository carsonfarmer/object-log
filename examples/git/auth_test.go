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
	io.ReadCloser
	closed bool
}

func (b *authTestBody) Close() error { b.closed = true; return b.ReadCloser.Close() }

type cognitoConfig struct {
	issuer, clientID, operatorClientID, requiredScope string
}

func (c cognitoConfig) get(name string) string {
	return map[string]string{
		"GIT_COGNITO_ISSUER": c.issuer, "GIT_COGNITO_CLIENT_ID": c.clientID,
		"GIT_COGNITO_OPERATOR_CLIENT_ID": c.operatorClientID, "GIT_COGNITO_SCOPE": c.requiredScope,
	}[name]
}

type cognitoClaims struct {
	jwt.Claims
	ClientID string   `json:"client_id"`
	TokenUse string   `json:"token_use"`
	Scope    string   `json:"scope"`
	Groups   []string `json:"cognito:groups"`
}

type authTestAuthenticator struct {
	config    cognitoConfig
	route     repositoryRoute
	transport http.RoundTripper
	now       func() time.Time
}

func (a *authTestAuthenticator) Authenticate(r *http.Request) (int, error) {
	return authorizeCognito(r, a.route, a.config.get, a.transport, a.now)
}

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

func authForTest(t *testing.T, transport http.RoundTripper) *authTestAuthenticator {
	t.Helper()
	return &authTestAuthenticator{
		config: authTestConfig(), transport: transport, now: time.Now,
		route: repositoryRoute{Action: gitRead, Repository: repositoryAccess{ReadGroups: []string{"readers"}}},
	}
}

func authenticateForContext(t *testing.T, auth *authTestAuthenticator, ctx context.Context) (int, error) {
	t.Helper()
	return auth.Authenticate(authRequest(authSignedToken(t, authTestClaims(time.Now()), nil)).WithContext(ctx))
}

func authRequest(token string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "https://git.example/sha1.git/info/refs", nil)
	r.SetBasicAuth("oauth2", token)
	return r
}

func authorizeForTest(t *testing.T, route repositoryRoute, claims cognitoClaims) (int, error) {
	t.Helper()
	get := func(name string) string {
		switch name {
		case "GIT_AUTH_MODE":
			return "cognito"
		case "GIT_COGNITO_OPERATOR_CLIENT_ID":
			return "maintenance-client"
		default:
			return authTestConfig().get(name)
		}
	}
	keys := authTestTransport(func(*http.Request) (*http.Response, error) {
		return authKeyResponse(t, authTestJWKS(t)), nil
	})
	return authorizeRequest(authRequest(authSignedToken(t, claims, nil)), route, get, keys)
}

func TestCognitoRejectsIncompleteConfiguration(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		change func(*cognitoConfig)
	}{
		{name: "empty issuer", change: func(c *cognitoConfig) { c.issuer = "" }},
		{name: "HTTP issuer", change: func(c *cognitoConfig) { c.issuer = strings.Replace(c.issuer, "https", "http", 1) }},
		{name: "client missing", change: func(c *cognitoConfig) { c.clientID = "" }},
		{name: "shared user and operator client", change: func(c *cognitoConfig) { c.operatorClientID = c.clientID }},
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
			if status, err := authorizeCognito(authRequest("token"), repositoryRoute{}, config.get, transport, time.Now); !errors.Is(err, errAuthConfig) || status != http.StatusInternalServerError {
				t.Fatal("accepted invalid configuration")
			}
		})
	}
	if status, err := authorizeCognito(authRequest("token"), repositoryRoute{}, authTestConfig().get, nil, time.Now); !errors.Is(err, errAuthConfig) || status != http.StatusInternalServerError {
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
		bearer bool
		want   error
		status int
	}{
		{name: "valid Basic password", change: func(*cognitoClaims) {}},
		{name: "valid Bearer", change: func(*cognitoClaims) {}, bearer: true},
		{name: "access token client overrides unrelated audience", change: func(c *cognitoClaims) {
			c.Audience = jwt.Audience{"another-resource"}
		}},
		{name: "expired", change: func(c *cognitoClaims) {
			c.Expiry = jwt.NewNumericDate(now.Add(-time.Second))
		}, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "expiry boundary", change: func(c *cognitoClaims) { c.Expiry = jwt.NewNumericDate(now) }, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "missing expiry", change: func(c *cognitoClaims) { c.Expiry = nil }, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "future issue", change: func(c *cognitoClaims) {
			c.IssuedAt = jwt.NewNumericDate(now.Add(time.Minute))
		}, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "not yet valid", change: func(c *cognitoClaims) {
			c.NotBefore = jwt.NewNumericDate(now.Add(time.Minute))
		}, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "wrong issuer", change: func(c *cognitoClaims) { c.Issuer += "Other" }, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "wrong client", change: func(c *cognitoClaims) { c.ClientID = "another-client" }, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "audience cannot substitute for access token client", change: func(c *cognitoClaims) {
			c.ClientID, c.Audience = "another-client", jwt.Audience{"git-client"}
		}, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "ID token", change: func(c *cognitoClaims) {
			c.TokenUse, c.Audience = "id", jwt.Audience{"git-client"}
		}, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "missing subject", change: func(c *cognitoClaims) { c.Subject = "" }, want: errAuthInvalid, status: http.StatusUnauthorized},
		{name: "scope missing", change: func(c *cognitoClaims) { c.Scope = "openid" }, want: errAuthScope, status: http.StatusForbidden},
		{name: "scope substring", change: func(c *cognitoClaims) { c.Scope = "git/access-more" }, want: errAuthScope, status: http.StatusForbidden},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.String() != authTestIssuer+"/.well-known/jwks.json" || r.Header.Get("Authorization") != "" {
					t.Fatal("key fetch was not restricted to the configured issuer without credentials")
				}
				return authKeyResponse(t, set), nil
			}))
			auth.now = func() time.Time { return now }
			claims := authTestClaims(now)
			tt.change(&claims)
			token := authSignedToken(t, claims, nil)
			request := authRequest(token)
			if tt.bearer {
				request.Header.Set("Authorization", "Bearer "+token)
			}
			status, err := auth.Authenticate(request)
			if !errors.Is(err, tt.want) || status != tt.status {
				t.Fatalf("authorization: status=%d error=%v, want status=%d error=%v", status, err, tt.status, tt.want)
			}
			if calls != 1 {
				t.Fatalf("key fetches = %d, want one", calls)
			}
		})
	}
}

func TestCognitoAuthenticateSigningKeyRotation(t *testing.T) {
	t.Parallel()
	claims := authTestClaims(time.Now())
	original := authSignedToken(t, claims, nil)
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: key},
		(&jose.SignerOptions{}).WithHeader("kid", "key-two"))
	if err != nil {
		t.Fatal(err)
	}
	rotated, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		t.Fatal(err)
	}
	set := authTestJWKS(t)
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		return authKeyResponse(t, set), nil
	}))
	if status, err := auth.Authenticate(authRequest(original)); err != nil || status != 0 {
		t.Fatalf("original key: status=%d error=%v", status, err)
	}
	set = jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{
		Key: &key.PublicKey, KeyID: "key-two", Algorithm: string(jose.RS256), Use: "sig",
	}}}
	if status, err := auth.Authenticate(authRequest(rotated)); err != nil || status != 0 {
		t.Fatalf("rotated key: status=%d error=%v", status, err)
	}
	if status, err := auth.Authenticate(authRequest(original)); !errors.Is(err, errAuthInvalid) || status != http.StatusUnauthorized {
		t.Fatalf("retired key: status=%d error=%v", status, err)
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
		{name: "unknown kid", token: authSignedToken(t, claims, (&jose.SignerOptions{}).WithHeader("kid", "unknown"))},
		{name: "missing kid", token: authSignedToken(t, claims, &jose.SignerOptions{})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
				return authKeyResponse(t, authTestJWKS(t)), nil
			}))
			if status, err := auth.Authenticate(authRequest(tt.token)); !errors.Is(err, errAuthInvalid) || status != http.StatusUnauthorized {
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

func TestCognitoKeyFetchFailuresDenyAndCloseBodies(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		get    func() *http.Response
		want   error
		status int
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
		{name: "empty keys", get: func() *http.Response { return authKeyResponse(t, jose.JSONWebKeySet{}) }, want: errAuthInvalid, status: http.StatusUnauthorized},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			response := tt.get()
			body := &authTestBody{ReadCloser: response.Body}
			response.Body = body
			auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.URL.String() != authTestIssuer+"/.well-known/jwks.json" {
					t.Fatal("followed redirect")
				}
				return response, nil
			}))
			want := tt.want
			if want == nil {
				want = errAuthUnavailable
			}
			wantStatus := tt.status
			if wantStatus == 0 {
				wantStatus = http.StatusServiceUnavailable
			}
			if status, err := authenticateForContext(t, auth, context.Background()); !errors.Is(err, want) || status != wantStatus {
				t.Fatalf("key response: status=%d error=%v, want status=%d error=%v", status, err, wantStatus, want)
			}
			if calls != 1 {
				t.Fatalf("key fetches = %d, want one", calls)
			}
			if !body.closed {
				t.Fatal("returned before closing key response")
			}
		})
	}
}

func TestCognitoTrustedIssuerIsMatchedExactly(t *testing.T) {
	t.Parallel()
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		return authKeyResponse(t, authTestJWKS(t)), nil
	}))
	auth.config.issuer = "https://identity.example/trusted-pool"
	claims := authTestClaims(time.Now())
	claims.Issuer = auth.config.issuer
	if _, err := auth.Authenticate(authRequest(authSignedToken(t, claims, nil))); err != nil {
		t.Fatalf("configured trusted issuer rejected: %v", err)
	}
	claims.Issuer = "https://attacker.example/pool"
	if _, err := auth.Authenticate(authRequest(authSignedToken(t, claims, nil))); !errors.Is(err, errAuthInvalid) {
		t.Fatalf("unconfigured issuer accepted: %v", err)
	}
}

func TestCognitoKeyMetadataDoesNotGrantTokenAuthority(t *testing.T) {
	t.Parallel()
	for _, change := range []func(*jose.JSONWebKeySet){
		func(s *jose.JSONWebKeySet) { s.Keys = append(s.Keys, s.Keys[0]) },
		func(s *jose.JSONWebKeySet) { s.Keys[0].Use = "enc" },
		func(s *jose.JSONWebKeySet) { s.Keys[0].Algorithm = string(jose.RS512) },
	} {
		keys := authTestJWKS(t)
		change(&keys)
		auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
			return authKeyResponse(t, keys), nil
		}))
		claims := authTestClaims(time.Now())
		if _, err := auth.Authenticate(authRequest(authSignedToken(t, claims, nil))); err != nil {
			t.Fatalf("trusted signing key rejected because of unrelated metadata: %v", err)
		}
		claims.ClientID = "other-client"
		if _, err := auth.Authenticate(authRequest(authSignedToken(t, claims, nil))); !errors.Is(err, errAuthInvalid) {
			t.Fatalf("key metadata bypassed client policy: %v", err)
		}
	}
}

func TestCognitoExpiryIsCheckedAfterKeyFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		claims := authTestClaims(time.Now())
		claims.Expiry = jwt.NewNumericDate(time.Now().Add(2 * time.Second))
		token := authSignedToken(t, claims, nil)
		auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
			time.Sleep(3 * time.Second)
			return authKeyResponse(t, authTestJWKS(t)), nil
		}))
		if _, err := auth.Authenticate(authRequest(token)); !errors.Is(err, errAuthInvalid) {
			t.Fatalf("accepted token that expired during key fetch: %v", err)
		}
	})
}

func TestCognitoAlreadyCanceledRequestDoesNotFetchKeys(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
		t.Fatal("already canceled authentication fetched keys")
		return nil, nil
	}))
	if _, err := authenticateForContext(t, auth, ctx); !errors.Is(err, errAuthUnavailable) {
		t.Fatalf("key error = %v, want unavailable", err)
	}
}

func TestCognitoCanceledRequestDoesNotAffectNextAuthentication(t *testing.T) {
	t.Parallel()
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			cancel()
			return nil, r.Context().Err()
		}
		return authKeyResponse(t, authTestJWKS(t)), nil
	}))
	request := authRequest(authSignedToken(t, authTestClaims(time.Now()), nil))
	if _, err := auth.Authenticate(request.WithContext(ctx)); !errors.Is(err, errAuthUnavailable) {
		t.Fatalf("canceled authentication error = %v", err)
	}
	if _, err := auth.Authenticate(request); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("key fetches = %d, want two", calls)
	}
}

type authContextBody struct {
	ctx    context.Context
	closed bool
}

func (b *authContextBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *authContextBody) Close() error { b.closed = true; return nil }

func TestCognitoFetchBodyClosesBeforeCancellationOrDeadlineReturns(t *testing.T) {
	for _, cancelEarly := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var body *authContextBody
			auth := authForTest(t, authTestTransport(func(r *http.Request) (*http.Response, error) {
				body = &authContextBody{ctx: r.Context()}
				if cancelEarly {
					time.AfterFunc(time.Second, cancel)
				}
				return &http.Response{StatusCode: http.StatusOK, Body: body}, nil
			}))
			start := time.Now()
			if _, err := authenticateForContext(t, auth, ctx); !errors.Is(err, errAuthUnavailable) {
				t.Fatalf("fetch error = %v, want unavailable", err)
			}
			if body == nil || !body.closed {
				t.Fatal("authentication returned while the detached OIDC fetch still owned its response body")
			}
			want := authFetchTimeout
			if cancelEarly {
				want = time.Second
			}
			if elapsed := time.Since(start); elapsed != want {
				t.Fatalf("fetch duration = %v, want %v", elapsed, want)
			}
			auth.transport = authTestTransport(func(*http.Request) (*http.Response, error) {
				return authKeyResponse(t, authTestJWKS(t)), nil
			})
			if _, err := authenticateForContext(t, auth, context.Background()); err != nil {
				t.Fatalf("subsequent authentication inherited a failed request: %v", err)
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
		if _, err := authenticateForContext(t, auth, context.Background()); !errors.Is(err, errAuthUnavailable) {
			t.Fatalf("key error = %v", err)
		}
		if elapsed := time.Since(start); elapsed != authFetchTimeout {
			t.Fatalf("fetch duration = %v, want %v", elapsed, authFetchTimeout)
		}
	})
}

func TestCognitoActionsAreIndependentAndDenyByDefault(t *testing.T) {
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
			auth := authForTest(t, authTestTransport(func(*http.Request) (*http.Response, error) {
				return authKeyResponse(t, authTestJWKS(t)), nil
			}))
			auth.route = repositoryRoute{Action: tt.action, Repository: policy}
			claims := authTestClaims(time.Now())
			claims.Groups = tt.groups
			request := authRequest(authSignedToken(t, claims, nil))
			status, err := auth.Authenticate(request)
			if (status == 0 && err == nil) != tt.want {
				t.Fatalf("authorization: status=%d error=%v, want allowed=%v", status, err, tt.want)
			}
			auth.route.Repository = repositoryAccess{}
			if status, err := auth.Authenticate(request); status != http.StatusForbidden || err == nil {
				t.Fatalf("empty policy: status=%d error=%v", status, err)
			}
		})
	}
}
