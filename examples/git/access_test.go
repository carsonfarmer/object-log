package main

import (
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestAuthenticationModeNeverFallsBack(t *testing.T) {
	route := repositoryRoute{Action: gitRead, Repository: repositoryConfig{
		repositoryAccess: repositoryAccess{ReadGroups: []string{"readers"}},
	}}
	settings := map[string]string{
		"GIT_AUTH_MODE": "cognito", "GIT_PASSWORD": "local-password",
		"GIT_COGNITO_ISSUER": authTestIssuer, "GIT_COGNITO_CLIENT_ID": "git-client",
		"GIT_COGNITO_SCOPE": "git/access",
	}
	get := func(name string) string { return settings[name] }
	keys := authTestTransport(func(*http.Request) (*http.Response, error) {
		return authKeyResponse(t, authTestJWKS(t)), nil
	})
	for _, mode := range []string{"cognito", "password", "anonymous", "", "invalid"} {
		settings["GIT_AUTH_MODE"] = mode
		status, err := authorizeRequest(authRequest("local-password"), route, get, keys)
		want := map[string]int{"cognito": 401, "password": 0, "anonymous": 500, "": 500, "invalid": 500}[mode]
		if status != want || (err == nil) != (want == 0) {
			t.Errorf("mode %q: status=%d error=%v", mode, status, err)
		}
	}
	settings["GIT_AUTH_MODE"] = "password"
	delete(settings, "GIT_PASSWORD")
	if status, _ := authorizeRequest(authRequest(""), route, get, keys); status != 500 {
		t.Fatalf("empty password config: %d", status)
	}
	settings["GIT_AUTH_MODE"] = "anonymous"
	route.Action = gitAdmin
	if status, _ := authorizeRequest(authRequest(""), route, get, keys); status != 403 {
		t.Fatalf("anonymous administration: %d", status)
	}
	settings["GIT_AUTH_MODE"] = "cognito"
	token := authSignedToken(t, authTestClaims(time.Now()), nil)
	for _, action := range []gitAction{gitRead, gitWrite, gitAdmin} {
		route.Action = action
		status, err := authorizeRequest(authRequest(token), route, get, keys)
		want := 403
		if action == gitRead {
			want = 0
		}
		if status != want {
			t.Fatalf("reader action %d: status=%d error=%v", action, status, err)
		}
	}
	unavailable := authTestTransport(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("key service unavailable")
	})
	if status, _ := authorizeRequest(authRequest(token), route, get, unavailable); status != 503 {
		t.Fatalf("key outage fell back to local auth: %d", status)
	}
}

func TestMaintenanceClientCannotReadOrPushRepositories(t *testing.T) {
	settings := map[string]string{
		"GIT_AUTH_MODE": "cognito", "GIT_COGNITO_ISSUER": authTestIssuer,
		"GIT_COGNITO_CLIENT_ID": "git-client", "GIT_COGNITO_OPERATOR_CLIENT_ID": "maintenance-client",
		"GIT_COGNITO_SCOPE": "git/access",
	}
	get := func(name string) string { return settings[name] }
	keys := authTestTransport(func(*http.Request) (*http.Response, error) {
		return authKeyResponse(t, authTestJWKS(t)), nil
	})
	for _, client := range []string{"maintenance-client", "git-client", "other-client"} {
		for _, scope := range []string{"git/access", "git/access git/maintenance"} {
			claims := authTestClaims(time.Now())
			claims.ClientID, claims.Scope = client, scope
			request := authRequest(authSignedToken(t, claims, nil))
			for _, action := range []gitAction{gitRead, gitWrite, gitAdmin} {
				status, _ := authorizeRequest(request, repositoryRoute{Action: action}, get, keys)
				want := 403 // No repository groups grant this token access.
				if client == "other-client" {
					want = 401
				}
				if client == "maintenance-client" && scope == "git/access git/maintenance" && action == gitAdmin {
					want = 0
				}
				if status != want {
					t.Fatalf("client=%s scope=%s action=%d: got %d want %d", client, scope, action, status, want)
				}
			}
		}
	}
}
