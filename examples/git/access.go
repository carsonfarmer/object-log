package main

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"time"
)

// Local password and anonymous modes are explicit alternatives to Cognito.
// Deployment configuration chooses one mode; failed Cognito never falls back.
func authorizeRequest(r *http.Request, route repositoryRoute, getenv func(string) string, transport http.RoundTripper) (int, error) {
	switch getenv("GIT_AUTH_MODE") {
	case "anonymous":
		if getenv("GIT_PASSWORD") != "" {
			return http.StatusInternalServerError, errors.New("anonymous mode conflicts with GIT_PASSWORD")
		}
		if route.Action == gitAdmin {
			return http.StatusForbidden, errors.New("administration requires authentication")
		}
		return 0, nil
	case "password":
		password := getenv("GIT_PASSWORD")
		if password == "" {
			return http.StatusInternalServerError, errors.New("password authentication requires GIT_PASSWORD")
		}
		_, supplied, ok := r.BasicAuth()
		if !ok || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare([]byte(password), []byte(supplied)) != 1 {
			return http.StatusUnauthorized, errAuthRequired
		}
		return 0, nil
	case "cognito":
		principal, err := authenticateCognito(
			r,
			getenv,
			transport,
			time.Now,
		)
		if errors.Is(err, errAuthConfig) {
			return http.StatusInternalServerError, err
		}
		if errors.Is(err, errAuthUnavailable) {
			return http.StatusServiceUnavailable, err
		}
		if errors.Is(err, errAuthScope) {
			return http.StatusForbidden, err
		}
		if err != nil {
			return http.StatusUnauthorized, err
		}
		if !principal.Allows(route.Repository.repositoryAccess, route.Action) {
			return http.StatusForbidden, errors.New("repository access denied")
		}
		return 0, nil
	default:
		return http.StatusInternalServerError, errors.New("GIT_AUTH_MODE must be cognito, password or anonymous")
	}
}
