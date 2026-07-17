package gateway

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteHTTPErrorHidesInternalDetails(t *testing.T) {
	recorder := httptest.NewRecorder()

	writeHTTPError(recorder, errors.New(
		"open /private/state?token=secret: permission denied",
	))

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "private") ||
		strings.Contains(body, "secret") {
		t.Fatalf("internal error details leaked: %s", body)
	}
	if !strings.Contains(body, internalErrorMessage) {
		t.Fatalf("internal error message = %s", body)
	}
}

func TestHTTPAuthenticationErrorHidesInternalDetails(t *testing.T) {
	api := httpAPI{authenticate: func(*http.Request) (string, error) {
		return "", errors.New("token secret rejected by private issuer")
	}}
	recorder := httptest.NewRecorder()

	if _, ok := api.user(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	); ok {
		t.Fatal("authentication succeeded")
	}

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
	}
	body := recorder.Body.String()
	if strings.Contains(body, "secret") ||
		strings.Contains(body, "private") {
		t.Fatalf("authentication error details leaked: %s", body)
	}
	if !strings.Contains(body, authenticationErrorMessage) {
		t.Fatalf("authentication error message = %s", body)
	}
}

func TestHTTPAuthenticationValidatesCustomIdentity(t *testing.T) {
	api := httpAPI{authenticate: func(*http.Request) (string, error) {
		return " \t ", nil
	}}
	recorder := httptest.NewRecorder()

	if _, ok := api.user(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	); ok {
		t.Fatal("invalid custom identity authenticated")
	}
	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", recorder.Code)
	}
}

func TestHTTPAuthenticationNormalizesCustomIdentity(t *testing.T) {
	api := httpAPI{authenticate: func(*http.Request) (string, error) {
		return " user-a ", nil
	}}
	recorder := httptest.NewRecorder()

	userID, ok := api.user(
		recorder,
		httptest.NewRequest(http.MethodGet, "/", nil),
	)
	if !ok || userID != "user-a" {
		t.Fatalf("userID = %q, ok = %v", userID, ok)
	}
}
