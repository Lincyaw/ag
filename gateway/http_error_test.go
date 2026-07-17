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
