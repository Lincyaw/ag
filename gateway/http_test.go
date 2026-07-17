package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

type fakeExecutionBackend struct {
	executions map[string]Execution
}

func newFakeExecutionBackend() *fakeExecutionBackend {
	return &fakeExecutionBackend{executions: make(map[string]Execution)}
}

func (*fakeExecutionBackend) CreateSession(context.Context, Session) error {
	return nil
}

func (backend *fakeExecutionBackend) Submit(
	_ context.Context,
	session Session,
	_ string,
) (Execution, error) {
	execution := Execution{
		SessionID: session.ID,
		Execution: sdk.TrajectoryExecution{
			ID: "execution-" + session.ID, State: sdk.TrajectoryExecutionPending,
			Revision: 1, InputEntryID: "input",
			CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
		},
	}
	backend.executions[session.ID] = execution
	return execution, nil
}

func (backend *fakeExecutionBackend) Current(
	_ context.Context,
	session Session,
) (Execution, error) {
	execution, exists := backend.executions[session.ID]
	if !exists {
		return Execution{}, ErrExecutionNotFound
	}
	return execution, nil
}

func (backend *fakeExecutionBackend) Get(
	ctx context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	execution, err := backend.Current(ctx, session)
	if err != nil {
		return Execution{}, err
	}
	if execution.Execution.ID != executionID {
		return Execution{}, ErrExecutionNotFound
	}
	return execution, nil
}

func (backend *fakeExecutionBackend) Cancel(
	_ context.Context,
	session Session,
	executionID string,
) (Execution, error) {
	execution, exists := backend.executions[session.ID]
	if !exists || execution.Execution.ID != executionID {
		return Execution{}, ErrExecutionNotFound
	}
	execution.Execution.State = sdk.TrajectoryExecutionCancelled
	execution.Execution.Revision++
	backend.executions[session.ID] = execution
	return execution, nil
}

func (*fakeExecutionBackend) Close(context.Context) error { return nil }

func TestHTTPGatewaySessionPluginMessageAndCancelFlow(t *testing.T) {
	ctx := t.Context()
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	if _, err := directory.Register(
		ctx,
		testRegistration("file", "node-a"),
		registry.LeaseOptions{TTL: time.Minute},
	); err != nil {
		t.Fatal(err)
	}
	store := NewMemorySessionStore()
	executions := newFakeExecutionBackend()
	service, err := NewService(ServiceConfig{
		Store: store, Directory: directory, Executions: executions,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler, err := NewHTTPHandler(service, HeaderAuthenticator)
	if err != nil {
		t.Fatal(err)
	}
	health := serveJSON(
		t,
		handler,
		http.MethodGet,
		"/healthz",
		"",
		nil,
	)
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	create := serveJSON(t, handler, http.MethodPost, "/v1/sessions", "user-a", map[string]any{
		"id": "web-session", "provider": "openai", "max_turns": 8,
	})
	if create.Code != http.StatusCreated {
		t.Fatalf("create status=%d body=%s", create.Code, create.Body.String())
	}
	var session Session
	decodeResponse(t, create, &session)
	if _, err := service.GetSession(
		ctx,
		" user-a ",
		session.ID,
	); err != nil {
		t.Fatalf("get session with normalized user ID: %v", err)
	}

	attach := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/web-session/plugins",
		"user-a",
		map[string]any{
			"selector": "file@node-a", "expected_revision": session.Revision,
		},
	)
	if attach.Code != http.StatusOK {
		t.Fatalf("attach status=%d body=%s", attach.Code, attach.Body.String())
	}
	decodeResponse(t, attach, &session)
	if len(session.Plugins) != 1 {
		t.Fatalf("attached session = %#v", session)
	}

	message := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/web-session/messages",
		"user-a",
		map[string]any{"content": "continue"},
	)
	if message.Code != http.StatusAccepted {
		t.Fatalf("message status=%d body=%s", message.Code, message.Body.String())
	}
	var execution Execution
	decodeResponse(t, message, &execution)
	if execution.Execution.State != sdk.TrajectoryExecutionPending {
		t.Fatalf("submitted execution = %#v", execution)
	}
	polled := serveJSON(
		t,
		handler,
		http.MethodGet,
		"/v1/sessions/web-session/executions/"+execution.Execution.ID,
		"user-a",
		nil,
	)
	if polled.Code != http.StatusOK {
		t.Fatalf("poll status=%d body=%s", polled.Code, polled.Body.String())
	}

	busy := serveJSON(
		t,
		handler,
		http.MethodDelete,
		"/v1/sessions/web-session/plugins/file?expected_revision=2",
		"user-a",
		nil,
	)
	if busy.Code != http.StatusConflict {
		t.Fatalf("busy status=%d body=%s", busy.Code, busy.Body.String())
	}

	cancelled := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/v1/sessions/web-session/executions/"+execution.Execution.ID+"/cancel",
		"user-a",
		nil,
	)
	if cancelled.Code != http.StatusOK {
		t.Fatalf(
			"cancel status=%d body=%s",
			cancelled.Code,
			cancelled.Body.String(),
		)
	}
	decodeResponse(t, cancelled, &execution)
	if execution.Execution.State != sdk.TrajectoryExecutionCancelled {
		t.Fatalf("cancelled execution = %#v", execution)
	}

	detach := serveJSON(
		t,
		handler,
		http.MethodDelete,
		"/v1/sessions/web-session/plugins/file?expected_revision=2",
		"user-a",
		nil,
	)
	if detach.Code != http.StatusOK {
		t.Fatalf("detach status=%d body=%s", detach.Code, detach.Body.String())
	}

	foreign := serveJSON(
		t,
		handler,
		http.MethodGet,
		"/v1/sessions/web-session",
		"user-b",
		nil,
	)
	if foreign.Code != http.StatusForbidden {
		t.Fatalf("foreign status=%d body=%s", foreign.Code, foreign.Body.String())
	}
}

func TestHTTPGatewayRequiresIdentityAndStrictJSON(t *testing.T) {
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	store := NewMemorySessionStore()
	service, err := NewService(ServiceConfig{
		Store: store, Directory: directory,
		Executions: newFakeExecutionBackend(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler, err := NewHTTPHandler(service, HeaderAuthenticator)
	if err != nil {
		t.Fatal(err)
	}
	missing := serveJSON(
		t,
		handler,
		http.MethodGet,
		"/v1/sessions",
		"",
		nil,
	)
	if missing.Code != http.StatusUnauthorized {
		t.Fatalf("missing auth status=%d", missing.Code)
	}
	unknown := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/v1/sessions",
		"user-a",
		map[string]any{
			"id": "strict", "max_turns": 8, "unknown": true,
		},
	)
	if unknown.Code != http.StatusBadRequest {
		t.Fatalf("unknown field status=%d body=%s", unknown.Code, unknown.Body.String())
	}
}

func TestHTTPGatewayAppliesConfiguredSessionDefaults(t *testing.T) {
	directory := registry.NewMemoryDirectory(registry.MemoryConfig{})
	service, err := NewService(ServiceConfig{
		Store: NewMemorySessionStore(), Directory: directory,
		Executions:       newFakeExecutionBackend(),
		DefaultProvider:  "openai",
		DefaultSystem:    "gateway system",
		DefaultMaxTurns:  6,
		DefaultNamespace: registry.DefaultNamespace,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	handler, err := NewHTTPHandler(service, HeaderAuthenticator)
	if err != nil {
		t.Fatal(err)
	}
	response := serveJSON(
		t,
		handler,
		http.MethodPost,
		"/v1/sessions",
		"user-a",
		map[string]any{"id": "defaulted"},
	)
	if response.Code != http.StatusCreated {
		t.Fatalf(
			"create status=%d body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	var session Session
	decodeResponse(t, response, &session)
	if session.Provider != "openai" ||
		session.System != "gateway system" ||
		session.MaxTurns != 6 {
		t.Fatalf("defaulted session = %#v", session)
	}
}

func TestHTTPGatewayMapsTrajectoryFencesToConflict(t *testing.T) {
	for _, err := range []error{
		sdk.ErrTrajectoryExecution,
		sdk.ErrTrajectoryClaimed,
		sdk.ErrTrajectoryFence,
	} {
		response := httptest.NewRecorder()
		writeHTTPError(response, err)
		if response.Code != http.StatusConflict {
			t.Fatalf(
				"error %v status=%d body=%s",
				err,
				response.Code,
				response.Body.String(),
			)
		}
	}
}

func serveJSON(
	t *testing.T,
	handler http.Handler,
	method string,
	path string,
	userID string,
	value any,
) *httptest.ResponseRecorder {
	t.Helper()
	var body bytes.Buffer
	if value != nil {
		if err := json.NewEncoder(&body).Encode(value); err != nil {
			t.Fatal(err)
		}
	}
	request := httptest.NewRequest(method, path, &body)
	if userID != "" {
		request.Header.Set(UserHeader, userID)
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeResponse(
	t *testing.T,
	recorder *httptest.ResponseRecorder,
	target any,
) {
	t.Helper()
	if err := json.Unmarshal(recorder.Body.Bytes(), target); err != nil {
		t.Fatalf("decode response %q: %v", recorder.Body.String(), err)
	}
}

var _ ExecutionBackend = (*fakeExecutionBackend)(nil)
