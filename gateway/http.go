package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/lincyaw/ag/registry"
	"github.com/lincyaw/ag/sdk"
)

const (
	UserHeader                 = "X-AG-User-ID"
	maxBodyBytes               = 1 << 20
	authenticationErrorMessage = "authentication failed"
	internalErrorMessage       = "internal server error"
)

type Authenticator func(*http.Request) (string, error)

func HeaderAuthenticator(request *http.Request) (string, error) {
	return normalizeUserID(request.Header.Get(UserHeader))
}

func NewHTTPHandler(
	service *Service,
	authenticate Authenticator,
) (http.Handler, error) {
	if service == nil {
		return nil, errors.New("gateway service is nil")
	}
	if authenticate == nil {
		return nil, errors.New("gateway authenticator is nil")
	}
	api := &httpAPI{service: service, authenticate: authenticate}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(
		writer http.ResponseWriter,
		_ *http.Request,
	) {
		writeJSON(writer, http.StatusOK, map[string]string{
			"status": "ok",
		})
	})
	mux.HandleFunc("POST /v1/sessions", api.createSession)
	mux.HandleFunc("GET /v1/sessions", api.listSessions)
	mux.HandleFunc("GET /v1/sessions/{session}", api.getSession)
	mux.HandleFunc("GET /v1/plugins", api.discoverPlugins)
	mux.HandleFunc(
		"POST /v1/sessions/{session}/plugins",
		api.attachPlugin,
	)
	mux.HandleFunc(
		"DELETE /v1/sessions/{session}/plugins/{plugin}",
		api.detachPlugin,
	)
	mux.HandleFunc(
		"POST /v1/sessions/{session}/messages",
		api.submitMessage,
	)
	mux.HandleFunc(
		"GET /v1/sessions/{session}/executions/{execution}",
		api.getExecution,
	)
	mux.HandleFunc(
		"POST /v1/sessions/{session}/executions/{execution}/context-injections",
		api.enqueueContextInjection,
	)
	mux.HandleFunc(
		"POST /v1/sessions/{session}/executions/{execution}/cancel",
		api.cancelExecution,
	)
	return mux, nil
}

type httpAPI struct {
	service      *Service
	authenticate Authenticator
}

type createSessionRequest struct {
	ID       string `json:"id"`
	Provider string `json:"provider,omitempty"`
	System   string `json:"system,omitempty"`
	MaxTurns int    `json:"max_turns"`
}

type attachPluginRequest struct {
	Selector         string `json:"selector"`
	ExpectedRevision uint64 `json:"expected_revision"`
}

type submitMessageRequest struct {
	Content string `json:"content"`
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func (api *httpAPI) createSession(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	var input createSessionRequest
	if !decodeRequest(writer, request, &input) {
		return
	}
	session, err := api.service.CreateSession(request.Context(), Session{
		ID: input.ID, UserID: userID, Provider: input.Provider,
		System: input.System, MaxTurns: input.MaxTurns,
	})
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusCreated, session)
}

func (api *httpAPI) listSessions(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	page, err := api.service.ListSessions(
		request.Context(),
		userID,
		sdk.PageRequest{
			After: request.URL.Query().Get("page_token"),
			Limit: queryInt(request, "page_size"),
		},
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (api *httpAPI) getSession(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	session, err := api.service.GetSession(
		request.Context(),
		userID,
		request.PathValue("session"),
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (api *httpAPI) discoverPlugins(
	writer http.ResponseWriter,
	request *http.Request,
) {
	if _, ok := api.user(writer, request); !ok {
		return
	}
	labels, err := queryLabels(request)
	if err != nil {
		writeHTTPError(writer, fmt.Errorf(
			"%w: %v",
			ErrInvalidRequest,
			err,
		))
		return
	}
	page, err := api.service.DiscoverPlugins(
		request.Context(),
		registry.DiscoveryQuery{
			Namespace: request.URL.Query().Get("namespace"),
			Name:      request.URL.Query().Get("name"),
			Version:   request.URL.Query().Get("version"),
			Resource:  request.URL.Query().Get("resource"),
			Labels:    labels,
		},
		registry.PageRequest{
			After: request.URL.Query().Get("page_token"),
			Limit: queryInt(request, "page_size"),
		},
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, page)
}

func (api *httpAPI) attachPlugin(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	var input attachPluginRequest
	if !decodeRequest(writer, request, &input) {
		return
	}
	session, err := api.service.AttachPlugin(
		request.Context(),
		userID,
		request.PathValue("session"),
		input.Selector,
		input.ExpectedRevision,
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (api *httpAPI) detachPlugin(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	revision, err := strconv.ParseUint(
		request.URL.Query().Get("expected_revision"),
		10,
		64,
	)
	if err != nil {
		writeHTTPError(writer, fmt.Errorf(
			"%w: expected_revision must be an unsigned integer: %v",
			ErrInvalidRequest,
			err,
		))
		return
	}
	session, err := api.service.DetachPlugin(
		request.Context(),
		userID,
		request.PathValue("session"),
		request.PathValue("plugin"),
		revision,
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, session)
}

func (api *httpAPI) submitMessage(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	var input submitMessageRequest
	if !decodeRequest(writer, request, &input) {
		return
	}
	execution, err := api.service.SubmitMessage(
		request.Context(),
		userID,
		request.PathValue("session"),
		input.Content,
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, execution)
}

func (api *httpAPI) getExecution(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	execution, err := api.service.GetExecution(
		request.Context(),
		userID,
		request.PathValue("session"),
		request.PathValue("execution"),
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, execution)
}

func (api *httpAPI) enqueueContextInjection(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	var input sdk.ContextInjection
	if !decodeRequest(writer, request, &input) {
		return
	}
	execution, err := api.service.EnqueueContextInjection(
		request.Context(),
		userID,
		request.PathValue("session"),
		request.PathValue("execution"),
		input,
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusAccepted, execution)
}

func (api *httpAPI) cancelExecution(
	writer http.ResponseWriter,
	request *http.Request,
) {
	userID, ok := api.user(writer, request)
	if !ok {
		return
	}
	execution, err := api.service.CancelExecution(
		request.Context(),
		userID,
		request.PathValue("session"),
		request.PathValue("execution"),
	)
	if err != nil {
		writeHTTPError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, execution)
}

func (api *httpAPI) user(
	writer http.ResponseWriter,
	request *http.Request,
) (string, bool) {
	userID, err := api.authenticate(request)
	if err == nil {
		userID, err = normalizeUserID(userID)
	}
	if err != nil {
		writeErrorCode(
			writer,
			http.StatusUnauthorized,
			"unauthenticated",
			errors.New(authenticationErrorMessage),
		)
		return "", false
	}
	return userID, true
}

func decodeRequest(
	writer http.ResponseWriter,
	request *http.Request,
	target any,
) bool {
	request.Body = http.MaxBytesReader(writer, request.Body, maxBodyBytes)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		writeErrorCode(writer, http.StatusBadRequest, "invalid_request", err)
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeErrorCode(
			writer,
			http.StatusBadRequest,
			"invalid_request",
			errors.New("request body contains trailing JSON"),
		)
		return false
	}
	return true
}

func queryInt(request *http.Request, name string) int {
	raw := strings.TrimSpace(request.URL.Query().Get(name))
	if raw == "" {
		return 0
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return -1
	}
	return value
}

func queryLabels(request *http.Request) (map[string]string, error) {
	values := request.URL.Query()["label"]
	if len(values) == 0 {
		return nil, nil
	}
	result := make(map[string]string, len(values))
	for _, value := range values {
		key, labelValue, found := strings.Cut(value, "=")
		if !found || strings.TrimSpace(key) == "" {
			return nil, fmt.Errorf("invalid label filter %q", value)
		}
		if _, duplicate := result[key]; duplicate {
			return nil, fmt.Errorf("duplicate label filter %q", key)
		}
		result[key] = labelValue
	}
	return result, nil
}

func writeHTTPError(writer http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrForbidden):
		writeErrorCode(writer, http.StatusForbidden, "forbidden", err)
	case errors.Is(err, ErrSessionNotFound),
		errors.Is(err, ErrExecutionNotFound),
		errors.Is(err, registry.ErrInstanceNotFound):
		writeErrorCode(writer, http.StatusNotFound, "not_found", err)
	case errors.Is(err, ErrSessionExists),
		errors.Is(err, ErrSessionConflict),
		errors.Is(err, ErrExecutionActive),
		errors.Is(err, ErrPluginAmbiguous),
		errors.Is(err, ErrPluginNotBound),
		errors.Is(err, ErrBindingStale),
		errors.Is(err, sdk.ErrTrajectoryExecution),
		errors.Is(err, sdk.ErrTrajectoryClaimed),
		errors.Is(err, sdk.ErrTrajectoryFence):
		writeErrorCode(writer, http.StatusConflict, "conflict", err)
	case errors.Is(err, ErrInvalidRequest),
		errors.Is(err, registry.ErrInvalidRequest):
		writeErrorCode(writer, http.StatusBadRequest, "invalid_request", err)
	default:
		writeErrorCode(
			writer,
			http.StatusInternalServerError,
			"internal",
			errors.New(internalErrorMessage),
		)
	}
}

func writeErrorCode(
	writer http.ResponseWriter,
	status int,
	code string,
	err error,
) {
	var response errorResponse
	response.Error.Code = code
	response.Error.Message = err.Error()
	writeJSON(writer, status, response)
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
