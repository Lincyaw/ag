package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/lincyaw/ag/sdk"
)

var (
	ErrInvalidRequest  = errors.New("invalid gateway request")
	ErrSessionNotFound = errors.New("gateway session not found")
	ErrSessionExists   = errors.New("gateway session already exists")
	ErrSessionConflict = errors.New("gateway session revision conflict")
	ErrStoreClosed     = errors.New("gateway session store closed")
)

type PluginBinding struct {
	Namespace  string            `json:"namespace"`
	Name       string            `json:"name"`
	InstanceID string            `json:"instance_id"`
	URI        string            `json:"uri"`
	Manifest   sdk.Manifest      `json:"manifest"`
	Labels     map[string]string `json:"labels,omitempty"`
	Epoch      uint64            `json:"epoch"`
}

type Session struct {
	ID            string `json:"id"`
	UserID        string `json:"user_id"`
	Provider      string `json:"provider,omitempty"`
	System        string `json:"system,omitempty"`
	MaxTurns      int    `json:"max_turns"`
	WorkspaceRoot string `json:"workspace_root,omitempty"`
	// RuntimeConfig is a private, durable execution profile supplied when the
	// trajectory is created. It is deliberately excluded from API JSON so
	// listing or attaching to a trajectory cannot disclose tool configuration.
	RuntimeConfig json.RawMessage `json:"-"`
	Paused        bool            `json:"paused,omitempty"`
	Revision      uint64          `json:"revision"`
	Plugins       []PluginBinding `json:"plugins"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

type SessionPage struct {
	Items []Session `json:"items"`
	Next  string    `json:"next,omitempty"`
}

type StoreCapabilities struct {
	Durable          bool `json:"durable"`
	MultiProcessSafe bool `json:"multi_process_safe"`
}

type SessionStore interface {
	Create(context.Context, Session) (Session, error)
	Get(context.Context, string) (Session, error)
	List(context.Context, sdk.PageRequest) (SessionPage, error)
	ListByUser(context.Context, string, sdk.PageRequest) (SessionPage, error)
	Save(context.Context, Session, uint64) (Session, error)
	Delete(context.Context, string, uint64) error
	Capabilities() StoreCapabilities
	Close(context.Context) error
}

func normalizeSession(session Session) (Session, error) {
	session.ID = strings.TrimSpace(session.ID)
	session.Provider = strings.TrimSpace(session.Provider)
	session.WorkspaceRoot = strings.TrimSpace(session.WorkspaceRoot)
	if err := sdk.ValidateResourceName("gateway session", session.ID); err != nil {
		return Session{}, err
	}
	var err error
	session.UserID, err = normalizeUserID(session.UserID)
	if err != nil {
		return Session{}, err
	}
	if session.Provider != "" {
		if err := sdk.ValidateResourceName("provider", session.Provider); err != nil {
			return Session{}, err
		}
	}
	if session.MaxTurns < 0 {
		return Session{}, errors.New("gateway session max turns cannot be negative")
	}
	if session.WorkspaceRoot != "" {
		if !filepath.IsAbs(session.WorkspaceRoot) {
			return Session{}, errors.New(
				"gateway session workspace root must be absolute",
			)
		}
		session.WorkspaceRoot = filepath.Clean(session.WorkspaceRoot)
	}
	if len(session.RuntimeConfig) > 0 {
		if len(session.RuntimeConfig) > 1<<20 {
			return Session{}, errors.New(
				"gateway session runtime config exceeds 1 MiB",
			)
		}
		if !json.Valid(session.RuntimeConfig) {
			return Session{}, errors.New(
				"gateway session runtime config is not valid JSON",
			)
		}
		session.RuntimeConfig = append(
			json.RawMessage(nil), session.RuntimeConfig...,
		)
	}
	plugins := make([]PluginBinding, 0, len(session.Plugins))
	seen := make(map[string]struct{}, len(session.Plugins))
	for _, binding := range session.Plugins {
		normalized, err := normalizeBinding(binding)
		if err != nil {
			return Session{}, err
		}
		if _, exists := seen[normalized.Name]; exists {
			return Session{}, fmt.Errorf(
				"gateway session contains plugin %q more than once",
				normalized.Name,
			)
		}
		seen[normalized.Name] = struct{}{}
		plugins = append(plugins, normalized)
	}
	slices.SortFunc(plugins, func(left, right PluginBinding) int {
		return strings.Compare(left.Name, right.Name)
	})
	session.Plugins = plugins
	session.CreatedAt = session.CreatedAt.UTC()
	session.UpdatedAt = session.UpdatedAt.UTC()
	return session, nil
}

func normalizeBinding(binding PluginBinding) (PluginBinding, error) {
	binding.Namespace = strings.TrimSpace(binding.Namespace)
	binding.Name = strings.TrimSpace(binding.Name)
	binding.InstanceID = strings.TrimSpace(binding.InstanceID)
	binding.URI = strings.TrimSpace(binding.URI)
	if err := sdk.ValidateResourceName(
		"registry namespace",
		binding.Namespace,
	); err != nil {
		return PluginBinding{}, err
	}
	if err := sdk.ValidateResourceName("plugin", binding.Name); err != nil {
		return PluginBinding{}, err
	}
	if err := sdk.ValidateResourceName(
		"plugin instance",
		binding.InstanceID,
	); err != nil {
		return PluginBinding{}, err
	}
	if binding.Epoch == 0 {
		return PluginBinding{}, errors.New("plugin binding epoch must be positive")
	}
	parsed, err := url.Parse(binding.URI)
	if err != nil {
		return PluginBinding{}, fmt.Errorf(
			"parse plugin binding URI %q: %w",
			binding.URI,
			err,
		)
	}
	if parsed.Scheme == "" ||
		(parsed.Host == "" && parsed.Opaque == "" && parsed.Path == "") {
		return PluginBinding{}, fmt.Errorf(
			"plugin binding URI %q has no scheme or target",
			binding.URI,
		)
	}
	if err := binding.Manifest.Validate(); err != nil {
		return PluginBinding{}, fmt.Errorf(
			"validate plugin binding manifest: %w",
			err,
		)
	}
	if binding.Manifest.Name != binding.Name {
		return PluginBinding{}, fmt.Errorf(
			"plugin binding name %q does not match manifest name %q",
			binding.Name,
			binding.Manifest.Name,
		)
	}
	binding.Manifest = sdk.CloneManifest(binding.Manifest)
	binding.Labels = maps.Clone(binding.Labels)
	return binding, nil
}

func validateUserID(value string) error {
	if value == "" {
		return errors.New("gateway session user ID is empty")
	}
	if len(value) > 256 {
		return errors.New("gateway session user ID exceeds 256 bytes")
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return errors.New("gateway session user ID contains control characters")
		}
	}
	return nil
}

func normalizeUserID(value string) (string, error) {
	value = strings.TrimSpace(value)
	if err := validateUserID(value); err != nil {
		return "", err
	}
	return value, nil
}

func cloneSession(session Session) Session {
	session.Plugins = clonePluginBindings(session.Plugins)
	session.RuntimeConfig = append(
		json.RawMessage(nil), session.RuntimeConfig...,
	)
	return session
}

func clonePluginBindings(bindings []PluginBinding) []PluginBinding {
	plugins := slices.Clone(bindings)
	for index := range plugins {
		plugins[index].Manifest = sdk.CloneManifest(
			plugins[index].Manifest,
		)
		plugins[index].Labels = maps.Clone(plugins[index].Labels)
	}
	return plugins
}

func prepareSessionUpdate(
	current Session,
	replacement Session,
	expectedRevision uint64,
	now time.Time,
) (Session, error) {
	if current.Revision != expectedRevision {
		return Session{}, fmt.Errorf(
			"%w: session %s has revision %d, expected %d",
			ErrSessionConflict,
			current.ID,
			current.Revision,
			expectedRevision,
		)
	}
	if current.UserID != replacement.UserID {
		return Session{}, fmt.Errorf(
			"gateway session %s user ID is immutable",
			current.ID,
		)
	}
	if current.Revision == math.MaxUint64 {
		return Session{}, fmt.Errorf(
			"%w: session %s revision is exhausted",
			ErrSessionConflict,
			current.ID,
		)
	}
	replacement.Revision = current.Revision + 1
	replacement.CreatedAt = current.CreatedAt
	replacement.UpdatedAt = now.UTC()
	return replacement, nil
}

func validatePage(request sdk.PageRequest) (sdk.PageRequest, error) {
	if request.Limit == 0 {
		request.Limit = sdk.DefaultPageSize
	}
	if request.Limit < 1 || request.Limit > sdk.MaxPageSize {
		return sdk.PageRequest{}, fmt.Errorf(
			"gateway session page limit must be between 1 and %d",
			sdk.MaxPageSize,
		)
	}
	return request, nil
}

func listSessions(
	sessions map[string]Session,
	request sdk.PageRequest,
) SessionPage {
	return listSessionMatches(
		sessions,
		request,
		func(Session) bool { return true },
	)
}

func listSessionsByUser(
	sessions map[string]Session,
	userID string,
	request sdk.PageRequest,
) SessionPage {
	return listSessionMatches(
		sessions,
		request,
		func(session Session) bool { return session.UserID == userID },
	)
}

func listSessionMatches(
	sessions map[string]Session,
	request sdk.PageRequest,
	match func(Session) bool,
) SessionPage {
	ids := make([]string, 0, len(sessions))
	for id, session := range sessions {
		if id > request.After && match(session) {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	limit := min(request.Limit, len(ids))
	page := SessionPage{Items: make([]Session, 0, limit)}
	for _, id := range ids[:limit] {
		page.Items = append(page.Items, cloneSession(sessions[id]))
	}
	if len(ids) > request.Limit {
		page.Next = ids[request.Limit-1]
	}
	return page
}
