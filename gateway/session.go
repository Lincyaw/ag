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

type PermissionRules struct {
	Allow []string `json:"allow,omitempty"`
	Ask   []string `json:"ask,omitempty"`
	Deny  []string `json:"deny,omitempty"`
}

// SessionSettings are the durable, user-facing controls captured atomically
// when a trajectory is created. Runtime credentials remain process-owned and
// never cross this boundary.
type SessionSettings struct {
	Title         string          `json:"title,omitempty"`
	Model         string          `json:"model,omitempty"`
	Models        []string        `json:"models,omitempty"`
	Tools         []string        `json:"tools,omitempty"`
	AutoCompact   *bool           `json:"auto_compact,omitempty"`
	ThinkingLevel string          `json:"thinking_level,omitempty"`
	Permissions   PermissionRules `json:"permissions,omitempty"`
}

// SessionPatch is the CAS-protected mutable control surface of one durable
// trajectory. Pointer fields distinguish "leave unchanged" from a deliberate
// empty/false value.
type SessionPatch struct {
	Title          *string         `json:"title,omitempty"`
	Model          *string         `json:"model,omitempty"`
	AutoCompact    *bool           `json:"auto_compact,omitempty"`
	ThinkingLevel  *string         `json:"thinking_level,omitempty"`
	PermissionRule *PermissionRule `json:"permission_rule,omitempty"`
}

type PermissionRule struct {
	Kind    string `json:"kind"`
	Pattern string `json:"pattern"`
}

type Session struct {
	ID            string   `json:"id"`
	UserID        string   `json:"user_id"`
	Title         string   `json:"title,omitempty"`
	Provider      string   `json:"provider,omitempty"`
	Model         string   `json:"model,omitempty"`
	Models        []string `json:"models,omitempty"`
	Tools         []string `json:"tools,omitempty"`
	System        string   `json:"system,omitempty"`
	MaxTurns      int      `json:"max_turns"`
	WorkspaceRoot string   `json:"workspace_root,omitempty"`
	// RuntimeConfig is a private, durable execution profile supplied when the
	// trajectory is created. It is deliberately excluded from API JSON so
	// listing or attaching to a trajectory cannot disclose tool configuration.
	RuntimeConfig json.RawMessage `json:"-"`
	AutoCompact   *bool           `json:"auto_compact,omitempty"`
	ThinkingLevel string          `json:"thinking_level,omitempty"`
	Permissions   PermissionRules `json:"permissions,omitempty"`
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
	session.Title = strings.TrimSpace(session.Title)
	session.Provider = strings.TrimSpace(session.Provider)
	session.Model = strings.TrimSpace(session.Model)
	session.ThinkingLevel = strings.ToLower(strings.TrimSpace(session.ThinkingLevel))
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
	if len([]rune(session.Title)) > 256 {
		return Session{}, errors.New("gateway session title exceeds 256 characters")
	}
	models, err := normalizeStringSet("gateway session model", session.Models, 128)
	if err != nil {
		return Session{}, err
	}
	session.Models = models
	session.Tools, err = normalizeStringSet("gateway session tool", session.Tools, 128)
	if err != nil {
		return Session{}, err
	}
	if session.Model != "" && !slices.Contains(session.Models, session.Model) {
		session.Models = append(session.Models, session.Model)
		slices.Sort(session.Models)
	}
	switch session.ThinkingLevel {
	case "", "off", "low", "medium", "high", "xhigh":
	default:
		return Session{}, fmt.Errorf(
			"gateway session thinking level %q is invalid",
			session.ThinkingLevel,
		)
	}
	session.Permissions, err = normalizePermissionRules(session.Permissions)
	if err != nil {
		return Session{}, err
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
	session.Models = slices.Clone(session.Models)
	session.Tools = slices.Clone(session.Tools)
	session.Permissions = clonePermissionRules(session.Permissions)
	if session.AutoCompact != nil {
		enabled := *session.AutoCompact
		session.AutoCompact = &enabled
	}
	session.RuntimeConfig = append(
		json.RawMessage(nil), session.RuntimeConfig...,
	)
	return session
}

func normalizePermissionRules(rules PermissionRules) (PermissionRules, error) {
	var err error
	rules.Allow, err = normalizeStringSet("allow permission rule", rules.Allow, 512)
	if err != nil {
		return PermissionRules{}, err
	}
	rules.Ask, err = normalizeStringSet("ask permission rule", rules.Ask, 512)
	if err != nil {
		return PermissionRules{}, err
	}
	rules.Deny, err = normalizeStringSet("deny permission rule", rules.Deny, 512)
	if err != nil {
		return PermissionRules{}, err
	}
	return rules, nil
}

func normalizeStringSet(kind string, values []string, maxRunes int) ([]string, error) {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if len([]rune(value)) > maxRunes {
			return nil, fmt.Errorf("%s exceeds %d characters", kind, maxRunes)
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	slices.Sort(result)
	return result, nil
}

func clonePermissionRules(rules PermissionRules) PermissionRules {
	rules.Allow = slices.Clone(rules.Allow)
	rules.Ask = slices.Clone(rules.Ask)
	rules.Deny = slices.Clone(rules.Deny)
	return rules
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
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
