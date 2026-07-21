// Package app defines the controller seam the TUI drives.
//
// The migrated TUI keeps its local presentation state here while every
// durable or executable action is delegated to the gateway-backed Controller.
//
// Only the symbols the TUI references are exposed:
//
//	App, New, WithReadOnly,
//	(*App).Run, CompactSession, Interrupt, ResolveInput, RunBangCommand, RunSkillFork,
//	SetAutoCompact,
//	UpdateSessionTitle, CurrentAgentSkills,
//	CurrentAgentCommands, IsReadOnly, ShouldExitAfterFirstResponse,
//	SkillCommandFork, Session,
//	ErrTitleGenerating, ErrNothingToUndo.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	tea "charm.land/bubbletea/v2"

	"github.com/lincyaw/ag/internal/cagent/chat"
	"github.com/lincyaw/ag/internal/cagent/config/types"
	"github.com/lincyaw/ag/internal/cagent/runtime"
	"github.com/lincyaw/ag/internal/cagent/session"
	"github.com/lincyaw/ag/internal/cagent/skills"
	"github.com/lincyaw/ag/internal/cagent/tools"
	"github.com/lincyaw/ag/internal/tui/messages"
)

// Controller is the behavioural seam between the vendored cagent App surface
// (which the TUI drives, see pkg/tui) and the live backend. In cagent the
// backend is a local runtime.Runtime; in the ag peer it is the
// gateway wire protocol, implemented by internal/adapter.
//
// Only the methods that actually need to reach the backend are listed here;
// everything else on App stays local presentation state. A nil Controller is
// used only by isolated/read-only views and unit tests.
//
// The Controller never sends events back directly: it pushes runtime.Event
// values into the App via App.EmitEvent, which fans them out to every
// SubscribeWith consumer (the TUI supervisor).
type Controller interface {
	// Run sends one user turn to the backend.
	Run(ctx context.Context, cancel context.CancelFunc, message string, attachments []messages.Attachment)
	// RunCooperative sends one user turn without interrupting an active run.
	RunCooperative(ctx context.Context, cancel context.CancelFunc, message string, attachments []messages.Attachment)
	// RunWithMessage sends a pre-built message (e.g. with attachments).
	RunWithMessage(ctx context.Context, cancel context.CancelFunc, msg *session.Message)
	// CompactSession asks the backend to summarise + compact history.
	CompactSession(ctx context.Context, cancel context.CancelFunc, additionalPrompt string)
	// Resume delivers a tool-confirmation decision to a waiting tool call.
	Resume(req runtime.ResumeRequest)
	// Interrupt asks the backend to stop the active turn.
	Interrupt()
	// TogglePause changes whether queued inputs may begin executing.
	TogglePause() (paused, supported bool)
	// CancelBackground asks the backend to stop a detached background task.
	CancelBackground(taskID string)
	// UpdateSessionTitle sets and persists the session title.
	UpdateSessionTitle(ctx context.Context, title string) error
	// RunBangCommand runs a shell command and surfaces its output.
	RunBangCommand(ctx context.Context, cancel context.CancelFunc, command string)
	// NewSession starts a fresh session on the backend.
	NewSession()
	// ClearSession starts a fresh backend session without echoing /new.
	ClearSession()
	// FirstMessage returns the queued first message, if any, to send on launch.
	FirstMessage() (content string, ok bool)
	// SwitchModel asks the backend to switch the active model profile by name.
	SwitchModel(name string, sessionOnly bool)
	// SetAutoCompact toggles backend automatic context compaction for this session.
	SetAutoCompact(enabled bool)
	// SetPermissionRule applies a session-scoped permission rule on the backend.
	SetPermissionRule(kind, pattern string)
	// SetThinkingMode toggles extended thinking for subsequent submitted turns.
	SetThinkingMode(enabled bool)
	// SetThinkingLevel sets the backend thinking level for subsequent turns.
	SetThinkingLevel(level string)
}

// UndoSnapshotResult reports the outcome of an undo/reset operation.
type UndoSnapshotResult struct {
	RestoredFiles int
}

// ErrTitleGenerating is returned when attempting to set a title while
// generation is in progress.
var ErrTitleGenerating = errors.New("title generation in progress, please wait")

// ErrNothingToUndo is returned when an undo/reset is requested but no snapshot
// is available to restore.
var ErrNothingToUndo = errors.New("nothing to undo")

// App is the controller the TUI drives. The adapter supplies the live
// implementation via a Controller; without one the methods are stubs.
type App struct {
	session                *session.Session
	exitAfterFirstResponse bool
	readOnly               bool

	controller Controller

	// agentInfoMu guards the session_ready-sourced agent capability view
	// (tool/command names, model list, active model). The translator writes it
	// from the wire-reader goroutine; the TUI reads it from the bubbletea
	// goroutine when opening /tools, /skills (commands) and the model picker.
	agentInfoMu   sync.Mutex
	toolNames     []string
	commandNames  []string
	modelNames    []string
	activeModel   string
	thinkingLevel string
	skills        []skills.Skill

	// events is the raw event stream the backend pushes into via EmitEvent;
	// startFanOut drains it and scatters every event to each SubscribeWith
	// consumer. Mirrors cagent pkg/app's fan-out so the vendored TUI's
	// supervisor sees the same subscription semantics.
	events     chan runtime.Event
	subsMu     sync.Mutex
	subs       []chan tea.Msg
	fanoutOnce sync.Once
}

const (
	eventsBufferSize     = 256
	subscriberBufferSize = 1024
)

// Opt is an option for creating a new App.
type Opt func(*App)

// WithReadOnly marks the session as read-only: the conversation history is
// displayed but no new messages can be sent to the LLM.
func WithReadOnly() Opt {
	return func(a *App) {
		a.readOnly = true
	}
}

// WithController wires the live backend the App delegates to. The adapter
// passes its wire-backed Controller here.
func WithController(c Controller) Opt {
	return func(a *App) {
		a.controller = c
	}
}

// WithExitAfterFirstResponse makes ShouldExitAfterFirstResponse report true.
func WithExitAfterFirstResponse() Opt {
	return func(a *App) {
		a.exitAfterFirstResponse = true
	}
}

// New creates a new App for the given session.
//
// The cagent original also threads a runtime.Runtime and a title generator;
// those belong to the adapter, which supplies a Controller via WithController.
func New(ctx context.Context, sess *session.Session, opts ...Opt) *App {
	_ = ctx
	app := &App{
		session: sess,
		events:  make(chan runtime.Event, eventsBufferSize),
	}
	for _, opt := range opts {
		opt(app)
	}
	return app
}

// EmitEvent pushes a runtime event into the App's stream so it fans out to
// every SubscribeWith consumer. Called by the adapter's translator. Non-blocking
// on a full buffer (the event is dropped) so a slow TUI cannot stall the wire
// reader.
func (a *App) EmitEvent(ev runtime.Event) {
	if ev == nil {
		return
	}
	select {
	case a.events <- ev:
	default:
	}
}

// Run one agent loop. Delegates to the wire-backed controller.
func (a *App) Run(ctx context.Context, cancel context.CancelFunc, message string, attachments []messages.Attachment) {
	if a.controller != nil {
		a.controller.Run(ctx, cancel, message, attachments)
	}
}

// RunCooperative sends one user turn without interrupting an active backend run.
func (a *App) RunCooperative(ctx context.Context, cancel context.CancelFunc, message string, attachments []messages.Attachment) {
	if a.controller != nil {
		a.controller.RunCooperative(ctx, cancel, message, attachments)
	}
}

// CompactSession generates a summary and compacts the session history.
func (a *App) CompactSession(ctx context.Context, cancel context.CancelFunc, additionalPrompt string) {
	if a.controller != nil {
		a.controller.CompactSession(ctx, cancel, additionalPrompt)
	}
}

// HasController reports whether this App is backed by a live gateway/runtime
// controller. Gateway-owned commands should let the backend decide whether the
// requested action is possible because the local cagent session is only a UI
// mirror.
func (a *App) HasController() bool {
	return a != nil && a.controller != nil
}

// SetAutoCompact toggles automatic context compaction on the backend session.
func (a *App) SetAutoCompact(enabled bool) {
	if a.controller != nil {
		a.controller.SetAutoCompact(enabled)
	}
}

// SetThinkingMode toggles extended thinking for subsequent submitted turns.
func (a *App) SetThinkingMode(enabled bool) {
	a.agentInfoMu.Lock()
	if enabled {
		if a.thinkingLevel == "" || a.thinkingLevel == "off" {
			a.thinkingLevel = "high"
		}
	} else {
		a.thinkingLevel = "off"
	}
	a.agentInfoMu.Unlock()
	if a.controller != nil {
		a.controller.SetThinkingMode(enabled)
	}
}

// SetThinkingLevel sets the thinking level for subsequent submitted turns.
func (a *App) SetThinkingLevel(level string) {
	a.agentInfoMu.Lock()
	a.thinkingLevel = strings.ToLower(strings.TrimSpace(level))
	a.agentInfoMu.Unlock()
	if a.controller != nil {
		a.controller.SetThinkingLevel(level)
	}
}

// TrackThinkingLevel refreshes the frontend projection without issuing a
// backend mutation (for attach/reconnect events).
func (a *App) TrackThinkingLevel(level string) {
	a.agentInfoMu.Lock()
	a.thinkingLevel = strings.ToLower(strings.TrimSpace(level))
	a.agentInfoMu.Unlock()
}

// Interrupt asks the backend to stop the active turn.
func (a *App) Interrupt() {
	if a.controller != nil {
		a.controller.Interrupt()
	}
}

// CancelBackground asks the backend to stop a detached background task.
func (a *App) CancelBackground(taskID string) {
	if a.controller != nil {
		a.controller.CancelBackground(taskID)
	}
}

// ResolveInput resolves user input (skill commands first, then agent commands)
// into the content ready to send to the agent. Stub: returns the input unchanged.
func (a *App) ResolveInput(ctx context.Context, input string) string {
	_ = ctx
	return input
}

// RunBangCommand runs a shell command and surfaces its output as a runtime
// event. Delegates to the wire-backed controller.
func (a *App) RunBangCommand(ctx context.Context, cancel context.CancelFunc, command string) {
	if a.controller != nil {
		a.controller.RunBangCommand(ctx, cancel, command)
	}
}

// RunSkillFork dispatches a fork-mode skill in an isolated sub-session.
// The wire protocol has no fork-skill notion; treat it as a normal turn that
// names the skill and task so the gateway scenario can dispatch it.
func (a *App) RunSkillFork(ctx context.Context, cancel context.CancelFunc, skillName, task string, attachments []messages.Attachment) {
	if a.controller != nil {
		a.controller.Run(ctx, cancel, "/"+skillName+" "+task, attachments)
	}
}

// UpdateSessionTitle updates the current session's title and persists it.
func (a *App) UpdateSessionTitle(ctx context.Context, title string) error {
	if a.controller != nil {
		return a.controller.UpdateSessionTitle(ctx, title)
	}
	return nil
}

// CurrentAgentSkills returns the available skills for the current agent,
// sourced from the welcome-handshake capability block (skills are gateway
// commands under the "skill" namespace). Empty until SetSkills runs.
func (a *App) CurrentAgentSkills() []skills.Skill {
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	return slices.Clone(a.skills)
}

// SetSkills records the skill catalog projected from the welcome handshake.
// Entries carry listing metadata such as name, summary, and source directory;
// the wire protocol does not carry skill bodies.
func (a *App) SetSkills(skillList []skills.Skill) {
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	a.skills = slices.Clone(skillList)
}

// SetAgentInfo records the capability view projected from a session_ready
// frame: the tool/command names, the selectable model-profile names and the
// active model. The translator calls this so /tools, /skills (commands) and the
// model picker populate. Slices are cloned to decouple from the caller's
// decoded wire maps.
func (a *App) SetAgentInfo(toolNames, commandNames, modelNames []string, activeModel string) {
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	a.toolNames = slices.Clone(toolNames)
	a.commandNames = slices.Clone(commandNames)
	a.modelNames = slices.Clone(modelNames)
	a.activeModel = activeModel
}

// CurrentModel returns the active model profile last advertised by session_ready
// or model-switch acknowledgement events.
func (a *App) CurrentModel(ctx context.Context) string {
	_ = ctx
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	return a.activeModel
}

// CurrentAgentCommands returns the commands for the active agent, sourced from
// the most recent session_ready frame. The wire protocol carries only command
// names, so each command has an empty body (name-only completion entries).
func (a *App) CurrentAgentCommands(ctx context.Context) types.Commands {
	_ = ctx
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	if len(a.commandNames) == 0 {
		return nil
	}
	cmds := make(types.Commands, len(a.commandNames))
	for _, name := range a.commandNames {
		cmds[name] = types.Command{}
	}
	return cmds
}

// SkillCommandFork reports whether input is a slash command for a fork-mode
// skill and, if so, returns the skill name and task. Stub: always reports false.
func (a *App) SkillCommandFork(ctx context.Context, input string) (skillName, task string, ok bool) {
	_, _ = ctx, input
	return "", "", false
}

// IsReadOnly reports whether the session is read-only.
func (a *App) IsReadOnly() bool {
	return a.readOnly
}

// ShouldExitAfterFirstResponse reports whether the app should exit after the
// first assistant response completes.
func (a *App) ShouldExitAfterFirstResponse() bool {
	return a.exitAfterFirstResponse
}

// Session returns the current session.
func (a *App) Session() *session.Session {
	return a.session
}

// SetSession replaces the current session view-model.
func (a *App) SetSession(sess *session.Session) {
	if sess != nil {
		a.session = sess
	}
}

// SubscribeWith subscribes to app events using a custom send function.
// Multiple concurrent subscribers are supported: a single fan-out goroutine
// drains the event stream and dispatches a copy to each one. Slow subscribers
// drop events rather than block the bus. Mirrors cagent pkg/app.SubscribeWith.
func (a *App) SubscribeWith(ctx context.Context, send func(tea.Msg)) {
	ch := make(chan tea.Msg, subscriberBufferSize)
	a.addSubscriber(ch)
	defer a.removeSubscriber(ch)

	a.fanoutOnce.Do(a.startFanOut)

	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-ch:
			send(msg)
		}
	}
}

func (a *App) addSubscriber(ch chan tea.Msg) {
	a.subsMu.Lock()
	defer a.subsMu.Unlock()
	a.subs = append(a.subs, ch)
}

func (a *App) removeSubscriber(ch chan tea.Msg) {
	a.subsMu.Lock()
	defer a.subsMu.Unlock()
	a.subs = slices.DeleteFunc(a.subs, func(c chan tea.Msg) bool { return c == ch })
}

// startFanOut runs once per App. It scatters every event to all
// currently-registered subscribers. Sends are non-blocking; if a subscriber's
// buffer is full the event is dropped for that subscriber so one slow consumer
// cannot stall the others.
func (a *App) startFanOut() {
	go func() {
		for ev := range a.events {
			var msg tea.Msg = ev
			a.subsMu.Lock()
			subs := slices.Clone(a.subs)
			a.subsMu.Unlock()
			for _, ch := range subs {
				select {
				case ch <- msg:
				default:
				}
			}
		}
	}()
}

// SnapshotsEnabled reports whether session snapshots (undo) are available.
// Stub: returns false until the adapter takes over.
func (a *App) SnapshotsEnabled() bool {
	return false
}

// SendFirstMessage returns a command that sends the first message of the
// session. When the controller has a queued first message it is emitted as a
// SendMsg so it flows through the normal TUI send path (queueing, title
// generation, event fan-out). Returns nil when there is no first message.
func (a *App) SendFirstMessage() tea.Cmd {
	if a.controller == nil {
		return nil
	}
	content, ok := a.controller.FirstMessage()
	if !ok {
		return nil
	}
	return func() tea.Msg {
		return messages.SendMsg{Content: content}
	}
}

// RunWithMessage runs one agent loop seeded with a pre-built message.
// Delegates to the wire-backed controller.
func (a *App) RunWithMessage(ctx context.Context, cancel context.CancelFunc, msg *session.Message) {
	if a.controller != nil {
		a.controller.RunWithMessage(ctx, cancel, msg)
	}
}

// Resume delivers a tool-confirmation decision to a waiting tool call.
// Delegates to the wire-backed controller.
func (a *App) Resume(req runtime.ResumeRequest) {
	if a.controller != nil {
		a.controller.Resume(req)
	}
}

// TogglePause toggles the durable gateway queue pause state.
func (a *App) TogglePause() (paused, supported bool) {
	if a.controller == nil {
		return false, false
	}
	return a.controller.TogglePause()
}

// NewSession starts a fresh session. Delegates to the wire-backed controller.
func (a *App) NewSession() {
	if a.controller != nil {
		a.controller.NewSession()
	}
}

func (a *App) ClearSession() {
	if a.controller != nil {
		a.controller.ClearSession()
	}
}

// SwitchAgent switches the active agent. The gateway currently exposes one
// agent per trajectory, so there is no backend transition to perform.
func (a *App) SwitchAgent(agentName string) error {
	_ = agentName
	return nil
}

// SetCurrentAgentModel persists the active trajectory's model. When
// sessionOnly is false the controller also updates the creation template used
// by new tabs in this frontend process.
func (a *App) SetCurrentAgentModel(
	ctx context.Context,
	modelRef string,
	sessionOnly ...bool,
) error {
	_ = ctx
	if modelRef == "" {
		return nil
	}
	if a.controller != nil {
		only := len(sessionOnly) > 0 && sessionOnly[0]
		a.controller.SwitchModel(modelRef, only)
	}
	return nil
}

// SupportsModelSwitching reports whether the current backend can switch models.
// True once a session_ready frame advertised at least one selectable model.
func (a *App) SupportsModelSwitching() bool {
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	return len(a.modelNames) > 0
}

// CycleAgentThinkingLevel advances and persists the active agent's reasoning
// effort level.
func (a *App) CycleAgentThinkingLevel(ctx context.Context) (string, error) {
	_ = ctx
	a.agentInfoMu.Lock()
	levels := []string{"off", "low", "medium", "high"}
	index := slices.Index(levels, a.thinkingLevel)
	if index < 0 {
		index = 0
	}
	next := levels[(index+1)%len(levels)]
	a.thinkingLevel = next
	a.agentInfoMu.Unlock()
	if a.controller != nil {
		a.controller.SetThinkingLevel(next)
	}
	return next, nil
}

// AvailableModels lists the models selectable for the current agent, sourced
// from the session_ready model-profile names. The wire protocol carries only
// names (no catalog/pricing metadata), so each choice is a bare name with its
// IsCurrent flag set to match the active model.
func (a *App) AvailableModels(ctx context.Context) []runtime.ModelChoice {
	_ = ctx
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	if len(a.modelNames) == 0 {
		return nil
	}
	choices := make([]runtime.ModelChoice, 0, len(a.modelNames))
	for _, name := range a.modelNames {
		choices = append(choices, runtime.ModelChoice{
			Name:      name,
			Ref:       name,
			IsCurrent: modelNameMatchesActive(name, a.activeModel),
		})
	}
	return choices
}

func modelNameMatchesActive(name, active string) bool {
	name = normalizeModelName(name)
	active = normalizeModelName(active)
	if name == "" || active == "" {
		return false
	}
	if name == active {
		return true
	}
	switch active {
	case "opus", "opus1m", "opus48", "opus481m", "opus481mcontext", "claudeopus48", "claudeopus481m":
		return name == "opus" || name == "opus1m" || name == "claudeopus48" || name == "claudeopus481m"
	case "sonnet", "sonnet5", "claudesonnet5":
		return name == "sonnet" || name == "claudesonnet5"
	case "sonnet1m", "sonnet51m", "sonnet51mcontext", "claudesonnet51m":
		return name == "sonnet1m" || name == "sonnet51m" || name == "claudesonnet51m"
	case "haiku", "haiku45", "claudehaiku45":
		return name == "haiku" || name == "claudehaiku45"
	default:
		return false
	}
}

func normalizeModelName(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(
		" ", "",
		"-", "",
		"_", "",
		".", "",
		"(", "",
		")", "",
	)
	value = replacer.Replace(value)
	value = strings.TrimPrefix(value, "default")
	return value
}

// PermissionsInfo returns the current session's permission patterns.
func (a *App) PermissionsInfo() *runtime.PermissionsInfo {
	if a.session == nil || a.session.Permissions == nil {
		return nil
	}
	return &runtime.PermissionsInfo{
		Allow: slices.Clone(a.session.Permissions.Allow),
		Ask:   slices.Clone(a.session.Permissions.Ask),
		Deny:  slices.Clone(a.session.Permissions.Deny),
	}
}

// AddPermissionRule records a permission rule on the current session.
func (a *App) AddPermissionRule(kind, pattern string) {
	if a.session == nil || pattern == "" {
		return
	}
	normalizedKind := normalizePermissionRuleKind(kind)
	if a.session.Permissions == nil {
		a.session.Permissions = &session.PermissionsConfig{}
	}
	switch normalizedKind {
	case "ask":
		a.session.Permissions.Ask = appendUniqueString(a.session.Permissions.Ask, pattern)
	case "deny":
		a.session.Permissions.Deny = appendUniqueString(a.session.Permissions.Deny, pattern)
	default:
		a.session.Permissions.Allow = appendUniqueString(a.session.Permissions.Allow, pattern)
	}
	if a.controller != nil {
		a.controller.SetPermissionRule(normalizedKind, pattern)
	}
}

// SyncPermissionRules applies the session's loaded permission rules to the
// live backend approval policy.
func (a *App) SyncPermissionRules() {
	if a == nil || a.controller == nil || a.session == nil || a.session.Permissions == nil {
		return
	}
	for _, pattern := range a.session.Permissions.Allow {
		a.controller.SetPermissionRule("allow", pattern)
	}
	for _, pattern := range a.session.Permissions.Ask {
		a.controller.SetPermissionRule("ask", pattern)
	}
	for _, pattern := range a.session.Permissions.Deny {
		a.controller.SetPermissionRule("deny", pattern)
	}
}

func appendUniqueString(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

func normalizePermissionRuleKind(kind string) string {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "ask":
		return "ask"
	case "deny":
		return "deny"
	default:
		return "allow"
	}
}

// LoadPermissionSettings reads Claude Code-compatible permission settings from
// user, project, and project-local settings files. Missing or malformed files
// are ignored so a bad Claude settings file cannot prevent AG startup.
func LoadPermissionSettings(workingDir string) *session.PermissionsConfig {
	if strings.TrimSpace(workingDir) == "" {
		workingDir, _ = os.Getwd()
	}
	var files []string
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		files = append(files, filepath.Join(home, ".claude", "settings.json"))
	}
	if workingDir != "" {
		files = append(files,
			filepath.Join(workingDir, ".claude", "settings.json"),
			filepath.Join(workingDir, ".claude", "settings.local.json"),
		)
	}

	perms := &session.PermissionsConfig{}
	for _, path := range files {
		mergePermissionSettingsFile(perms, path)
	}
	if len(perms.Allow) == 0 && len(perms.Ask) == 0 && len(perms.Deny) == 0 {
		return nil
	}
	return perms
}

type permissionSettingsFile struct {
	Permissions permissionSettingsBlock `json:"permissions"`
}

type permissionSettingsBlock struct {
	Allow []string `json:"allow"`
	Ask   []string `json:"ask"`
	Deny  []string `json:"deny"`
}

func mergePermissionSettingsFile(dst *session.PermissionsConfig, path string) {
	data, err := os.ReadFile(path)
	if err != nil || len(strings.TrimSpace(string(data))) == 0 {
		return
	}
	var file permissionSettingsFile
	if err := json.Unmarshal(data, &file); err != nil {
		return
	}
	dst.Allow = appendUniqueStrings(dst.Allow, file.Permissions.Allow)
	dst.Ask = appendUniqueStrings(dst.Ask, file.Permissions.Ask)
	dst.Deny = appendUniqueStrings(dst.Deny, file.Permissions.Deny)
}

func appendUniqueStrings(dst, src []string) []string {
	for _, value := range src {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		dst = appendUniqueString(dst, value)
	}
	return dst
}

// CurrentAgentTools returns the tools available to the active agent, sourced
// from the most recent session_ready frame. The wire protocol carries only tool
// names, so each tool is a name-only definition (no schema/description).
func (a *App) CurrentAgentTools(ctx context.Context) ([]tools.Tool, error) {
	_ = ctx
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	if len(a.toolNames) == 0 {
		return nil, nil
	}
	ts := make([]tools.Tool, 0, len(a.toolNames))
	for _, name := range a.toolNames {
		ts = append(ts, tools.Tool{Name: name})
	}
	return ts, nil
}

// ResolveCommand converts a /command into its prompt text. Stub: returns the
// input unchanged until the adapter takes over.
func (a *App) ResolveCommand(ctx context.Context, userInput string) string {
	_ = ctx
	return userInput
}

// LookupCommand parses userInput as a /command invocation. Stub: always reports
// no match until the adapter takes over.
func (a *App) LookupCommand(ctx context.Context, userInput string) (types.Command, string, bool) {
	_, _ = ctx, userInput
	return types.Command{}, "", false
}

// TrackCurrentAgentModel records the active agent's current model.
func (a *App) TrackCurrentAgentModel(model string) {
	model = strings.TrimSpace(model)
	if model == "" {
		return
	}
	a.agentInfoMu.Lock()
	defer a.agentInfoMu.Unlock()
	a.activeModel = model
}

// PlainTextTranscript renders the hydrated durable conversation locally.
func (a *App) PlainTextTranscript() string {
	if a == nil || a.session == nil {
		return ""
	}
	var builder strings.Builder
	for _, item := range a.session.GetAllMessages() {
		if item.Implicit {
			continue
		}
		label := strings.ToUpper(string(item.Message.Role))
		if item.AgentName != "" {
			label += " (" + item.AgentName + ")"
		}
		builder.WriteString(label)
		builder.WriteString(":\n")
		builder.WriteString(strings.TrimSpace(item.Message.Content))
		if len(item.Message.ToolCalls) > 0 {
			raw, _ := json.MarshalIndent(item.Message.ToolCalls, "", "  ")
			if builder.Len() > 0 {
				builder.WriteByte('\n')
			}
			builder.Write(raw)
		}
		builder.WriteString("\n\n")
	}
	result := strings.TrimSpace(builder.String())
	if result == "" {
		return ""
	}
	return result + "\n"
}

// ExportHTML writes a standalone, escaped transcript without consulting the
// backend because the attached session is already fully hydrated.
func (a *App) ExportHTML(ctx context.Context, filename string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if a == nil || a.session == nil {
		return "", errors.New("no active session")
	}
	filename = strings.TrimSpace(filename)
	if filename == "" {
		filename = "trajectory-" + a.session.ID + ".html"
	}
	if strings.ToLower(filepath.Ext(filename)) != ".html" {
		filename += ".html"
	}
	absolute, err := filepath.Abs(filename)
	if err != nil {
		return "", fmt.Errorf("resolve export path: %w", err)
	}
	title := strings.TrimSpace(a.session.Title)
	if title == "" {
		title = a.session.ID
	}
	transcript := html.EscapeString(a.PlainTextTranscript())
	document := "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width,initial-scale=1\"><title>" +
		html.EscapeString(title) +
		"</title><style>body{max-width:960px;margin:40px auto;padding:0 24px;background:#111;color:#eee;font:15px/1.55 ui-monospace,SFMono-Regular,Menlo,monospace}pre{white-space:pre-wrap;overflow-wrap:anywhere}</style></head><body><h1>" +
		html.EscapeString(title) + "</h1><pre>" + transcript + "</pre></body></html>\n"
	if err := os.WriteFile(absolute, []byte(document), 0o600); err != nil {
		return "", fmt.Errorf("write session export: %w", err)
	}
	return absolute, nil
}

// RegenerateSessionTitle derives the same deterministic first-turn title used
// during hydration and persists it through the controller.
func (a *App) RegenerateSessionTitle(ctx context.Context) error {
	if a == nil || a.session == nil {
		return errors.New("no active session")
	}
	for _, item := range a.session.GetAllMessages() {
		if item.Message.Role != chat.MessageRoleUser || item.Implicit {
			continue
		}
		line := strings.TrimSpace(strings.SplitN(item.Message.Content, "\n", 2)[0])
		if line == "" {
			continue
		}
		runes := []rune(line)
		if len(runes) > 48 {
			line = string(runes[:47]) + "…"
		}
		return a.UpdateSessionTitle(ctx, line)
	}
	return errors.New("cannot generate a title without a user message")
}

// SessionStore returns nil because durable sessions are accessed through the
// gateway navigator callbacks rather than the copied local store abstraction.
func (a *App) SessionStore() session.Store {
	return nil
}

// ReplaceSession swaps the active session. Stub: replaces the in-memory
// reference only; the adapter wires persistence.
func (a *App) ReplaceSession(ctx context.Context, sess *session.Session) {
	_ = ctx
	a.session = sess
}

// UndoLastSnapshot restores the most recent snapshot. Stub: returns a zero
// result and nil until the adapter takes over.
func (a *App) UndoLastSnapshot(ctx context.Context) (UndoSnapshotResult, error) {
	_ = ctx
	return UndoSnapshotResult{}, nil
}

// ResetSnapshot restores history, keeping the given number of entries.
// Stub: returns a zero result and nil until the adapter takes over.
func (a *App) ResetSnapshot(ctx context.Context, keep int) (UndoSnapshotResult, error) {
	_, _ = ctx, keep
	return UndoSnapshotResult{}, nil
}

// ListSnapshots returns the available snapshot indices. Stub: returns nil until
// the adapter takes over.
func (a *App) ListSnapshots() []int {
	return nil
}
