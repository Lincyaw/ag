package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lincyaw/ag/sdk"
	"github.com/lincyaw/ag/sdk/runtime/internal/durability"
)

type SessionConfig struct {
	ID           string
	Provider     string
	System       string
	MaxTurns     int
	ResumePolicy ResumePolicy
}

type ResumePolicy string

const (
	// ResumeExact restores provider/system checkpoint state and rebuilds the
	// trajectory's recorded composition from currently mounted resources.
	ResumeExact ResumePolicy = "exact"
	// ResumeCurrent restores messages but deliberately uses the caller's current
	// provider/system configuration and mounted composition.
	ResumeCurrent ResumePolicy = "current"
)

type Session struct {
	runtime        *Runtime
	config         SessionConfig
	mu             sync.Mutex
	executionMu    sync.Mutex
	executionID    string
	executionToken string
	messages       []sdk.Message
	head           string
	pinnedSnapshot *registrySnapshot
	causal         causalInvocationScope
}

type Result struct {
	Output     string        `json:"output"`
	Messages   []sdk.Message `json:"messages"`
	Turns      int           `json:"turns"`
	ToolCalls  int           `json:"tool_calls"`
	Generation uint64        `json:"generation"`
	Cause      sdk.Cause     `json:"cause"`
}

type trajectorySessionProjection struct {
	Metadata       sdk.TrajectoryMetadata
	Config         SessionConfig
	Head           string
	Messages       []sdk.Message
	PinnedSnapshot *registrySnapshot
}

type trajectorySessionCreation struct {
	Label                string
	Config               SessionConfig
	Snapshot             *registrySnapshot
	Lineage              *trajectorySessionLineage
	PinExecutionSnapshot bool
	Invocation           *sdk.Invocation
}

func (runtime *Runtime) projectTrajectorySession(
	projection trajectorySessionProjection,
) *Session {
	session := &Session{
		runtime:        runtime,
		config:         projection.Config,
		messages:       sdk.CloneMessages(projection.Messages),
		head:           projection.Head,
		pinnedSnapshot: projection.PinnedSnapshot,
	}
	session.applyTrajectoryOrigin(projection.Metadata.Environment)
	return session
}

func (runtime *Runtime) projectResumeBaseSession(
	ctx context.Context,
	metadata sdk.TrajectoryMetadata,
	config SessionConfig,
	pinnedSnapshot *registrySnapshot,
) (*Session, error) {
	base, err := durability.LoadSessionResumeBase(
		ctx,
		runtime.trajectories,
		metadata,
	)
	if err != nil {
		return nil, err
	}
	return runtime.projectSessionFromResumeBase(
		metadata,
		config,
		base,
		base.Head,
		pinnedSnapshot,
	), nil
}

func (runtime *Runtime) projectSessionFromResumeBase(
	metadata sdk.TrajectoryMetadata,
	config SessionConfig,
	base durability.SessionResumeBase,
	head string,
	pinnedSnapshot *registrySnapshot,
) *Session {
	return runtime.projectTrajectorySession(trajectorySessionProjection{
		Metadata:       metadata,
		Config:         config,
		Head:           head,
		Messages:       base.Messages,
		PinnedSnapshot: pinnedSnapshot,
	})
}

func (runtime *Runtime) createTrajectorySessionFromSnapshot(
	ctx context.Context,
	creation trajectorySessionCreation,
) (*Session, error) {
	if creation.Snapshot == nil {
		return nil, errors.New("trajectory session creation snapshot is nil")
	}
	label := creation.Label
	if label == "" {
		label = "session"
	}
	executionSnapshot, environment, err := newExecutionEnvironment(
		runtime,
		creation.Snapshot,
		creation.Config,
	)
	if err != nil {
		return nil, err
	}
	trajectory := sdk.Trajectory{
		ID:          creation.Config.ID,
		Environment: environment,
	}
	if creation.Lineage != nil {
		lineage := *creation.Lineage
		lineage.applyEnvironment(&environment)
		trajectory = lineage.trajectory(creation.Config.ID, environment)
	}
	if err := runtime.trajectories.Create(ctx, trajectory); err != nil {
		return nil, fmt.Errorf(
			"create %s trajectory %q: %w",
			label,
			creation.Config.ID,
			err,
		)
	}
	metadata, err := runtime.trajectories.LoadMetadata(ctx, creation.Config.ID)
	if err != nil {
		return nil, fmt.Errorf(
			"load created %s trajectory %q: %w",
			label,
			creation.Config.ID,
			err,
		)
	}
	var pinnedSnapshot *registrySnapshot
	if creation.PinExecutionSnapshot {
		pinnedSnapshot = executionSnapshot
	}
	session, err := runtime.projectResumeBaseSession(
		ctx,
		metadata,
		creation.Config,
		pinnedSnapshot,
	)
	if err != nil {
		return nil, err
	}
	if creation.Invocation != nil {
		session.applyInvocationScope(*creation.Invocation)
	}
	return session, nil
}

func (runtime *Runtime) NewSession(
	ctx context.Context,
	config SessionConfig,
) (*Session, error) {
	if err := validateSessionConfig(runtime, &config); err != nil {
		return nil, err
	}
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return nil, err
	}
	defer releaseWork()
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return nil, err
	}
	defer lease.release()
	return runtime.createTrajectorySessionFromSnapshot(
		ctx,
		trajectorySessionCreation{
			Label:    "session",
			Config:   config,
			Snapshot: lease.snapshot,
		},
	)
}

func (runtime *Runtime) ResumeSession(
	ctx context.Context,
	id string,
	config SessionConfig,
) (*Session, error) {
	config.ID = id
	if err := validateSessionConfig(runtime, &config); err != nil {
		return nil, err
	}
	releaseWork, err := runtime.beginTrajectoryWork()
	if err != nil {
		return nil, err
	}
	defer releaseWork()
	metadata, err := runtime.trajectories.LoadMetadata(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load session trajectory %q: %w", id, err)
	}
	if metadata.Execution != nil && !metadata.Execution.Terminal() {
		return nil, fmt.Errorf(
			"%w: trajectory %s has active execution %s; call RecoverExecution",
			sdk.ErrTrajectoryExecution,
			id,
			metadata.Execution.ID,
		)
	}
	base, err := durability.LoadSessionResumeBase(
		ctx,
		runtime.trajectories,
		metadata,
	)
	if err != nil {
		return nil, err
	}
	resumeHead := base.Head
	checkpoint := base.Checkpoint
	checkpointEntry := base.CheckpointEntry
	var pinnedSnapshot *registrySnapshot
	var pinnedLease *snapshotLease
	defer func() {
		if pinnedLease != nil {
			pinnedLease.release()
		}
	}()
	if config.ResumePolicy == ResumeExact {
		recordedEnvironment, environmentErr := checkpointResumeEnvironment(
			ctx,
			runtime.trajectories,
			metadata,
			checkpointEntry,
		)
		if environmentErr != nil {
			return nil, environmentErr
		}
		projection, projectionErr := runtime.acquireExactResumeProjection(
			metadata.Environment,
			config,
			checkpoint,
			recordedEnvironment,
		)
		if projectionErr != nil {
			return nil, projectionErr
		}
		config = projection.Config
		pinnedLease = projection.Lease
		pinnedSnapshot = projection.snapshot()
	}
	head := metadata.Head
	moveHead, err := runtime.trajectoryHeadNeedsMove(
		ctx,
		metadata.ID,
		metadata.Head,
		resumeHead,
	)
	if err != nil {
		return nil, err
	}
	if moveHead {
		var eventLease *snapshotLease
		if pinnedSnapshot != nil {
			eventLease = pinnedLease
		} else {
			eventLease, err = runtime.acquireSnapshot()
		}
		if err != nil {
			return nil, err
		}
		head, err = runtime.moveTrajectoryHead(
			ctx,
			eventLease.snapshot,
			metadata.ID,
			metadata.Head,
			resumeHead,
			sdk.TrajectoryKindRestore,
		)
		eventLease.release()
		if err != nil {
			return nil, fmt.Errorf("restore session trajectory %q: %w", id, err)
		}
	}
	return runtime.projectSessionFromResumeBase(
		metadata,
		config,
		base,
		head,
		pinnedSnapshot,
	), nil
}

func validateSessionConfig(runtime *Runtime, config *SessionConfig) error {
	if runtime == nil {
		return errors.New("runtime is nil")
	}
	if config.ID == "" {
		config.ID = sdk.NewID()
	}
	if err := sdk.ValidateResourceName("session", config.ID); err != nil {
		return err
	}
	if config.MaxTurns == 0 {
		config.MaxTurns = 8
	}
	if config.MaxTurns < 1 {
		return errors.New("session max turns must be positive")
	}
	if config.ResumePolicy == "" {
		config.ResumePolicy = ResumeExact
	}
	switch config.ResumePolicy {
	case ResumeExact, ResumeCurrent:
	default:
		return fmt.Errorf("unknown resume policy %q", config.ResumePolicy)
	}
	return nil
}

func (session *Session) ID() string {
	return session.config.ID
}

func (session *Session) acquireSnapshot() (*snapshotLease, error) {
	if session.pinnedSnapshot != nil {
		return session.runtime.acquireRegistrySnapshot(
			session.pinnedSnapshot,
		)
	}
	return session.runtime.acquireSnapshot()
}

func (session *Session) pinExecutionSnapshot(
	environment sdk.TrajectoryEnvironment,
) (*snapshotLease, func(), error) {
	if session.pinnedSnapshot != nil {
		lease, err := session.runtime.acquireRegistrySnapshot(
			session.pinnedSnapshot,
		)
		return lease, func() {}, err
	}
	currentLease, err := session.runtime.acquireSnapshot()
	if err != nil {
		return nil, nil, err
	}
	executionLease, err := session.runtime.acquireResolvedResumeSnapshot(
		currentLease,
		environment,
		newResumeEnvironment(environment),
		session.config,
	)
	currentLease.release()
	if err != nil {
		return nil, nil, err
	}
	session.pinnedSnapshot = executionLease.snapshot
	return executionLease, func() { session.pinnedSnapshot = nil }, nil
}

func (session *Session) Messages() []sdk.Message {
	session.mu.Lock()
	defer session.mu.Unlock()
	return sdk.CloneMessages(session.messages)
}
