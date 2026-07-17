package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lincyaw/ag/sdk"
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
	// ResumeExact restores provider/system checkpoint state and requires the
	// current mounted composition to match the trajectory's creation snapshot.
	ResumeExact ResumePolicy = "exact"
	// ResumeCurrent restores messages but deliberately uses the caller's current
	// provider/system configuration and mounted composition.
	ResumeCurrent ResumePolicy = "current"
)

type Session struct {
	runtime  *Runtime
	config   SessionConfig
	mu       sync.Mutex
	messages []sdk.Message
	head     string
}

type Result struct {
	Output     string        `json:"output"`
	Messages   []sdk.Message `json:"messages"`
	Turns      int           `json:"turns"`
	ToolCalls  int           `json:"tool_calls"`
	Generation uint64        `json:"generation"`
	Cause      sdk.Cause     `json:"cause"`
}

func (runtime *Runtime) NewSession(
	ctx context.Context,
	config SessionConfig,
) (*Session, error) {
	if err := validateSessionConfig(runtime, &config); err != nil {
		return nil, err
	}
	lease, err := runtime.acquireSnapshot()
	if err != nil {
		return nil, err
	}
	environment, err := newTrajectoryEnvironment(runtime, lease.snapshot, config)
	lease.release()
	if err != nil {
		return nil, err
	}
	if err := runtime.trajectories.Create(ctx, sdk.Trajectory{
		ID:          config.ID,
		Environment: environment,
	}); err != nil {
		return nil, fmt.Errorf("create session trajectory %q: %w", config.ID, err)
	}
	return &Session{runtime: runtime, config: config}, nil
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
	trajectory, err := runtime.trajectories.Load(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load session trajectory %q: %w", id, err)
	}
	checkpointID, checkpoint, err := latestTrajectoryCheckpoint(trajectory)
	if err != nil {
		return nil, err
	}
	if config.ResumePolicy == ResumeExact {
		lease, acquireErr := runtime.acquireSnapshot()
		if acquireErr != nil {
			return nil, acquireErr
		}
		current, environmentErr := newTrajectoryEnvironment(
			runtime,
			lease.snapshot,
			config,
		)
		lease.release()
		if environmentErr != nil {
			return nil, environmentErr
		}
		if err := validateResumeEnvironment(trajectory.Environment, current); err != nil {
			return nil, err
		}
		if checkpoint != nil {
			config.System = checkpoint.System
			if checkpoint.Provider != "" {
				config.Provider = checkpoint.Provider
			} else if trajectory.Environment.RequestedProvider != "" {
				config.Provider = trajectory.Environment.RequestedProvider
			}
		}
	}
	head := trajectory.Head
	if trajectory.Head != checkpointID &&
		!trajectoryHeadRestoresCheckpoint(trajectory, checkpointID) {
		head, err = runtime.moveTrajectoryHead(
			ctx,
			trajectory,
			checkpointID,
			sdk.TrajectoryKindRestore,
		)
		if err != nil {
			return nil, fmt.Errorf("restore session trajectory %q: %w", id, err)
		}
	}
	session := &Session{
		runtime:  runtime,
		config:   config,
		messages: cloneMessages(checkpointMessages(checkpoint)),
		head:     head,
	}
	runtime.emitTrajectoryEvent(ctx, sdk.EventTrajectoryRestore, sdk.TrajectoryEventPayload{
		TrajectoryID: id,
		EntryID:      head,
		EntryKind:    sdk.TrajectoryKindRestore,
		From:         trajectory.Head,
		To:           checkpointID,
	})
	return session, nil
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

func (session *Session) Messages() []sdk.Message {
	session.mu.Lock()
	defer session.mu.Unlock()
	return cloneMessages(session.messages)
}
