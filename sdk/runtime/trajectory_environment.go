package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

var ErrResumeEnvironmentMismatch = errors.New(
	"current runtime composition is incompatible with trajectory",
)

func newTrajectoryEnvironment(
	runtime *Runtime,
	snapshot *registrySnapshot,
	config SessionConfig,
) (TrajectoryEnvironment, error) {
	catalog := catalogFromSnapshot(snapshot)
	environment := TrajectoryEnvironment{
		SDKAPIVersion:     APIVersion,
		RuntimeVersion:    runtime.version,
		CreatedGeneration: catalog.Generation,
		RequestedProvider: config.Provider,
		SystemDigest:      digestString(config.System),
		Providers:         append([]ProviderSpec(nil), catalog.Providers...),
		Tools:             append([]ToolSpec(nil), catalog.Tools...),
		Hooks:             append([]HookSpec(nil), catalog.Hooks...),
		Subscribers:       append([]SubscriberSpec(nil), catalog.Subscribers...),
		Capabilities:      append([]CapabilitySpec(nil), catalog.Capabilities...),
		Events:            append([]EventContract(nil), catalog.Events...),
	}
	for _, plugin := range catalog.Plugins {
		environment.Plugins = append(environment.Plugins, TrajectoryPlugin{
			Name:      plugin.Name,
			Version:   plugin.Version,
			Registers: append([]string(nil), plugin.Registers...),
		})
	}
	raw, err := json.Marshal(struct {
		SDKAPIVersion int                `json:"sdk_api_version"`
		Plugins       []TrajectoryPlugin `json:"plugins"`
		Providers     []ProviderSpec     `json:"providers"`
		Tools         []ToolSpec         `json:"tools"`
		Hooks         []HookSpec         `json:"hooks"`
		Subscribers   []SubscriberSpec   `json:"subscribers"`
		Capabilities  []CapabilitySpec   `json:"capabilities"`
		Events        []EventContract    `json:"events"`
	}{
		SDKAPIVersion: environment.SDKAPIVersion,
		Plugins:       environment.Plugins,
		Providers:     environment.Providers,
		Tools:         environment.Tools,
		Hooks:         environment.Hooks,
		Subscribers:   environment.Subscribers,
		Capabilities:  environment.Capabilities,
		Events:        environment.Events,
	})
	if err != nil {
		return TrajectoryEnvironment{}, fmt.Errorf(
			"encode trajectory environment: %w",
			err,
		)
	}
	environment.CompositionDigest = digestBytes(raw)
	return environment, nil
}

func validateResumeEnvironment(
	recorded TrajectoryEnvironment,
	current TrajectoryEnvironment,
) error {
	// Schema-zero trajectories predate environment snapshots. They remain
	// resumable, but cannot receive exact-composition guarantees retroactively.
	if recorded.SDKAPIVersion == 0 && recorded.CompositionDigest == "" {
		return nil
	}
	if recorded.SDKAPIVersion != current.SDKAPIVersion {
		return fmt.Errorf(
			"%w: SDK API version changed from %d to %d",
			ErrResumeEnvironmentMismatch,
			recorded.SDKAPIVersion,
			current.SDKAPIVersion,
		)
	}
	if recorded.CompositionDigest != current.CompositionDigest {
		return fmt.Errorf(
			"%w: composition digest changed from %s to %s",
			ErrResumeEnvironmentMismatch,
			recorded.CompositionDigest,
			current.CompositionDigest,
		)
	}
	return nil
}

func digestString(value string) string {
	return digestBytes([]byte(value))
}

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
