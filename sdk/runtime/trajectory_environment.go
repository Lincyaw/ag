package runtime

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/lincyaw/ag/sdk"
)

var ErrResumeEnvironmentMismatch = errors.New(
	"current runtime composition is incompatible with trajectory",
)

func newTrajectoryEnvironment(
	runtime *Runtime,
	snapshot *registrySnapshot,
	config SessionConfig,
) (sdk.TrajectoryEnvironment, error) {
	catalog := catalogFromSnapshot(snapshot)
	environment := sdk.TrajectoryEnvironment{
		SDKAPIVersion:     sdk.APIVersion,
		RuntimeVersion:    runtime.version,
		CreatedGeneration: catalog.Generation,
		RequestedProvider: config.Provider,
		SystemDigest:      digestBytes([]byte(config.System)),
		Providers:         append([]sdk.ProviderSpec(nil), catalog.Providers...),
		Tools:             append([]sdk.ToolSpec(nil), catalog.Tools...),
		Hooks:             append([]sdk.HookSpec(nil), catalog.Hooks...),
		Subscribers:       append([]sdk.SubscriberSpec(nil), catalog.Subscribers...),
		Capabilities:      append([]sdk.CapabilitySpec(nil), catalog.Capabilities...),
		Events:            append([]sdk.EventContract(nil), catalog.Events...),
	}
	for _, plugin := range catalog.Plugins {
		environment.Plugins = append(environment.Plugins, sdk.TrajectoryPlugin{
			Name:      plugin.Name,
			Version:   plugin.Version,
			Registers: append([]string(nil), plugin.Registers...),
		})
	}
	raw, err := json.Marshal(struct {
		SDKAPIVersion int                    `json:"sdk_api_version"`
		Plugins       []sdk.TrajectoryPlugin `json:"plugins"`
		Providers     []sdk.ProviderSpec     `json:"providers"`
		Tools         []sdk.ToolSpec         `json:"tools"`
		Hooks         []sdk.HookSpec         `json:"hooks"`
		Subscribers   []sdk.SubscriberSpec   `json:"subscribers"`
		Capabilities  []sdk.CapabilitySpec   `json:"capabilities"`
		Events        []sdk.EventContract    `json:"events"`
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
		return sdk.TrajectoryEnvironment{}, fmt.Errorf(
			"encode trajectory environment: %w",
			err,
		)
	}
	environment.CompositionDigest = digestBytes(raw)
	return environment, nil
}

func validateResumeEnvironment(
	recorded sdk.TrajectoryEnvironment,
	current sdk.TrajectoryEnvironment,
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

func digestBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
