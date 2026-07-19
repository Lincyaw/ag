package sdk

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

type trajectoryEnvironmentComposition struct {
	SDKAPIVersion int                `json:"sdk_api_version"`
	Plugins       []TrajectoryPlugin `json:"plugins"`
	Providers     []ProviderSpec     `json:"providers"`
	Tools         []ToolSpec         `json:"tools"`
	Agents        []AgentSpec        `json:"agents"`
	Hooks         []HookSpec         `json:"hooks"`
	Subscribers   []SubscriberSpec   `json:"subscribers"`
	Capabilities  []CapabilitySpec   `json:"capabilities"`
	Events        []EventContract    `json:"events"`
}

// TrajectorySystemDigest returns the stable digest stored on trajectory
// environments for the system prompt that created or resumed a session.
func TrajectorySystemDigest(system string) string {
	return digestTrajectoryBytes([]byte(system))
}

// TrajectoryEnvironmentHasCompositionSnapshot reports whether an environment
// can identify a concrete mounted composition instead of only legacy metadata.
func TrajectoryEnvironmentHasCompositionSnapshot(
	environment TrajectoryEnvironment,
) bool {
	return len(environment.Plugins) != 0 ||
		len(environment.Providers) != 0 ||
		len(environment.Tools) != 0 ||
		len(environment.Agents) != 0 ||
		len(environment.Hooks) != 0 ||
		len(environment.Subscribers) != 0 ||
		len(environment.Capabilities) != 0 ||
		len(environment.Events) != 0
}

// FinalizeTrajectoryEnvironment returns an owned environment snapshot with its
// canonical composition digest populated.
func FinalizeTrajectoryEnvironment(
	environment TrajectoryEnvironment,
) (TrajectoryEnvironment, error) {
	environment = CloneTrajectoryEnvironment(environment)
	digest, err := TrajectoryEnvironmentCompositionDigest(environment)
	if err != nil {
		return TrajectoryEnvironment{}, err
	}
	environment.CompositionDigest = digest
	return environment, nil
}

// TrajectoryEnvironmentCompositionDigest computes the stable identity of the
// mounted plugins and resources that exact resume must reproduce.
func TrajectoryEnvironmentCompositionDigest(
	environment TrajectoryEnvironment,
) (string, error) {
	environment = CloneTrajectoryEnvironment(environment)
	raw, err := json.Marshal(trajectoryEnvironmentComposition{
		SDKAPIVersion: environment.SDKAPIVersion,
		Plugins:       environment.Plugins,
		Providers:     environment.Providers,
		Tools:         environment.Tools,
		Agents:        environment.Agents,
		Hooks:         environment.Hooks,
		Subscribers:   environment.Subscribers,
		Capabilities:  environment.Capabilities,
		Events:        environment.Events,
	})
	if err != nil {
		return "", fmt.Errorf("encode trajectory environment composition: %w", err)
	}
	return digestTrajectoryBytes(raw), nil
}

func digestTrajectoryBytes(value []byte) string {
	sum := sha256.Sum256(value)
	return hex.EncodeToString(sum[:])
}
