package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	gatewayclient "github.com/lincyaw/ag/gateway/client"
	gatewaymanager "github.com/lincyaw/ag/gateway/manager"
	appconfig "github.com/lincyaw/ag/internal/config"
)

func normalizeAgentViewConfig(config appconfig.Config) (appconfig.Config, error) {
	workspaceRoot, err := filepath.Abs(config.Workspace.Root)
	if err != nil {
		return appconfig.Config{}, fmt.Errorf("resolve agent workspace root: %w", err)
	}
	config.Workspace.Root = filepath.Clean(workspaceRoot)
	gatewayDirectory, err := filepath.Abs(config.Gateway.Directory)
	if err != nil {
		return appconfig.Config{}, fmt.Errorf("resolve gateway directory: %w", err)
	}
	config.Gateway.Directory = filepath.Clean(gatewayDirectory)
	return config, nil
}

func (application *app) ensureManagedGateway(
	ctx context.Context,
	config appconfig.Config,
) (string, error) {
	if target := strings.TrimSpace(config.Gateway.Target); target != "" {
		client, err := gatewayclient.New(gatewayclient.Config{
			Target: target, UserID: localGatewayUserID,
		})
		if err != nil {
			return "", err
		}
		defer client.Close()
		if err := client.Health(ctx); err != nil {
			return "", err
		}
		return target, nil
	}
	raw, err := json.Marshal(config)
	if err != nil {
		return "", fmt.Errorf("encode managed gateway config: %w", err)
	}
	manager, err := gatewaymanager.New(gatewaymanager.Config{
		Directory: config.Gateway.Directory, RuntimeConfig: raw,
		Launcher: application.launchGateway,
		Probe:    application.probeGateway,
	})
	if err != nil {
		return "", err
	}
	ready, err := manager.Ensure(ctx)
	if err != nil {
		return "", err
	}
	return ready.Target, nil
}
