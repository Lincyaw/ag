package runtime

// Durability tests cover resume-environment compatibility.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lincyaw/ag/sdk"
	sdkstorage "github.com/lincyaw/ag/sdk/storage"
)

type executionEnvironmentProvider struct{}

func (executionEnvironmentProvider) Spec() sdk.ProviderSpec {
	return sdk.ProviderSpec{Name: "environment", Model: "test"}
}

func (executionEnvironmentProvider) Complete(
	context.Context,
	sdk.ModelRequest,
) (sdk.ModelResponse, error) {
	return sdk.ModelResponse{
		Content: "recovered",
		Model:   "test",
	}, nil
}

func TestRecoverExecutionUsesItsOwnCompositionSnapshot(t *testing.T) {
	backend := sdkstorage.NewMemoryStateBackend()
	t.Cleanup(func() {
		if err := backend.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	initial := newExecutionEnvironmentRuntime(t, backend, false)
	session, err := initial.NewSession(t.Context(), SessionConfig{
		ID: "execution-environment", Provider: "environment", MaxTurns: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Prompt(t.Context(), "first composition"); err != nil {
		t.Fatal(err)
	}
	closeExecutionEnvironmentRuntime(t, initial)

	changed := newExecutionEnvironmentRuntime(t, backend, true)
	session, err = changed.ResumeSession(
		t.Context(),
		"execution-environment",
		SessionConfig{
			Provider: "environment", MaxTurns: 2,
			ResumePolicy: ResumeCurrent,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	submission, err := session.SubmitPrompt(
		t.Context(),
		"recover under changed composition",
	)
	if err != nil {
		t.Fatal(err)
	}
	executionID := submission.Execution().ID
	closeExecutionEnvironmentRuntime(t, changed)

	recovery := newExecutionEnvironmentRuntime(t, backend, true)
	result, err := recovery.RecoverExecution(
		t.Context(),
		"execution-environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Output != "recovered" {
		t.Fatalf("recovery result = %#v", result)
	}
	metadata, err := backend.Trajectories().LoadMetadata(
		t.Context(),
		"execution-environment",
	)
	if err != nil {
		t.Fatal(err)
	}
	if metadata.Execution == nil ||
		metadata.Execution.ID != executionID ||
		metadata.Execution.State != sdk.TrajectoryExecutionSucceeded {
		t.Fatalf("recovered execution = %#v", metadata.Execution)
	}
	closeExecutionEnvironmentRuntime(t, recovery)
}

func newExecutionEnvironmentRuntime(
	t *testing.T,
	backend sdk.StateBackend,
	withMarker bool,
) *Runtime {
	t.Helper()
	runtime, err := NewRuntime(RuntimeConfig{
		Storage:          backend,
		StorageOwnership: StorageBorrowed,
		OperationPoll:    time.Millisecond,
		TrajectoryLease:  time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	provider := sdk.PluginFunc{
		PluginManifest: sdk.Manifest{
			Name:        "environment-provider",
			Version:     "1.0.0",
			Description: "execution environment test provider",
			APIVersion:  sdk.APIVersion,
			Registers: []string{
				sdk.ProviderResource("environment"),
			},
		},
		InstallFunc: func(
			_ context.Context,
			registrar sdk.Registrar,
		) error {
			return registrar.RegisterProvider(executionEnvironmentProvider{})
		},
	}
	if _, err := runtime.Mount(t.Context(), sdk.Local(provider)); err != nil {
		t.Fatal(err)
	}
	if withMarker {
		marker := sdk.PluginFunc{
			PluginManifest: sdk.Manifest{
				Name:        "composition-marker",
				Version:     "1.0.0",
				Description: "changes the runtime composition digest",
				APIVersion:  sdk.APIVersion,
			},
			InstallFunc: func(context.Context, sdk.Registrar) error {
				return nil
			},
		}
		if _, err := runtime.Mount(
			t.Context(),
			sdk.Local(marker),
		); err != nil {
			t.Fatal(err)
		}
	}
	return runtime
}

func closeExecutionEnvironmentRuntime(t *testing.T, runtime *Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil &&
		!errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
