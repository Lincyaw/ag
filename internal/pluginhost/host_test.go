package pluginhost

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/lincyaw/ag/sdk"
)

type closingHostPlugin struct {
	closes atomic.Int64
}

func (*closingHostPlugin) Manifest() sdk.Manifest {
	return sdk.Manifest{
		Name:        "closing-host-plugin",
		Version:     "1.0.0",
		Description: "verifies standalone host cleanup",
		APIVersion:  sdk.APIVersion,
		Registers: []string{
			sdk.SubscriberResource("host-cleanup-events"),
		},
	}
}

func (*closingHostPlugin) Install(
	_ context.Context,
	registrar sdk.Registrar,
) error {
	return registrar.RegisterSubscriber(sdk.SubscriberFunc{
		SubscriberSpec: sdk.SubscriberSpec{
			Name:   "host-cleanup-events",
			Events: []string{sdk.EventAgentEnd},
		},
		ReceiveFunc: func(context.Context, sdk.Delivery) error {
			return nil
		},
	})
}

func (plugin *closingHostPlugin) Close(context.Context) error {
	plugin.closes.Add(1)
	return nil
}

type failingReadyWriter struct {
	err error
}

func (writer failingReadyWriter) Write([]byte) (int, error) {
	return 0, writer.err
}

func TestServeCleansUpAfterReadyWriteFailure(t *testing.T) {
	t.Parallel()
	readyErr := errors.New("ready output failed")
	plugin := &closingHostPlugin{}
	err := Serve(t.Context(), Config{
		Plugin:      plugin,
		Listen:      "127.0.0.1:0",
		StorageURI:  "memory://local?namespace=host-cleanup",
		ReadyWriter: failingReadyWriter{err: readyErr},
	})
	if !errors.Is(err, readyErr) {
		t.Fatalf("serve error = %v", err)
	}
	if got := plugin.closes.Load(); got != 1 {
		t.Fatalf("plugin close calls = %d, want 1", got)
	}
}
