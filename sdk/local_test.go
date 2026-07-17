package sdk

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

type localLifecyclePlugin struct {
	PluginFunc
	closes   atomic.Int64
	closeErr error
}

func (plugin *localLifecyclePlugin) Close(context.Context) error {
	plugin.closes.Add(1)
	return plugin.closeErr
}

func TestLocalSourceOpensOnceAndConnectionClosesOnce(t *testing.T) {
	closeErr := errors.New("close failed")
	plugin := &localLifecyclePlugin{closeErr: closeErr}
	source := Local(plugin)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := source.Open(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(canceled) error = %v, want context.Canceled", err)
	}
	connection, err := source.Open(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := source.Open(context.Background()); err == nil ||
		!strings.Contains(err.Error(), "already open") {
		t.Fatalf("second Open() error = %v, want already open", err)
	}

	for range 2 {
		if err := connection.Close(context.Background()); !errors.Is(err, closeErr) {
			t.Fatalf("Close() error = %v, want %v", err, closeErr)
		}
	}
	if got := plugin.closes.Load(); got != 1 {
		t.Fatalf("plugin close calls = %d, want 1", got)
	}
}
