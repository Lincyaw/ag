package sdk

import (
	"context"
	"errors"
	"sync"
)

type localSource struct {
	plugin Plugin
	open   sync.Once
}

func Local(plugin Plugin) Source {
	return &localSource{plugin: plugin}
}

func (source *localSource) String() string {
	if source.plugin == nil {
		return "local://<nil>"
	}
	return "local://" + source.plugin.Manifest().Name
}

func (source *localSource) Open(ctx context.Context) (Connection, error) {
	if source.plugin == nil {
		return nil, errors.New("local plugin is nil")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	opened := false
	source.open.Do(func() { opened = true })
	if !opened {
		return nil, errors.New("local plugin source is already open")
	}
	return &localConnection{Plugin: source.plugin}, nil
}

type localConnection struct {
	Plugin
	close    sync.Once
	closeErr error
}

func (connection *localConnection) Close(ctx context.Context) error {
	connection.close.Do(func() {
		if closer, ok := connection.Plugin.(interface {
			Close(context.Context) error
		}); ok {
			connection.closeErr = closer.Close(ctx)
		}
	})
	return connection.closeErr
}
