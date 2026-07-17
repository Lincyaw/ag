package sdk

import (
	"context"
	"errors"
	"fmt"
)

type localSource struct {
	plugin Plugin
}

func Local(plugin Plugin) Source {
	return localSource{plugin: plugin}
}

func (source localSource) String() string {
	if source.plugin == nil {
		return "local://<nil>"
	}
	return "local://" + source.plugin.Manifest().Name
}

func (source localSource) Open(context.Context) (Connection, error) {
	if source.plugin == nil {
		return nil, errors.New("local plugin is nil")
	}
	return localConnection{Plugin: source.plugin}, nil
}

type localConnection struct {
	Plugin
}

func (connection localConnection) Close(ctx context.Context) error {
	if closer, ok := connection.Plugin.(interface {
		Close(context.Context) error
	}); ok {
		return closer.Close(ctx)
	}
	return nil
}

func sourceDescription(source Source) string {
	if source == nil {
		return "<nil>"
	}
	value := source.String()
	if value == "" {
		return fmt.Sprintf("%T", source)
	}
	return value
}

func SourceDescription(source Source) string {
	return sourceDescription(source)
}
