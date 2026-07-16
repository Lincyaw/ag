package logging

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type Config struct {
	Level  string
	Format string
	Writer io.Writer
}

func New(config Config) (*slog.Logger, error) {
	if config.Writer == nil {
		config.Writer = os.Stderr
	}
	if config.Format == "" {
		config.Format = "json"
	}

	level, err := parseLevel(config.Level)
	if err != nil {
		return nil, err
	}
	options := &slog.HandlerOptions{Level: level}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(config.Format)) {
	case "json":
		handler = slog.NewJSONHandler(config.Writer, options)
	case "text":
		handler = slog.NewTextHandler(config.Writer, options)
	default:
		return nil, errors.New(`log format must be "json" or "text"`)
	}
	return slog.New(traceHandler{Handler: handler}), nil
}

func parseLevel(raw string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, errors.New(`log level must be "debug", "info", "warn", or "error"`)
	}
}

type traceHandler struct {
	slog.Handler
}

func (h traceHandler) Handle(ctx context.Context, record slog.Record) error {
	spanContext := trace.SpanContextFromContext(ctx)
	if spanContext.IsValid() {
		record.AddAttrs(
			slog.String("trace_id", spanContext.TraceID().String()),
			slog.String("span_id", spanContext.SpanID().String()),
		)
	}
	return h.Handler.Handle(ctx, record)
}

func (h traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return traceHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h traceHandler) WithGroup(name string) slog.Handler {
	return traceHandler{Handler: h.Handler.WithGroup(name)}
}
