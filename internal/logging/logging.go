package logging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

func WithHandler(logger *slog.Logger, additional slog.Handler) *slog.Logger {
	if additional == nil {
		return logger
	}
	if logger == nil {
		return slog.New(additional)
	}
	return slog.New(multiHandler{handlers: []slog.Handler{logger.Handler(), additional}})
}

type multiHandler struct{ handlers []slog.Handler }

func (handler multiHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, candidate := range handler.handlers {
		if candidate.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (handler multiHandler) Handle(ctx context.Context, record slog.Record) error {
	var errs []error
	for _, candidate := range handler.handlers {
		if candidate.Enabled(ctx, record.Level) {
			errs = append(errs, candidate.Handle(ctx, record.Clone()))
		}
	}
	return errors.Join(errs...)
}

func (handler multiHandler) WithAttrs(attributes []slog.Attr) slog.Handler {
	result := make([]slog.Handler, len(handler.handlers))
	for index, candidate := range handler.handlers {
		result[index] = candidate.WithAttrs(attributes)
	}
	return multiHandler{handlers: result}
}

func (handler multiHandler) WithGroup(name string) slog.Handler {
	result := make([]slog.Handler, len(handler.handlers))
	for index, candidate := range handler.handlers {
		result[index] = candidate.WithGroup(name)
	}
	return multiHandler{handlers: result}
}

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

func OpenFile(
	config Config,
	path string,
	console io.Writer,
) (*slog.Logger, io.Closer, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, nil, errors.New("log file path is empty")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, nil, fmt.Errorf("create log directory: %w", err)
	}
	file, err := os.OpenFile(
		path,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY,
		0o600,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("open log file: %w", err)
	}
	config.Writer = file
	if console != nil {
		config.Writer = io.MultiWriter(file, console)
	}
	logger, err := New(config)
	if err != nil {
		return nil, nil, errors.Join(err, file.Close())
	}
	return logger, file, nil
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
