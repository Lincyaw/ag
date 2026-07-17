package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.39.0"
	"go.opentelemetry.io/otel/trace"
)

const instrumentationName = "github.com/lincyaw/ag"

type Config struct {
	ServiceName    string
	ServiceVersion string
	Logger         *slog.Logger
}

type Runtime struct {
	Tracer      trace.Tracer
	Meter       metric.Meter
	LogHandler  slog.Handler
	shutdowns   []func(context.Context) error
	shutdown    sync.Once
	shutdownErr error
}

func Setup(ctx context.Context, config Config) (*Runtime, error) {
	if strings.TrimSpace(config.ServiceName) == "" {
		return nil, errors.New("telemetry service name is empty")
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	res, err := resource.New(
		ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceNameKey.String(config.ServiceName),
			semconv.ServiceVersionKey.String(config.ServiceVersion),
		),
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, fmt.Errorf("create OTel resource: %w", err)
	}

	traceEnabled, err := exporterEnabled(
		os.Getenv("OTEL_TRACES_EXPORTER"),
		hasEndpoint("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"),
	)
	if err != nil {
		return nil, fmt.Errorf("configure trace exporter: %w", err)
	}
	metricEnabled, err := exporterEnabled(
		os.Getenv("OTEL_METRICS_EXPORTER"),
		hasEndpoint("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"),
	)
	if err != nil {
		return nil, fmt.Errorf("configure metric exporter: %w", err)
	}
	logEnabled, err := exporterEnabled(
		os.Getenv("OTEL_LOGS_EXPORTER"),
		hasEndpoint("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"),
	)
	if err != nil {
		return nil, fmt.Errorf("configure log exporter: %w", err)
	}
	if traceEnabled {
		if err := validateHTTPProtocol(
			os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL"),
			os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		); err != nil {
			return nil, fmt.Errorf("configure trace protocol: %w", err)
		}
	}
	if metricEnabled {
		if err := validateHTTPProtocol(
			os.Getenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL"),
			os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		); err != nil {
			return nil, fmt.Errorf("configure metric protocol: %w", err)
		}
	}
	if logEnabled {
		if err := validateHTTPProtocol(
			os.Getenv("OTEL_EXPORTER_OTLP_LOGS_PROTOCOL"),
			os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"),
		); err != nil {
			return nil, fmt.Errorf("configure log protocol: %w", err)
		}
	}

	runtime := &Runtime{}
	cleanupOnError := func(cause error) (*Runtime, error) {
		return nil, errors.Join(cause, runtime.Shutdown(context.Background()))
	}
	var installGlobals []func()

	if traceEnabled {
		exporter, exportErr := otlptracehttp.New(ctx)
		if exportErr != nil {
			return cleanupOnError(fmt.Errorf("create OTLP trace exporter: %w", exportErr))
		}
		provider := sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		runtime.shutdowns = append(runtime.shutdowns, provider.Shutdown)
		installGlobals = append(installGlobals, func() {
			otel.SetTracerProvider(provider)
		})
	}

	if metricEnabled {
		exporter, exportErr := otlpmetrichttp.New(ctx)
		if exportErr != nil {
			return cleanupOnError(fmt.Errorf("create OTLP metric exporter: %w", exportErr))
		}
		reader := sdkmetric.NewPeriodicReader(exporter)
		provider := sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(reader),
			sdkmetric.WithResource(res),
		)
		runtime.shutdowns = append(runtime.shutdowns, provider.Shutdown)
		installGlobals = append(installGlobals, func() {
			otel.SetMeterProvider(provider)
		})
	}
	if logEnabled {
		exporter, exportErr := otlploghttp.New(ctx)
		if exportErr != nil {
			return cleanupOnError(fmt.Errorf("create OTLP log exporter: %w", exportErr))
		}
		provider := sdklog.NewLoggerProvider(
			sdklog.WithProcessor(sdklog.NewBatchProcessor(exporter)),
			sdklog.WithResource(res),
		)
		runtime.LogHandler = otelslog.NewHandler(
			instrumentationName,
			otelslog.WithLoggerProvider(provider),
		)
		runtime.shutdowns = append(runtime.shutdowns, provider.Shutdown)
	}

	for _, install := range installGlobals {
		install()
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		config.Logger.Error("OpenTelemetry error", "error", err)
	}))

	runtime.Tracer = otel.Tracer(instrumentationName)
	runtime.Meter = otel.Meter(instrumentationName)
	config.Logger.Debug(
		"OpenTelemetry initialized",
		"traces_exported",
		traceEnabled,
		"metrics_exported",
		metricEnabled,
		"logs_exported",
		logEnabled,
	)
	return runtime, nil
}

func (r *Runtime) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.shutdown.Do(func() {
		var errs []error
		for index := len(r.shutdowns) - 1; index >= 0; index-- {
			if err := r.shutdowns[index](ctx); err != nil {
				errs = append(errs, err)
			}
		}
		r.shutdownErr = errors.Join(errs...)
	})
	return r.shutdownErr
}

func hasEndpoint(signalEndpoint string) bool {
	return strings.TrimSpace(os.Getenv(signalEndpoint)) != "" ||
		strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")) != ""
}

func exporterEnabled(selector string, endpointConfigured bool) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(selector)) {
	case "":
		return endpointConfigured, nil
	case "none":
		return false, nil
	case "otlp":
		return true, nil
	default:
		return false, fmt.Errorf(
			"unsupported exporter %q; use otlp or none",
			selector,
		)
	}
}

func validateHTTPProtocol(signalSpecific, common string) error {
	protocol := strings.TrimSpace(signalSpecific)
	if protocol == "" {
		protocol = strings.TrimSpace(common)
	}
	switch strings.ToLower(protocol) {
	case "", "http/protobuf":
		return nil
	default:
		return fmt.Errorf(
			"unsupported OTLP protocol %q; this binary includes http/protobuf",
			protocol,
		)
	}
}
