package pluginrpc

import (
	"errors"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"github.com/lincyaw/ag/registry"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func NewGRPCServer(
	plugin Server,
	maxMessageBytes int,
	options ...grpc.ServerOption,
) (*grpc.Server, error) {
	if plugin == nil {
		return nil, errors.New("plugin RPC server is nil")
	}
	server, healthServer, err := newGRPCServer(maxMessageBytes, options...)
	if err != nil {
		return nil, err
	}
	pluginv1.RegisterPluginServiceServer(server, plugin)
	healthServer.SetServingStatus(
		pluginv1.PluginService_ServiceDesc.ServiceName,
		healthv1.HealthCheckResponse_SERVING,
	)
	return server, nil
}

func NewRegistryGRPCServer(
	directory registry.Directory,
	maxMessageBytes int,
	options ...grpc.ServerOption,
) (*grpc.Server, error) {
	adapter, err := NewRegistryServer(directory)
	if err != nil {
		return nil, err
	}
	server, healthServer, err := newGRPCServer(maxMessageBytes, options...)
	if err != nil {
		return nil, err
	}
	pluginv1.RegisterRegistryServiceServer(server, adapter)
	healthServer.SetServingStatus(
		pluginv1.RegistryService_ServiceDesc.ServiceName,
		healthv1.HealthCheckResponse_SERVING,
	)
	return server, nil
}

func newGRPCServer(
	maxMessageBytes int,
	options ...grpc.ServerOption,
) (*grpc.Server, *health.Server, error) {
	if maxMessageBytes == 0 {
		maxMessageBytes = defaultMaxMessageBytes
	}
	if maxMessageBytes < 1 {
		return nil, nil, errors.New("RPC max message bytes must be positive")
	}
	base := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
	}
	server := grpc.NewServer(append(base, options...)...)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(server, healthServer)
	return server, healthServer, nil
}
