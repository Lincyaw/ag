package pluginrpc

import (
	"errors"

	pluginv1 "github.com/lincyaw/ag/pluginrpc/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1 "google.golang.org/grpc/health/grpc_health_v1"
)

func NewGRPCServer(
	plugin *Server,
	maxMessageBytes int,
	options ...grpc.ServerOption,
) (*grpc.Server, error) {
	if plugin == nil {
		return nil, errors.New("plugin RPC server is nil")
	}
	if maxMessageBytes == 0 {
		maxMessageBytes = defaultMaxMessageBytes
	}
	if maxMessageBytes < 1 {
		return nil, errors.New("RPC max message bytes must be positive")
	}
	base := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
	}
	server := grpc.NewServer(append(base, options...)...)
	pluginv1.RegisterPluginServiceServer(server, plugin)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthv1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus(pluginv1.PluginService_ServiceDesc.ServiceName, healthv1.HealthCheckResponse_SERVING)
	healthv1.RegisterHealthServer(server, healthServer)
	return server, nil
}
