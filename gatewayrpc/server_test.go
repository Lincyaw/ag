package gatewayrpc

import (
	"testing"

	"github.com/lincyaw/ag/gateway"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsGatewayDrainToUnavailable(t *testing.T) {
	if code := status.Code(rpcError(gateway.ErrGatewayDraining)); code != codes.Unavailable {
		t.Fatalf("draining RPC code = %s, want %s", code, codes.Unavailable)
	}
}
