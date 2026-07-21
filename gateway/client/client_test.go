package client

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/lincyaw/ag/gateway"
	gatewayv1 "github.com/lincyaw/ag/gatewayrpc/v1"
	"github.com/lincyaw/ag/sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientUsesGRPCAndMultiplexesViewCommandsWithEvents(t *testing.T) {
	remote := &fakeGatewayServer{}
	target := serveFakeGateway(t, remote)
	client, err := New(Config{Target: target, UserID: "user-a"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Health(t.Context()); err != nil {
		t.Fatal(err)
	}

	created, err := client.CreateSession(t.Context(), CreateSessionRequest{
		ID: "trajectory-a", Provider: "openai", MaxTurns: 8,
		WorkspaceRoot: "/workspace", RuntimeConfig: []byte(`{"tree":true}`),
	})
	if err != nil || created.ID != "trajectory-a" {
		t.Fatalf("created = %#v, %v", created, err)
	}
	if remote.created.GetUserId() != "user-a" ||
		remote.created.GetWorkspaceRoot() != "/workspace" ||
		string(remote.created.GetRuntimeConfigJson()) != `{"tree":true}` {
		t.Fatalf("create request = %#v", remote.created)
	}
	conversation, err := client.ListConversation(
		t.Context(), created.ID, "head-a",
		gateway.ConversationQuery{After: 2, Limit: 10},
	)
	if err != nil || conversation.Head != "head-a" ||
		remote.conversation.GetAfter() != 2 || remote.conversation.GetLimit() != 10 {
		t.Fatalf(
			"conversation = %#v request=%#v error=%v",
			conversation,
			remote.conversation,
			err,
		)
	}
	entries, err := client.ListTrajectoryEntries(
		t.Context(), created.ID, "head-a",
		gateway.TrajectoryEntryQuery{After: 3, Limit: 11},
	)
	if err != nil || entries.Trajectory.Head != "head-a" ||
		remote.entries.GetAfter() != 3 || remote.entries.GetLimit() != 11 {
		t.Fatalf(
			"entries = %#v request=%#v error=%v",
			entries,
			remote.entries,
			err,
		)
	}

	view, err := client.OpenView(t.Context(), created.ID, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = view.Close() })
	input, err := view.EnqueueInput(t.Context(), "input-a", "hello")
	if err != nil || input.ID != "input-a" {
		t.Fatalf("input = %#v, %v", input, err)
	}
	event, err := view.Next()
	if err != nil || event.Sequence != 5 || view.Cursor() != 5 {
		t.Fatalf("event = %#v cursor=%d, %v", event, view.Cursor(), err)
	}
}

func TestClientPreservesGRPCStatus(t *testing.T) {
	remote := &fakeGatewayServer{getError: status.Error(codes.Aborted, "busy")}
	client, err := New(Config{
		Target: serveFakeGateway(t, remote), UserID: "user-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	_, err = client.GetSession(t.Context(), "trajectory-a")
	if status.Code(err) != codes.Aborted {
		t.Fatalf("status = %s, error = %v", status.Code(err), err)
	}
}

func TestClientRejectsNonGRPCTarget(t *testing.T) {
	_, err := New(Config{Target: "http://127.0.0.1:8080", UserID: "user-a"})
	if err == nil {
		t.Fatal("HTTP gateway target accepted")
	}
}

func TestClientRejectsIncompatibleGatewayProtocol(t *testing.T) {
	remote := &fakeGatewayServer{protocolVersion: "gateway.future"}
	client, err := New(Config{
		Target: serveFakeGateway(t, remote), UserID: "user-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Health(t.Context()); err == nil {
		t.Fatal("incompatible gateway protocol accepted")
	}
}

type fakeGatewayServer struct {
	gatewayv1.UnimplementedGatewayServiceServer
	created         *gatewayv1.CreateTrajectoryRequest
	conversation    *gatewayv1.ListConversationRequest
	entries         *gatewayv1.ListTrajectoryEntriesRequest
	getError        error
	protocolVersion string
}

func (server *fakeGatewayServer) ListConversation(
	_ context.Context,
	request *gatewayv1.ListConversationRequest,
) (*gatewayv1.JsonValue, error) {
	server.conversation = request
	return testJSON(gateway.ConversationPage{
		Head: request.GetHead(),
		Items: []gateway.ConversationMessage{{
			Role: sdk.RoleUser, Content: "hello",
		}},
	})
}

func (server *fakeGatewayServer) ListTrajectoryEntries(
	_ context.Context,
	request *gatewayv1.ListTrajectoryEntriesRequest,
) (*gatewayv1.JsonValue, error) {
	server.entries = request
	return testJSON(gateway.TrajectoryEntryPage{
		Trajectory: gateway.TrajectoryInspection{
			ID: request.GetTrajectoryId(), Head: request.GetHead(),
		},
	})
}

func (server *fakeGatewayServer) Health(
	context.Context,
	*gatewayv1.HealthRequest,
) (*gatewayv1.HealthResponse, error) {
	protocolVersion := server.protocolVersion
	if protocolVersion == "" {
		protocolVersion = gatewayv1.ProtocolVersion
	}
	return &gatewayv1.HealthResponse{
		Status: "ok", ProtocolVersion: protocolVersion,
	}, nil
}

func (server *fakeGatewayServer) CreateTrajectory(
	_ context.Context,
	request *gatewayv1.CreateTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	server.created = request
	return testJSON(gateway.Session{
		ID: request.GetId(), UserID: request.GetUserId(),
		Provider: request.GetProvider(), MaxTurns: int(request.GetMaxTurns()),
		WorkspaceRoot: request.GetWorkspaceRoot(),
	})
}

func (server *fakeGatewayServer) GetTrajectory(
	context.Context,
	*gatewayv1.GetTrajectoryRequest,
) (*gatewayv1.JsonValue, error) {
	if server.getError != nil {
		return nil, server.getError
	}
	return testJSON(gateway.Session{ID: "trajectory-a", UserID: "user-a"})
}

func (*fakeGatewayServer) Connect(
	stream grpc.BidiStreamingServer[gatewayv1.ViewRequest, gatewayv1.ViewResponse],
) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	open := request.GetOpen()
	trajectory, _ := testJSON(gateway.Session{
		ID: open.GetTrajectoryId(), UserID: open.GetUserId(),
	})
	if err := stream.Send(&gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Ready{Ready: &gatewayv1.ViewReady{
			Trajectory: trajectory, Cursor: open.GetAfterEvent(),
		}},
	}); err != nil {
		return err
	}
	request, err = stream.Recv()
	if err != nil {
		return err
	}
	command := request.GetCommand()
	enqueue := command.GetEnqueueInput()
	event, _ := testJSON(gateway.AgentEvent{
		Sequence: 5, SessionID: open.GetTrajectoryId(),
		ID: "event-5", Name: "turn_start", CreatedAt: time.Now().UTC(),
		Payload: json.RawMessage(`{"turn":1}`),
	})
	if err := stream.Send(&gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Event{Event: &gatewayv1.ViewEvent{Event: event}},
	}); err != nil {
		return err
	}
	input, _ := testJSON(gateway.AgentInput{
		ID: enqueue.GetInputId(), SessionID: open.GetTrajectoryId(),
		Content: enqueue.GetContent(), State: gateway.AgentInputQueued,
	})
	if err := stream.Send(&gatewayv1.ViewResponse{
		Frame: &gatewayv1.ViewResponse_Result{Result: &gatewayv1.ViewResult{
			RequestId: command.GetRequestId(), Value: input,
		}},
	}); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func serveFakeGateway(t *testing.T, service gatewayv1.GatewayServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	gatewayv1.RegisterGatewayServiceServer(server, service)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, grpc.ErrServerStopped) {
				t.Errorf("serve fake gateway: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("fake gateway did not stop")
		}
	})
	return "grpc://" + listener.Addr().String()
}

func testJSON(value any) (*gatewayv1.JsonValue, error) {
	raw, err := json.Marshal(value)
	return &gatewayv1.JsonValue{Json: raw}, err
}
