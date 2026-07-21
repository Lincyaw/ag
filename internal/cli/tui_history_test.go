package cli

import (
	"context"
	"testing"

	"github.com/lincyaw/ag/gateway"
	"github.com/lincyaw/ag/sdk"
)

func TestLoadGatewayConversationAtHistoricalHead(t *testing.T) {
	client := &historicalConversationClient{}
	messages, execution, err := loadGatewayConversationAtHead(
		t.Context(), client, "trajectory-a", "checkpoint-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	if client.head != "checkpoint-a" {
		t.Fatalf("requested head = %q", client.head)
	}
	if len(messages) != 2 || messages[0].Content != "old prompt" ||
		messages[1].Content != "old answer" {
		t.Fatalf("historical messages = %#v", messages)
	}
	if execution == nil || execution.ID != "execution-old" {
		t.Fatalf("historical execution = %#v", execution)
	}
}

type historicalConversationClient struct {
	head string
}

func (client *historicalConversationClient) ListConversation(
	_ context.Context,
	_ string,
	head string,
	_ gateway.ConversationQuery,
) (gateway.ConversationPage, error) {
	client.head = head
	return gateway.ConversationPage{
		Head: head,
		Execution: &sdk.TrajectoryExecution{
			ID: "execution-old", State: sdk.TrajectoryExecutionSucceeded,
		},
		Items: []gateway.ConversationMessage{
			{Role: sdk.RoleUser, Content: "old prompt"},
			{Role: sdk.RoleAssistant, Content: "old answer"},
		},
	}, nil
}
