package cli

import (
	"context"
	"fmt"
	"io"
	"slices"
	"time"

	"github.com/lincyaw/ag/gateway"
	gatewayclient "github.com/lincyaw/ag/gateway/client"
	"github.com/lincyaw/ag/sdk"
)

const (
	agentStatusIdle    = "idle"
	agentStatusQueued  = "queued"
	agentStatusRunning = "running"
	agentStatusWaiting = "waiting"
	agentStatusPaused  = "paused"
)

type managedTrajectorySummary struct {
	ID                 string    `json:"id"`
	Status             string    `json:"status"`
	WorkspaceRoot      string    `json:"workspace_root,omitempty"`
	Provider           string    `json:"provider,omitempty"`
	UpdatedAt          time.Time `json:"updated_at"`
	PendingInputs      int       `json:"pending_inputs"`
	PendingInteraction string    `json:"pending_interaction,omitempty"`
}

type trajectoryControlOutput struct {
	TrajectoryID string `json:"trajectory_id"`
	Action       string `json:"action"`
	Status       string `json:"status"`
	Affected     int    `json:"affected,omitempty"`
	InputID      string `json:"input_id,omitempty"`
}

func (application *app) listManagedTrajectories(
	ctx context.Context,
	client *gatewayclient.Client,
) error {
	sessions, err := listAllGatewaySessions(ctx, client)
	if err != nil {
		return fmt.Errorf("list managed trajectories: %w", err)
	}
	summaries := make([]managedTrajectorySummary, 0, len(sessions))
	for _, session := range sessions {
		inputs, err := listAllGatewayInputs(ctx, client, session.ID)
		if err != nil {
			return fmt.Errorf("list trajectory %s inputs: %w", session.ID, err)
		}
		interactions, err := listAllPendingGatewayInteractions(
			ctx,
			client,
			session.ID,
		)
		if err != nil {
			return fmt.Errorf("list trajectory %s interactions: %w", session.ID, err)
		}
		summaries = append(
			summaries,
			projectManagedTrajectory(session, inputs, interactions),
		)
	}
	slices.SortFunc(summaries, func(left, right managedTrajectorySummary) int {
		if comparison := right.UpdatedAt.Compare(left.UpdatedAt); comparison != 0 {
			return comparison
		}
		return compareStrings(left.ID, right.ID)
	})
	return application.writeManagedTrajectories(summaries)
}

func listAllPendingGatewayInteractions(
	ctx context.Context,
	client *gatewayclient.Client,
	sessionID string,
) ([]gateway.Interaction, error) {
	query := gateway.InteractionQuery{
		Limit: gatewayEventPageSize,
		State: gateway.InteractionPending,
	}
	var interactions []gateway.Interaction
	for {
		page, err := client.ListInteractions(ctx, sessionID, query)
		if err != nil {
			return nil, err
		}
		interactions = append(interactions, page.Items...)
		if page.Next == 0 || len(page.Items) < gatewayEventPageSize {
			return interactions, nil
		}
		query.After = page.Next
	}
}

func listAllGatewaySessions(
	ctx context.Context,
	client *gatewayclient.Client,
) ([]gateway.Session, error) {
	request := sdk.PageRequest{Limit: sdk.MaxPageSize}
	var sessions []gateway.Session
	for {
		page, err := client.ListSessions(ctx, request)
		if err != nil {
			return nil, err
		}
		sessions = append(sessions, page.Items...)
		if page.Next == "" {
			return sessions, nil
		}
		request.After = page.Next
	}
}

func listAllGatewayInputs(
	ctx context.Context,
	client *gatewayclient.Client,
	sessionID string,
) ([]gateway.AgentInput, error) {
	query := gateway.InputQuery{Limit: gatewayEventPageSize}
	var inputs []gateway.AgentInput
	for {
		page, err := client.ListInputs(ctx, sessionID, query)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, page.Items...)
		if page.Next == 0 || len(page.Items) < gatewayEventPageSize {
			return inputs, nil
		}
		query.After = page.Next
	}
}

func projectManagedTrajectory(
	session gateway.Session,
	inputs []gateway.AgentInput,
	interactions []gateway.Interaction,
) managedTrajectorySummary {
	summary := managedTrajectorySummary{
		ID: session.ID, Status: agentStatusIdle,
		WorkspaceRoot: session.WorkspaceRoot,
		Provider:      session.Provider,
		UpdatedAt:     session.UpdatedAt,
	}
	hasRunning := false
	for _, input := range inputs {
		if input.UpdatedAt.After(summary.UpdatedAt) {
			summary.UpdatedAt = input.UpdatedAt
		}
		if input.State.Terminal() {
			continue
		}
		summary.PendingInputs++
		if input.State == gateway.AgentInputDispatching {
			hasRunning = true
		}
	}
	for _, interaction := range interactions {
		if interaction.State != gateway.InteractionPending {
			continue
		}
		summary.PendingInteraction = interaction.ID
		if interaction.UpdatedAt.After(summary.UpdatedAt) {
			summary.UpdatedAt = interaction.UpdatedAt
		}
	}
	switch {
	case summary.PendingInteraction != "":
		summary.Status = agentStatusWaiting
	case hasRunning:
		summary.Status = agentStatusRunning
	case session.Paused:
		summary.Status = agentStatusPaused
	case summary.PendingInputs > 0:
		summary.Status = agentStatusQueued
	}
	return summary
}

func (application *app) writeManagedTrajectories(
	trajectories []managedTrajectorySummary,
) error {
	return application.render(trajectories, func(writer io.Writer) error {
		if len(trajectories) == 0 {
			_, err := fmt.Fprintln(writer, "No trajectories found.")
			return err
		}
		table := newTable(writer)
		fmt.Fprintln(table, "ID\tSTATUS\tUPDATED\tWORKSPACE")
		for _, trajectory := range trajectories {
			fmt.Fprintf(
				table,
				"%s\t%s\t%s\t%s\n",
				tableCell(trajectory.ID),
				tableCell(trajectory.Status),
				formatTime(trajectory.UpdatedAt),
				tableCell(emptyAs(trajectory.WorkspaceRoot, "-")),
			)
		}
		return table.Flush()
	})
}

func compareStrings(left string, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func (application *app) writeTrajectoryControl(value trajectoryControlOutput) error {
	return application.render(value, func(writer io.Writer) error {
		table := newTable(writer)
		fmt.Fprintf(table, "Trajectory:\t%s\n", tableCell(value.TrajectoryID))
		fmt.Fprintf(table, "Action:\t%s\n", tableCell(value.Action))
		fmt.Fprintf(table, "Status:\t%s\n", tableCell(value.Status))
		if value.InputID != "" {
			fmt.Fprintf(table, "Input:\t%s\n", tableCell(value.InputID))
		}
		if value.Affected != 0 {
			fmt.Fprintf(table, "Affected:\t%d\n", value.Affected)
		}
		return table.Flush()
	})
}
