package gateway

import (
	"encoding/json"
	"errors"
	"unicode/utf8"

	"github.com/lincyaw/ag/sdk"
	agentruntime "github.com/lincyaw/ag/sdk/runtime"
)

const (
	defaultConversationPageSize = 100
	maxConversationPageSize     = 1000
	conversationChunkBytes      = 1 << 20
	conversationPageBytes       = 4 << 20
)

type ConversationMessage struct {
	Role         sdk.Role `json:"role"`
	Content      string   `json:"content"`
	Continuation bool     `json:"continuation,omitempty"`
}

type ConversationQuery struct {
	After uint64 `json:"after,omitempty"`
	Limit int    `json:"limit,omitempty"`
}

type ConversationPage struct {
	Head      string                   `json:"head,omitempty"`
	Execution *sdk.TrajectoryExecution `json:"execution,omitempty"`
	Items     []ConversationMessage    `json:"items"`
	Next      uint64                   `json:"next,omitempty"`
}

func projectConversationPage(
	trajectory sdk.Trajectory,
	query ConversationQuery,
) (ConversationPage, error) {
	messages, err := agentruntime.ProjectTrajectoryMessages(trajectory)
	if err != nil {
		return ConversationPage{}, err
	}
	return projectConversationMessagesPage(
		trajectory.Head,
		trajectory.Execution,
		messages,
		query,
	)
}

func projectConversationMessagesPage(
	head string,
	execution *sdk.TrajectoryExecution,
	messages []sdk.Message,
	query ConversationQuery,
) (ConversationPage, error) {
	query, err := normalizeConversationQuery(query)
	if err != nil {
		return ConversationPage{}, err
	}
	chunks := conversationChunks(messages)
	page := ConversationPage{
		Head:      head,
		Execution: sdk.CloneTrajectoryExecution(execution),
		Items:     make([]ConversationMessage, 0, query.Limit),
	}
	if query.After >= uint64(len(chunks)) {
		return page, nil
	}
	pageBytes := 0
	for index := int(query.After); index < len(chunks); index++ {
		item := chunks[index]
		if len(page.Items) >= query.Limit {
			break
		}
		encoded, err := json.Marshal(item)
		if err != nil {
			return ConversationPage{}, err
		}
		if len(page.Items) > 0 && pageBytes+len(encoded) > conversationPageBytes {
			break
		}
		page.Items = append(page.Items, item)
		pageBytes += len(encoded)
	}
	end := query.After + uint64(len(page.Items))
	if end < uint64(len(chunks)) {
		page.Next = end
	}
	return page, nil
}

func normalizeConversationQuery(
	query ConversationQuery,
) (ConversationQuery, error) {
	if query.Limit == 0 {
		query.Limit = defaultConversationPageSize
	}
	if query.Limit < 1 || query.Limit > maxConversationPageSize {
		return ConversationQuery{}, errors.New(
			"conversation page limit must be between 1 and 1000",
		)
	}
	return query, nil
}

func conversationChunks(messages []sdk.Message) []ConversationMessage {
	var result []ConversationMessage
	for _, message := range messages {
		if (message.Role != sdk.RoleUser && message.Role != sdk.RoleAssistant) ||
			message.Content == "" {
			continue
		}
		content := message.Content
		continuation := false
		for content != "" {
			split := conversationChunkBoundary(content, conversationChunkBytes)
			result = append(result, ConversationMessage{
				Role: message.Role, Content: content[:split],
				Continuation: continuation,
			})
			content = content[split:]
			continuation = true
		}
	}
	return result
}

func conversationChunkBoundary(content string, limit int) int {
	if len(content) <= limit {
		return len(content)
	}
	boundary := limit
	for boundary > 0 && !utf8.RuneStart(content[boundary]) {
		boundary--
	}
	if boundary == 0 {
		_, size := utf8.DecodeRuneInString(content)
		return size
	}
	return boundary
}
