package messages

// Attachment represents content attached to a message.
type Attachment struct {
	Name     string
	FilePath string
	Content  string
}

// Session lifecycle messages control session state and persistence.
type (
	NewSessionMsg             struct{}
	ClearSessionMsg           struct{}
	ExitSessionMsg            struct{}
	ExitAfterFirstResponseMsg struct{}
	CompactSessionMsg         struct{ AdditionalPrompt string }
	CopySessionToClipboardMsg struct{ Argument string }
	UndoSnapshotMsg           struct{}
	ShowSnapshotsDialogMsg    struct{}
	ResetSnapshotMsg          struct{ Keep int }
	ExportSessionMsg          struct{ Filename string }
	ShowExportDialogMsg       struct{}
	BackgroundSessionMsg      struct{}
	OpenSessionBrowserMsg     struct{}
	LoadSessionMsg            struct{ SessionID string }
	ToggleSessionStarMsg      struct{ SessionID string }
	DeleteSessionMsg          struct{ SessionID string }
	SessionStarChangedMsg     struct {
		SessionID string
		Starred   bool
	}
	SessionDeletedMsg   struct{ SessionID string }
	SetSessionTitleMsg  struct{ Title string }
	RegenerateTitleMsg  struct{}
	ForkSessionMsg      struct{}
	StreamCancelledMsg  struct{ ShowMessage bool }
	ToggleSplitDiffMsg  struct{}
	SendMsg             struct {
		Content     string
		Attachments []Attachment
		BypassQueue bool
		QueueIfBusy bool
	}
	SendAttachmentMsg struct{ Content any }
)

// OpenSessionBrowserWithDataMsg opens the session browser with pre-fetched data.
type OpenSessionBrowserWithDataMsg struct{ Sessions []any }

func (OpenSessionBrowserWithDataMsg) GetAgentName() string { return "" }
