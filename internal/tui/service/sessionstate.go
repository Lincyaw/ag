package service

// SessionState holds the current session's display state.
// This is a stub — the ag runtime provides session state through its own interfaces.
type SessionState struct {
	title string
}

func (s *SessionState) SessionTitle() string { return s.title }
func (s *SessionState) SetTitle(t string)    { s.title = t }

// SessionStateReader provides read-only access to session state.
type SessionStateReader interface {
	HideToolResults() bool
	SessionTitle() string
}
