package cli

import (
	"errors"
	"fmt"
	"io"
	"strings"
)

var errUserCanceled = errors.New("cancelled by user")
var errEarlyExit = errors.New("early exit")

func writeTextError(writer io.Writer, err error) {
	fmt.Fprintf(writer, "ag: %v\n", err)
	if suggestion := suggestionForError(err); suggestion != "" {
		fmt.Fprintf(writer, "hint: %s\n", suggestion)
	}
}

func suggestionForError(err error) string {
	if err == nil {
		return ""
	}
	message := err.Error()
	normalized := strings.ToLower(message)
	var usage usageError
	switch {
	case strings.Contains(message, "--prompt is required"):
		return `pass --prompt "..." or -p "...".`
	case strings.Contains(message, "--session and --resume are mutually exclusive"):
		return "use --session for a new trajectory, or --resume to continue an existing one."
	case strings.Contains(message, "--before is required"):
		return "pass --before with an RFC3339 timestamp or duration such as 720h."
	case strings.Contains(message, "--instance-id requires --name"):
		return "pass --name with --instance-id, for example ag plugin discover --name file --instance-id node-a."
	case errors.Is(err, errUserCanceled):
		return "rerun the command when you are ready; pass --yes to skip the prompt."
	case strings.Contains(normalized, "state.directory or state.backend_uri is required"):
		return "pass --state-dir <directory>, --storage <uri>, or set [state].directory in the config file."
	case strings.Contains(normalized, "plugin registry uri is not configured"):
		return "start a registry with ag registry serve, or pass --registry-uri grpc://host:port."
	case strings.Contains(normalized, "gateway requires a plugin registry"):
		return "start ag registry serve first, then pass its URI with --registry-uri."
	case strings.Contains(normalized, "trajectory not found"):
		return "run ag trajectory list to see available trajectory IDs."
	case strings.Contains(normalized, "invocation root not found"):
		return "check the root invocation ID, or inspect the trajectory JSON for recorded invocation IDs."
	case strings.Contains(normalized, "unknown flag"):
		return "run ag --help or ag <command> --help for valid flags."
	case errors.As(err, &usage):
		return "run ag --help or ag <command> --help for valid arguments."
	default:
		return ""
	}
}
