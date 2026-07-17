package cli

import (
	"io"
	"log/slog"

	appconfig "github.com/lincyaw/ag/internal/config"
	"github.com/lincyaw/ag/internal/logging"
)

func openConfiguredLogger(
	config appconfig.Logging,
	stderr io.Writer,
) (*slog.Logger, io.Closer, error) {
	var console io.Writer
	if config.Console {
		console = stderr
	}
	return logging.OpenFile(logging.Config{
		Level:  config.Level,
		Format: config.Format,
	}, config.File, console)
}
