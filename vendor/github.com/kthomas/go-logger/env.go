package logger

import (
	"os"

	log "github.com/kthomas/logrus"
)

var defaultLogFormatter log.Formatter

func init() {
	if os.Getenv("LOG_FORMATTER") == "json" {
		defaultLogFormatter = &log.JSONFormatter{}
	} else {
		defaultLogFormatter = &log.TextFormatter{}
	}
}
