package logs

import (
	"fmt"
	"strings"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// ParseLevel parses a human friendly level string to log level.
func ParseLevel(str string) (logspb.LogEntry_Level, error) {
	level := logspb.LogEntry_NONE
	switch strings.ToLower(str) {
	case "", "no", "none":
	case "i", "info":
		level = logspb.LogEntry_INFO
	case "w", "warn", "warning":
		level = logspb.LogEntry_WARNING
	case "e", "err", "error":
		level = logspb.LogEntry_ERROR
	case "c", "crit", "critical":
		level = logspb.LogEntry_CRITICAL
	case "f", "fatal":
		level = logspb.LogEntry_FATAL
	default:
		return level, fmt.Errorf("unknown level: %s", str)
	}
	return level, nil
}
