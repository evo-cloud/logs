package logs

import (
	"io"
	"path/filepath"
	"strings"
	"time"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

var (
	levelStrs = map[logspb.LogEntry_Level]string{
		logspb.LogEntry_INFO:     "I",
		logspb.LogEntry_WARNING:  "W",
		logspb.LogEntry_ERROR:    "E",
		logspb.LogEntry_CRITICAL: "C",
		logspb.LogEntry_FATAL:    "F",
	}
)

// EmergentEmitter is used for log the errors from the logging system
// without emit log entries to other emitters.
type EmergentEmitter struct {
	Out io.Writer
}

// EmitLogEntry implements LogEmitter.
func (e *EmergentEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	var sb strings.Builder
	sb.WriteString("LOGE:")
	str := levelStrs[entry.GetLevel()]
	if str == "" {
		str = " "
	}
	sb.WriteString(str)
	sb.WriteString(time.Unix(0, entry.GetNanoTs()).Format("0102 15:04:05.000000 "))
	if loc := entry.GetLocation(); loc != "" {
		sb.WriteString(filepath.Base(loc))
		sb.WriteByte(' ')
	}
	sb.WriteString(entry.GetMessage())
	sb.WriteString("\r\n")
	io.WriteString(e.Out, sb.String())
}
