package console

import (
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
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

// Emitter implements an emitter to emit logs to console.
type Emitter struct {
	Out  io.Writer
	JSON bool
}

func levelStr(level logspb.LogEntry_Level) string {
	str := levelStrs[level]
	if str == "" {
		str = " "
	}
	return str
}

// EmitLogEntry implements LogEmitter.
func (e *Emitter) EmitLogEntry(entry *logspb.LogEntry) {
	if e.JSON {
		fmt.Fprintln(e.Out, protojson.MarshalOptions{Multiline: false, UseProtoNames: true}.Format(entry))
		return
	}
	var sb strings.Builder
	sb.WriteString(levelStr(entry.GetLevel()))
	sb.WriteString(time.Unix(0, entry.GetNanoTs()).Format("0102 15:04:05.000000 "))
	if loc := entry.GetLocation(); loc != "" {
		sb.WriteString(filepath.Base(loc))
		sb.WriteByte(' ')
	}
	sb.WriteString(entry.GetMessage())
	fmt.Fprintln(e.Out, sb.String())
}
