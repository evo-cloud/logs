package console

import (
	"fmt"

	"google.golang.org/protobuf/encoding/protojson"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// Emitter implements an emitter to emit logs to console.
type Emitter struct {
	Printer *Printer
	JSON    bool
}

// EmitLogEntry implements LogEmitter.
func (e *Emitter) EmitLogEntry(entry *logspb.LogEntry) {
	if e.JSON {
		fmt.Fprintln(e.Printer.Out, protojson.MarshalOptions{Multiline: false, UseProtoNames: true}.Format(entry))
		return
	}
	e.Printer.EmitLogEntry(entry)
}
