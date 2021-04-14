package logs

import logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"

// MultiEmitter emits log entry to multiple emitters.
type MultiEmitter []LogEmitter

// EmitLogEntry implements LogEmitter.
func (e MultiEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	for _, emitter := range e {
		emitter.EmitLogEntry(entry)
	}
}
