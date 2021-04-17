package logs

import (
	"os"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

var (
	defaultLogger  = newLogger(&DummyEmitter{})
	emergentLogger = newLogger(&EmergentEmitter{Out: os.Stderr})
)

// DummyEmitter discards log entries sliently.
type DummyEmitter struct {
}

// EmitLogEntry implements LogEmitter.
func (e *DummyEmitter) EmitLogEntry(*logspb.LogEntry) {
}

// Default returns the default logger.
func Default() *Logger {
	return defaultLogger
}

// Emergent returns the emergent logger.
func Emergent() *Logger {
	return emergentLogger
}

// Setup sets up the default logger.
func Setup(emitter LogEmitter) *Logger {
	l := newLogger(emitter)
	defaultLogger = l
	return l
}

// Root creates a root logger.
func Root(emitter LogEmitter) *Logger {
	return newLogger(emitter)
}

func newLogger(emitter LogEmitter) *Logger {
	return &Logger{
		emitter: emitter,
		attrs:   make(map[string]*logspb.Value),
	}
}
