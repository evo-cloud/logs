package server

import (
	"context"
	"io"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// LogStore is the abstraction of log storage.
type LogStore interface {
	WriteBatch(ctx context.Context, name string) (BatchWriter, error)
	WriteStream(ctx context.Context, name string) (logs.LogEmitter, error)
}

// BatchWriter writes logs in batch.
type BatchWriter interface {
	io.Closer
	WriteLogEntry(ctx context.Context, entry *logspb.LogEntry) error
}
