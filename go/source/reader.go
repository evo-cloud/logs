package source

import (
	"context"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// Reader reads log entries.
type Reader interface {
	// Read reads one log entries.
	// On error, it returns nil, err
	// On end, it returns nil, nil
	// After end, it returns nil, io.EOF
	Read(ctx context.Context) (*logspb.LogEntry, error)
}

// FilteredReader filters the entries.
type FilteredReader struct {
	Reader
	Filter LogEntryFilter
}

// Read implements Reader.
func (r *FilteredReader) Read(ctx context.Context) (*logspb.LogEntry, error) {
	for {
		entry, err := r.Reader.Read(ctx)
		if err != nil || entry == nil {
			return entry, err
		}
		if f := r.Filter; f != nil && !f.FilterLogEntry(entry) {
			continue
		}
		return entry, nil
	}
}
