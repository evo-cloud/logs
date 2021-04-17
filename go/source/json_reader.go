package source

import (
	"bufio"
	"context"
	"io"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"google.golang.org/protobuf/encoding/protojson"
)

// JSONReader reads log entries in newline-separated JSON from a stream.
type JSONReader struct {
	SkipErrors bool

	scanner *bufio.Scanner
	end     bool
}

// NewJSON creates a JSONReader.
func NewJSON(in io.Reader) *JSONReader {
	return &JSONReader{scanner: bufio.NewScanner(in)}
}

// Read implements Reader.
func (r *JSONReader) Read(ctx context.Context) (*logspb.LogEntry, error) {
	if r.end {
		return nil, io.EOF
	}
	for {
		if !r.scanner.Scan() {
			r.end = true
			return nil, nil
		}
		entry := &logspb.LogEntry{}
		if err := protojson.Unmarshal([]byte(r.scanner.Text()), entry); err != nil {
			if r.SkipErrors {
				continue
			}
			return nil, err
		}
		return entry, nil
	}
}
