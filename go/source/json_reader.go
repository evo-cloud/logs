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

	reader *bufio.Reader
	err error
}

// NewJSON creates a JSONReader.
func NewJSON(in io.Reader) *JSONReader {
	return &JSONReader{reader: bufio.NewReader(in)}
}

// Read implements Reader.
func (r *JSONReader) Read(ctx context.Context) (*logspb.LogEntry, error) {
	if r.err != nil {
		return nil, r.err
	}
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			r.err = err
			return nil, err
		}
		entry := &logspb.LogEntry{}
		if err := protojson.Unmarshal([]byte(line), entry); err != nil {
			if r.SkipErrors {
				continue
			}
			return nil, err
		}
		return entry, nil
	}
}
