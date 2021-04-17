package source

import (
	"context"
	"io"

	"github.com/evo-cloud/logs/go/blob"
	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// BlobReader reads log entries from a blob stream.
type BlobReader struct {
	reader *blob.Reader
}

// NewBlob creates a BlobReader from a stream.
func NewBlob(in io.Reader) *BlobReader {
	return &BlobReader{reader: &blob.Reader{R: in}}
}

// Read implements Reader.
func (r *BlobReader) Read(ctx context.Context) (*logspb.LogEntry, error) {
	return r.reader.Read()
}
