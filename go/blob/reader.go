package blob

import (
	"encoding/binary"
	"fmt"
	"io"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// Reader reads log entries.
type Reader struct {
	R io.Reader
}

// Read reads one entry.
func (r *Reader) Read() (*logspb.LogEntry, error) {
	buf := make([]byte, 4)
	if _, err := io.ReadFull(r.R, buf); err != nil {
		return nil, err
	}
	size := int32(binary.LittleEndian.Uint32(buf))
	if size <= 0 {
		return nil, fmt.Errorf("head size %d invalid: %w", size, ErrBadRecord)
	}
	paddedSize := int(size)
	if rest := size & 3; rest != 0 {
		paddedSize += 4 - int(rest)
	}
	buf = make([]byte, paddedSize+4)
	if _, err := io.ReadFull(r.R, buf); err != nil {
		return nil, err
	}
	tailSize := int32(binary.LittleEndian.Uint32(buf[paddedSize:]))
	if tailSize != size {
		return nil, fmt.Errorf("tail size %d not match head size %d: %w", tailSize, size, ErrBadRecord)
	}
	var entry logspb.LogEntry
	if err := proto.Unmarshal(buf[:size], &entry); err != nil {
		return nil, err
	}
	return &entry, nil
}

// Close implements io.Closer.
func (r *Reader) Close() error {
	if closer, ok := r.R.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
