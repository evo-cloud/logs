package source

import (
	"bytes"
	"context"
	"io"
	"strings"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

const (
	whiteSpaces = " \t\r\n"
	maxPreRead  = 4096
)

// StreamReader auto detects content from a stream to decode log entries.
type StreamReader struct {
	In         io.Reader
	SkipErrors bool

	preRead bytes.Buffer
	reader  Reader
}

// Read implements Reader.
func (r *StreamReader) Read(ctx context.Context) (*logspb.LogEntry, error) {
	if r.reader != nil {
		return r.reader.Read(ctx)
	}
	for {
		b := []byte{0}
		_, err := r.In.Read(b)
		if err != nil {
			return nil, err
		}
		r.preRead.Write(b)
		if strings.IndexByte(whiteSpaces, b[0]) >= 0 {
			if r.preRead.Len() > maxPreRead {
				r.preRead.Reset()
			}
			continue
		}
		if b[0] == '{' {
			jsonReader := NewJSON(io.MultiReader(bytes.NewBuffer(b), r.In))
			jsonReader.SkipErrors = r.SkipErrors
			r.reader = jsonReader
		} else {
			r.reader = NewBlob(io.MultiReader(&r.preRead, r.In))
		}
		break
	}
	return r.reader.Read(ctx)
}

// Close implements io.Closer.
func (r *StreamReader) Close() error {
	if closer, ok := r.In.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}
