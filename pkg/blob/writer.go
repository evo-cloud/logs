package blob

import (
	"errors"
	"io"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

var (
	// ErrSizeLimitExceeded indicates the file size will exceed limit if an entry is written.
	ErrSizeLimitExceeded = errors.New("size limit exceeded")
)

// Writer is blob writer.
type Writer struct {
	W           io.Writer
	Sync        bool
	SizeLimit   int64
	WrittenSize int64
}

// Syncable defines a writer supports Sync().
type Syncable interface {
	Sync() error
}

// Flushable defines a write implements Flush().
type Flushable interface {
	Flush()
}

// Close implements io.Closer.
func (w *Writer) Close() error {
	if closer, ok := w.W.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// WriteLogEntry writes singe log entry.
func (w *Writer) WriteLogEntry(entry *logspb.LogEntry) error {
	if w.SizeLimit > 0 && w.WrittenSize+int64(RawRecordSize(entry)) > w.SizeLimit {
		return ErrSizeLimitExceeded
	}
	rec, err := EncodeToRawRecord(entry)
	if err != nil {
		return err
	}
	if _, err := w.W.Write(rec.Head); err != nil {
		return err
	}
	if _, err := w.W.Write(rec.Body); err != nil {
		return err
	}
	if _, err := w.W.Write(rec.Tail); err != nil {
		return err
	}
	w.WrittenSize += int64(len(rec.Head) + len(rec.Body) + len(rec.Tail))
	if w.Sync {
		if s, ok := w.W.(Syncable); ok {
			return s.Sync()
		}
		if f, ok := w.W.(Flushable); ok {
			f.Flush()
		}
	}
	return nil
}
