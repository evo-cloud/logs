package blob

import (
	"bytes"
	"errors"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/evo-cloud/logs/go/blob"
	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// Emitter emits log entries encoded in binary protos.
type Emitter struct {
	CreateFile func() (io.Writer, error)
	Sync       bool
	SizeLimit  int64

	writerLock sync.RWMutex
	writer     *blob.Writer
}

// EmitLogEntry implements LogEmitter.
func (e *Emitter) EmitLogEntry(entry *logspb.LogEntry) {
	e.writerLock.RLock()
	w := e.writer
	e.writerLock.RUnlock()
	for {
		var err error
		if w != nil {
			if err = w.WriteLogEntry(entry); err == nil {
				return
			}
			e.closeWriter()
			if !errors.Is(err, blob.ErrSizeLimitExceeded) {
				logs.Emergent().Error(err).PrintErr("BlobWriter: ")
				return
			}
		}
		if w, err = e.newFile(); err != nil {
			logs.Emergent().Error(err).PrintErr("BlobWriter CreateFile: ")
			return
		}
	}
}

func (e *Emitter) closeWriter() {
	e.writerLock.Lock()
	if e.writer != nil {
		e.writer.Close()
		e.writer = nil
	}
	e.writerLock.Unlock()
}

func (e *Emitter) newFile() (*blob.Writer, error) {
	e.writerLock.Lock()
	defer e.writerLock.Unlock()
	f, err := e.CreateFile()
	if err != nil {
		return nil, err
	}
	e.writer = &blob.Writer{W: f, Sync: e.Sync, SizeLimit: e.SizeLimit}
	return e.writer, nil
}

type filenameTemplateContext struct {
	Timestamp int64
	Nanos     int64
	Sequence  int64
}

// CreateFileWith returns a CreateFile func which creates a file using the filenameTemplate.
// In the filenameTemplate, the following substitutions are supported:
// - {{.Timestamp}} a timestamp in unix seconds.
// - {{.Nanos}} the nano seconds part of the timestamp.
// - {{.Sequence}} auto-incremented sequence, starting from 0.
func CreateFileWith(filenameTemplate string) (func() (io.Writer, error), error) {
	tpl, err := template.New("").Parse(filenameTemplate)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	tplCtx := &filenameTemplateContext{}
	return func() (io.Writer, error) {
		now := time.Now()
		tplCtx.Timestamp = now.Unix()
		tplCtx.Nanos = now.UnixNano() - now.Unix()*1e9
		var out bytes.Buffer
		if err := tpl.Execute(&out, tplCtx); err != nil {
			return nil, fmt.Errorf("generate filename: %w", err)
		}
		fn := out.String()
		os.MkdirAll(filepath.Dir(fn), 0755)
		f, err := os.Create(fn)
		if err != nil {
			return nil, err
		}
		tplCtx.Sequence++
		return f, nil
	}, nil
}
