package server

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

const (
	// DefaultFileSizeLimit defines the limit of each log blob file.
	DefaultFileSizeLimit = 1 << 26 // 64M

	logFileSuffix   = ".logs.blob"
	currentFileName = "current" + logFileSuffix
	maxRecordBody   = 1 << 24 // 16M
)

var (
	// ErrWriterClosed indicates the writer is already closed.
	ErrWriterClosed = errors.New("writer already closed")
	// ErrInvalidData indicates data is invalid in the file.
	ErrInvalidData = errors.New("invalid data")
)

// FileStore persists logs in files.
type FileStore struct {
	BaseDir       string
	FileSizeLimit int64

	writersLock sync.Mutex
	writers     map[string]*fileBatchWriter
}

type fileBatchWriter struct {
	store *FileStore
	name  string
	ref   int32
	dir   string

	lock      sync.Mutex
	file      *os.File
	startTime int64
	size      int64
}

type fileBatchWriterRef struct {
	*fileBatchWriter
}

type encodedRecord struct {
	head []byte
	body []byte
	tail []byte
}

// NewFileStore creates a FileStore.
func NewFileStore(baseDir string) *FileStore {
	return &FileStore{
		BaseDir:       baseDir,
		FileSizeLimit: DefaultFileSizeLimit,
	}
}

// WriteBatch starts write a batch of logs.
func (s *FileStore) WriteBatch(ctx context.Context, name string) (BatchWriter, error) {
	s.writersLock.Lock()
	defer s.writersLock.Unlock()
	w := s.writers[name]
	if w == nil {
		w = &fileBatchWriter{store: s, name: name, dir: filepath.Join(s.BaseDir, name)}
		s.writers[name] = w
	}
	atomic.AddInt32(&w.ref, 1)
	return &fileBatchWriterRef{fileBatchWriter: w}, nil
}

func (w *fileBatchWriterRef) WriteLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	writer := w.fileBatchWriter
	if writer == nil {
		return ErrWriterClosed
	}
	return writer.writeLogEntry(entry)
}

func (w *fileBatchWriterRef) Close() error {
	writer := w.fileBatchWriter
	if writer == nil {
		return ErrWriterClosed
	}
	w.fileBatchWriter = nil
	writer.deref()
	return nil
}

func (w *fileBatchWriter) writeLogEntry(entry *logspb.LogEntry) error {
	rec, err := encodeLogEntry(entry)
	if err != nil {
		return err
	}
	recSize := len(rec.head) + len(rec.body) + len(rec.tail)
	w.lock.Lock()
	defer w.lock.Unlock()

	if w.file == nil {
		if err := w.currentFile(); err != nil {
			return err
		}
		if w.startTime == 0 {
			w.startTime = entry.GetNanoTs()
		}
	}

	if w.size+int64(recSize) > w.store.FileSizeLimit {
		if err := w.rotateFile(); err != nil {
			return err
		}
	}

	if _, err := w.file.Write(rec.head); err != nil {
		return err
	}
	if _, err := w.file.Write(rec.body); err != nil {
		return err
	}
	if _, err := w.file.Write(rec.tail); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.size += int64(recSize)
	return nil
}

func (w *fileBatchWriter) currentFile() error {
	fn := filepath.Join(w.dir, currentFileName)
	info, err := os.Stat(fn)
	if err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(w.dir, 0755)
			f, err := os.Create(fn)
			if err != nil {
				return err
			}
			w.size, w.file = 0, f
			return nil
		}
		return err
	}

	f, err := os.OpenFile(fn, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	if info.Size() > 0 {
		entry, err := readRecordAndDecode(f)
		if err != nil {
			f.Close()
			return err
		}
		w.startTime = entry.GetNanoTs()
	}
	pos, err := f.Seek(0, os.SEEK_END)
	if err != nil {
		f.Close()
		return err
	}
	w.size, w.file = pos, f
	return nil
}

func (w *fileBatchWriter) rotateFile() error {
	fn := filepath.Join(w.dir, currentFileName)
	if w.file != nil {
		w.file.Close()
		w.file, w.size = nil, 0
		rotatedFn := filepath.Join(w.dir, strconv.FormatInt(w.startTime, 10)+logFileSuffix)
		if err := os.Rename(fn, rotatedFn); err != nil {
			return err
		}
	}
	f, err := os.Create(fn)
	if err != nil {
		return err
	}
	w.file = f
	return nil
}

func (w *fileBatchWriter) deref() {
	if atomic.AddInt32(&w.ref, -1) == 0 {
		if w.file != nil {
			w.file.Close()
			w.file = nil
		}
		w.store.writersLock.Lock()
		defer w.store.writersLock.Unlock()
		if writer := w.store.writers[w.name]; writer == w {
			delete(w.store.writers, w.name)
		}
	}
}

func encodeLogEntry(entry *logspb.LogEntry) (*encodedRecord, error) {
	encoded, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}
	rec := &encodedRecord{
		head: make([]byte, 4),
		body: encoded,
	}
	binary.LittleEndian.PutUint32(rec.head, uint32(len(encoded)))
	rec.tail = rec.head
	return rec, nil
}

func readRecord(r io.Reader) (*encodedRecord, error) {
	var rec encodedRecord
	rec.head = make([]byte, 4)
	if _, err := io.ReadFull(r, rec.head); err != nil {
		return nil, err
	}
	size := int64(binary.LittleEndian.Uint32(rec.head))
	if size > maxRecordBody {
		return nil, ErrInvalidData
	}
	rec.body = make([]byte, size+4)
	if _, err := io.ReadFull(r, rec.body); err != nil {
		return nil, err
	}
	rec.tail = rec.body[size:]
	rec.body = rec.body[:size]
	tailSize := int64(binary.LittleEndian.Uint32(rec.tail))
	if tailSize != size {
		return nil, ErrInvalidData
	}
	return &rec, nil
}

func readRecordAndDecode(r io.Reader) (*logspb.LogEntry, error) {
	rec, err := readRecord(r)
	if err != nil {
		return nil, err
	}
	var pb logspb.LogEntry
	if err := proto.Unmarshal(rec.body, &pb); err != nil {
		return nil, err
	}
	return &pb, nil
}
