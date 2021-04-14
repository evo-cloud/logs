package logs

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	"github.com/golang/glog"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

// LogStreamer streams log entries.
type LogStreamer interface {
	StreamLogEntries(ctx context.Context, entries []*logspb.LogEntry) error
}

// StreamEmitter simply emits collected logs.
type StreamEmitter struct {
	Streamer LogStreamer

	emitCh  chan struct{}
	workers int32

	lock    sync.Mutex
	entries *list.List
}

// NewStreamEmitter creates a StreamEmitter.
func NewStreamEmitter(streamer LogStreamer) *StreamEmitter {
	return &StreamEmitter{
		Streamer: streamer,
		emitCh:   make(chan struct{}, 1),
		entries:  list.New(),
	}
}

// EmitLogEntry implements LogEmitter.
func (e *StreamEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	if atomic.LoadInt32(&e.workers) == 0 {
		go e.runWorker(context.Background())
	}
	e.lock.Lock()
	e.entries.PushBack(entry)
	e.lock.Unlock()
	select {
	case e.emitCh <- struct{}{}:
	default:
	}
}

func (e *StreamEmitter) runWorker(ctx context.Context) {
	defer func() {
		atomic.AddInt32(&e.workers, -1)
	}()
	if atomic.AddInt32(&e.workers, 1) > 1 {
		return
	}
	for {
		e.emitEntries(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.emitCh:
		}
	}
}

func (e *StreamEmitter) emitEntries(ctx context.Context) {
	e.lock.Lock()
	entryList := e.entries
	e.entries = list.New()
	e.lock.Unlock()
	if entryList.Len() == 0 {
		return
	}
	entries := make([]*logspb.LogEntry, 0, entryList.Len())
	for elem := entryList.Front(); elem != nil; elem = elem.Next() {
		entries = append(entries, elem.Value.(*logspb.LogEntry))
	}

	if err := e.Streamer.StreamLogEntries(ctx, entries); err != nil {
		glog.Errorf("Stream log entries error: %v", err)
	}
}
