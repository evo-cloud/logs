package logs

import (
	"container/list"
	"context"
	"sync"
	"sync/atomic"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// BatchEmitter emits log entries in batch.
type BatchEmitter interface {
	EmitLogEntries(ctx context.Context, entries []*logspb.LogEntry) error
}

// Emit log entries in batch asynchronously.
type AsyncBatchEmitter struct {
	Emitter BatchEmitter

	emitCh  chan struct{}
	workers int32

	lock    sync.Mutex
	entries *list.List
}

// NewAsyncBatchEmitter creates an AsyncBatchEmitter.
func NewAsyncBatchEmitter(emitter BatchEmitter) *AsyncBatchEmitter {
	return &AsyncBatchEmitter{
		Emitter: emitter,
		emitCh:  make(chan struct{}, 1),
		entries: list.New(),
	}
}

// EmitLogEntry implements LogEmitter.
func (e *AsyncBatchEmitter) EmitLogEntry(entry *logspb.LogEntry) {
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

func (e *AsyncBatchEmitter) runWorker(ctx context.Context) {
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

func (e *AsyncBatchEmitter) emitEntries(ctx context.Context) {
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

	if err := e.Emitter.EmitLogEntries(ctx, entries); err != nil {
		Emergent().Error(err).PrintErr("Stream: ")
	}
}
