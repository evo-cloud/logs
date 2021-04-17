package logs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

const (
	defaultCollectPeriod = time.Second
)

// ChunkedStreamer streams log entries in chunks.
type ChunkedStreamer interface {
	StartStreamInChunk(ctx context.Context, info ChunkInfo) (ChunkedLogStreamer, error)
}

// ChunkedLogStreamer streams a chunk of log entries one by one.
type ChunkedLogStreamer interface {
	// StreamLogEntry streams a single log entry.
	// If err is returned, no more records are sent, and StreamEnd will be called to get the last received nanoTS.
	StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error
	// StreamEnd completes the chunk. Regardless of err, the last received nanoTS is expected
	// to decide which records has been successfully received.
	StreamEnd(ctx context.Context) (int64, error)
}

// ChunkInfo provides general information of a chunk.
type ChunkInfo struct {
	TotalSize   int
	NumEntries  int
	FirstNanoTS int64
	LastNanoTS  int64
}

// ChunkedEmitter emits logs in chunks and maintains a buffer of serialized log entries capped at maximum size.
type ChunkedEmitter struct {
	Streamer      ChunkedStreamer
	MaxSize       int
	ChunkSize     int
	CollectPeriod time.Duration

	emitCh  chan struct{}
	workers int32

	lock      sync.Mutex
	first     *record
	last      *record
	totalSize int
}

type record struct {
	entry *logspb.LogEntry
	size  int
	next  *record
}

// NewChunkedEmitter creates a ChunkedEmitter.
func NewChunkedEmitter(streamer ChunkedStreamer, maxSize, chunkSize int) *ChunkedEmitter {
	return &ChunkedEmitter{
		Streamer:      streamer,
		MaxSize:       maxSize,
		ChunkSize:     chunkSize,
		CollectPeriod: defaultCollectPeriod,
		emitCh:        make(chan struct{}, 1),
	}
}

// EmitLogEntry implements LogEmitter.
func (e *ChunkedEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	if atomic.LoadInt32(&e.workers) == 0 {
		go e.runWorker(context.Background())
	}
	rec := &record{entry: entry, size: proto.Size(entry)}
	e.lock.Lock()
	defer e.lock.Unlock()
	if e.last == nil {
		e.first, e.last = rec, rec
	} else {
		e.last.next = rec
		e.last = rec
	}
	e.totalSize += rec.size
	var lostSize int
	for e.totalSize > e.MaxSize && e.first != nil {
		e.totalSize -= e.first.size
		lostSize += e.first.size
		e.first = e.first.next
	}
	if e.first == nil {
		e.last = nil
	}
	if lostSize > 0 {
		Emergent().Errorf("Overrun %d bytes of records", lostSize)
	}
	select {
	case e.emitCh <- struct{}{}:
	default:
	}
}

func (e *ChunkedEmitter) runWorker(ctx context.Context) {
	defer func() {
		atomic.AddInt32(&e.workers, -1)
	}()
	if atomic.AddInt32(&e.workers, 1) > 1 {
		return
	}
	for {
		e.emitChunks(ctx)
		select {
		case <-ctx.Done():
			return
		case <-e.emitCh:
		case <-time.After(e.CollectPeriod):
		}
	}
}

func (e *ChunkedEmitter) emitChunks(ctx context.Context) {
	head, tail, info := e.fetchChunk()
	if info.NumEntries == 0 {
		return
	}
	var lastTS int64
	rs, err := e.Streamer.StartStreamInChunk(ctx, *info)
	if err != nil {
		Emergent().Error(err).PrintErr("StartStreamChunk: ")
	} else {
		for rec := head; rec != nil; rec = rec.next {
			if err := rs.StreamLogEntry(ctx, rec.entry); err != nil {
				Emergent().Error(err).PrintErrf("StreamRecord(%v): ", rec.entry.GetNanoTs())
				break
			}
		}
		lastTS, err = rs.StreamEnd(ctx)
		if err != nil {
			Emergent().Error(err).PrintErr("StreamEnd: ")
		}
	}

	// Discard received records.
	for head != nil && head.entry.GetNanoTs() <= lastTS {
		info.TotalSize -= head.size
		head = head.next
	}

	if head == nil {
		return
	}

	// Not all records received, requeue the rest of records.
	e.lock.Lock()
	defer e.lock.Unlock()
	totalSize := info.TotalSize + e.totalSize
	var lostSize, returnedSize int
	for head != nil && totalSize > e.MaxSize {
		totalSize -= head.size
		lostSize += head.size
		head = head.next
	}
	if head != nil {
		tail.next = e.first
		e.first = head
		returnedSize = totalSize - e.totalSize
		e.totalSize = totalSize
	}
	Emergent().Errorf("Returned %d bytes, discarded %d bytes", returnedSize, lostSize)
}

func (e *ChunkedEmitter) fetchChunk() (*record, *record, *ChunkInfo) {
	var info ChunkInfo
	e.lock.Lock()
	defer e.lock.Unlock()
	var head, tail *record
	for e.first != nil && info.TotalSize < e.ChunkSize {
		if info.TotalSize+e.first.size > e.ChunkSize {
			break
		}
		if tail == nil {
			head, tail = e.first, e.first
		} else {
			tail = e.first
		}
		info.TotalSize += e.first.size
		info.NumEntries++
		e.first = e.first.next
	}
	if e.first == nil {
		e.last = nil
	}
	if tail != nil {
		tail.next = nil
	}
	return head, tail, &info
}
