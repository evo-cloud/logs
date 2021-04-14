package logs

import (
	"sync"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

const (
	maxEntriesPerPage = 1024
)

// LimitedEmitter is an in-memory storage of most recent logs with a max cap of total size.
type LimitedEmitter struct {
	MaxSize int

	lock      sync.RWMutex
	pages     []*logPage
	totalSize int
	writePage int
	startPage int
}

type logPage struct {
	entries    []*logspb.LogEntry
	totalSize  int
	entryCount int
}

// NewLimitedEmitter creates a LimitedEmitter.
func NewLimitedEmitter(maxSize, pageCount int) *LimitedEmitter {
	return &LimitedEmitter{
		MaxSize: maxSize,
		pages:   make([]*logPage, pageCount),
	}
}

// EmitLogEntry implements LogEmitter.
func (e *LimitedEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	size := proto.Size(entry)
	e.lock.Lock()
	defer e.lock.Unlock()
	page := e.pages[e.writePage]
	if page != nil && page.entryCount >= len(page.entries) {
		e.writePage++
		if e.writePage >= len(e.pages) {
			e.writePage = 0
		}
		if e.startPage == e.writePage {
			e.startPage++
			if e.startPage >= len(e.pages) {
				e.startPage = 0
			}
		}
		page = e.pages[e.writePage]
		if page != nil {
			page.entryCount = 0
			page.totalSize = 0
		}
	}
	if page == nil {
		page = &logPage{
			entries: make([]*logspb.LogEntry, maxEntriesPerPage),
		}
		e.pages[e.writePage] = page
	}
	page.entries[page.entryCount] = entry
	page.entryCount++
	page.totalSize += size
	e.totalSize += size

	for e.totalSize > e.MaxSize && e.startPage != e.writePage {
		e.totalSize -= e.pages[e.startPage].totalSize
		e.startPage++
		if e.startPage >= len(e.pages) {
			e.startPage = 0
		}
	}
}
