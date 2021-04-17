package logs

import (
	"sync"

	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
)

// SpanAssembler assembles spans from a stream of log entries.
// The log entries must be added in the time order.
type SpanAssembler struct {
	spans sync.Map
}

// AddLogEntry add a log entry for assembling.
func (a *SpanAssembler) AddLogEntry(entry *logspb.LogEntry) *logspb.Span {
	id := IDStringFrom(entry.GetTrace().GetSpanContext())
	if id == "" {
		return nil
	}
	var completedSpan *logspb.Span
	switch entry.GetTrace().GetEvent().(type) {
	case *logspb.Trace_SpanStart_:
		completedSpan = a.spanStart(id, entry)
	case *logspb.Trace_SpanEnd_:
		completedSpan = a.spanEnd(id, entry)
	default:
		a.regularLog(id, entry)
	}
	return completedSpan
}

func (a *SpanAssembler) spanStart(id string, entry *logspb.LogEntry) *logspb.Span {
	event := entry.GetTrace().GetSpanStart()
	span := &logspb.Span{
		Context:    proto.Clone(entry.GetTrace().GetSpanContext()).(*logspb.SpanContext),
		Name:       event.GetName(),
		Kind:       event.GetKind(),
		StartNs:    entry.GetNanoTs(),
		Attributes: entry.Attributes,
		Links:      event.Links,
	}
	span.Logs = append(span.Logs, entry)
	a.spans.Store(id, span)
	return nil
}

func (a *SpanAssembler) spanEnd(id string, entry *logspb.LogEntry) *logspb.Span {
	val, ok := a.spans.LoadAndDelete(id)
	if !ok {
		return nil
	}
	span := val.(*logspb.Span)
	span.Logs = append(span.Logs, entry)
	span.Duration = entry.NanoTs - span.StartNs
	return span
}

func (a *SpanAssembler) regularLog(id string, entry *logspb.LogEntry) {
	val, ok := a.spans.Load(id)
	if !ok {
		return
	}
	span := val.(*logspb.Span)
	span.Logs = append(span.Logs, entry)
}
