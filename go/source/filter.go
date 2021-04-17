package source

import (
	"strings"
	"time"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// LogEntryFilter defines the interface to filter log entries.
type LogEntryFilter interface {
	FilterLogEntry(entry *logspb.LogEntry) bool
}

// LogEntryFilterFunc is the func form of LogEntryFilter.
type LogEntryFilterFunc func(entry *logspb.LogEntry) bool

// FilterLogEntry implements LogEntryFilter.
func (f LogEntryFilterFunc) FilterLogEntry(entry *logspb.LogEntry) bool {
	return f(entry)
}

// LogEntryFilters is a slice of LogEntryFilter instances and can be
// used as a LogEntryFilter.
type LogEntryFilters []LogEntryFilter

// FilterLogEntry implements LogEntryFilter.
func (f LogEntryFilters) FilterLogEntry(entry *logspb.LogEntry) bool {
	for _, filter := range f {
		if !filter.FilterLogEntry(entry) {
			return false
		}
	}
	return true
}

// TimeRangeFilter filters logs by start and end time.
// Both Since/Before are optional (ignored if IsZero is true).
type TimeRangeFilter struct {
	// Since defines the start time, inclusive.
	// LogEntry.nano_ts >= Since.
	Since time.Time
	// Before defines the end time, exclusive.
	// LogEntry.nano_ts < Before.
	Before time.Time
}

// FilterLogEntry implements LogEntryFilter.
func (f *TimeRangeFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	if !f.Since.IsZero() && f.Since.UnixNano() > entry.GetNanoTs() {
		return false
	}
	if !f.Before.IsZero() && f.Before.UnixNano() <= entry.GetNanoTs() {
		return false
	}
	return true
}

// AndSince can be chained to add since constraint.
func (f *TimeRangeFilter) AndSince(t time.Time) *TimeRangeFilter {
	f.Since = t
	return f
}

// AndBefore can be chained to add before constraint.
func (f *TimeRangeFilter) AndBefore(t time.Time) *TimeRangeFilter {
	f.Before = t
	return f
}

// FilterSince creates a filter with start time.
func FilterSince(t time.Time) *TimeRangeFilter {
	return &TimeRangeFilter{Since: t}
}

// FilterBefore creates a filter with end time.
func FilterBefore(t time.Time) *TimeRangeFilter {
	return &TimeRangeFilter{Before: t}
}

// FilterByTime creates a filter with start/end time range.
func FilterByTime(since, before time.Time) *TimeRangeFilter {
	return &TimeRangeFilter{Since: since, Before: before}
}

// LevelFilter filters logs by severity levels.
type LevelFilter struct {
	// MinLevel specifies the minimal log level that entry must match or exceed.
	// entry.level >= MinLevel.
	MinLevel logspb.LogEntry_Level
	// MaxLevel specifies the log level that entry must not reach or exceed.
	// entry.level < MaxLevel.
	MaxLevel logspb.LogEntry_Level
}

// FilterLogEntry implements LogEntryFilter.
func (f *LevelFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	level := entry.GetLevel()
	if level < f.MinLevel {
		return false
	}
	if f.MaxLevel != logspb.LogEntry_NONE && f.MaxLevel <= level {
		return false
	}
	return true
}

// AndBelow can be chained to limit the maximum level of logs.
func (f *LevelFilter) AndBelow(level logspb.LogEntry_Level) *LevelFilter {
	f.MaxLevel = level
	return f
}

// FilterByLevel creates a LevelFilter.
func FilterByLevel(level logspb.LogEntry_Level) *LevelFilter {
	return &LevelFilter{MinLevel: level}
}

// TraceSpanFilter filters logs by trace/span IDs.
type TraceSpanFilter struct {
	// TraceIDContains specifies the partial hex string to be included in full trace ID.
	TraceIDContains string
	// The partial hex string to be included in full span ID.
	SpanIDContains string
}

// FilterLogEntry implements LogEntryFilter.
func (f *TraceSpanFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	if f.TraceIDContains != "" &&
		!strings.Contains(logs.TraceIDStringFrom(entry.GetTrace().GetSpanContext()), f.TraceIDContains) {
		return false
	}
	if f.SpanIDContains != "" &&
		!strings.Contains(logs.SpanIDStringFrom(entry.GetTrace().GetSpanContext()), f.SpanIDContains) {
		return false
	}
	return true
}

// AndSpan can be chained to filter by span ID.
func (f *TraceSpanFilter) AndSpan(spanIDContains string) *TraceSpanFilter {
	f.SpanIDContains = spanIDContains
	return f
}

// FilterByTrace creates a TraceSpanFilter with partial traceID.
func FilterByTrace(traceIDContains string) *TraceSpanFilter {
	return &TraceSpanFilter{TraceIDContains: traceIDContains}
}

// LocationFilter filter logs by location.
type LocationFilter struct {
	// Contains specifies the substring to be contained in the location.
	Contains string
	// Reverse if set true, matches entries without containing Contains.
	Reverse bool
}

// FilterLogEntry implements LogEntryFilter.
func (f *LocationFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	contains := strings.Contains(entry.GetLocation(), f.Contains)
	if f.Reverse {
		return !contains
	}
	return contains
}

// LocationContains returns a LocationFilter for matching entries containing substr in location.
func LocationContains(substr string) *LocationFilter {
	return &LocationFilter{Contains: substr}
}

// LocationContains returns a LocationFilter for matching entries not containing substr in location.
func LocationNotContains(substr string) *LocationFilter {
	return &LocationFilter{Contains: substr, Reverse: true}
}

// SpanEventFilter filter logs by matching span events.
type SpanEventFilter struct {
	// Exclude excludes entries representing span events.
	Exclude bool
}

// FilterLogEntry implements LogEntryFilter.
func (f *SpanEventFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	if f.Exclude && entry.GetTrace().GetEvent() != nil {
		return false
	}
	return true
}

// ExcludeSpanEvents returns a SpanEventFilter to exclude all span events.
func ExcludeSpanEvents() *SpanEventFilter {
	return &SpanEventFilter{Exclude: true}
}

// MessageFilter filter logs by matching message content.
type MessageFilter struct {
	Contains string
}

// FilterLogEntry implements LogEntryFilter.
func (f *MessageFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	return strings.Contains(entry.GetMessage(), f.Contains)
}

// MessageContains returns a MessageFilter to match substr in messages.
func MessageContains(substr string) *MessageFilter {
	return &MessageFilter{Contains: substr}
}
