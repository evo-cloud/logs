package source

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

var (
	attrFilterRegexp = regexp.MustCompile(`^([^:=~<>!]+)(=|:|~|<|>|!=)(.*)$`)
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
func (f TimeRangeFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
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
func (f LevelFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
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
func (f TraceSpanFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
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
	// ContainsAny specifies the substrings to be contained in the location.
	// It matches when at least one matches.
	ContainsAny []string
	// ContainsAll specifies the substrings must all be matched.
	ContainsAll []string
	// NotContains specifies the substrings none should be matched.
	NotContains []string
}

// FilterLogEntry implements LogEntryFilter.
func (f LocationFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	if len(f.ContainsAny) > 0 {
		matched := false
		for _, str := range f.ContainsAny {
			if strings.Contains(entry.GetLocation(), str) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(f.ContainsAll) > 0 {
		for _, str := range f.ContainsAny {
			if !strings.Contains(entry.GetLocation(), str) {
				return false
			}
		}
	}
	if len(f.NotContains) > 0 {
		for _, str := range f.ContainsAny {
			if strings.Contains(entry.GetLocation(), str) {
				return false
			}
		}
	}
	return true
}

// SpanEventFilter filter logs by matching span events.
type SpanEventFilter struct {
	// Exclude excludes entries representing span events.
	Exclude bool
}

// FilterLogEntry implements LogEntryFilter.
func (f SpanEventFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	if f.Exclude && entry.GetTrace().GetEvent() != nil {
		return false
	}
	return true
}

// ExcludeSpanEvents returns a SpanEventFilter to exclude all span events.
func ExcludeSpanEvents() *SpanEventFilter {
	return &SpanEventFilter{Exclude: true}
}

// AttributeFilter implements LogEntryFilter.
type AttributeFilter struct {
	// Name is the attribute name.
	Name string

	Matcher func(*logspb.Value) bool
}

func (f AttributeFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	return f.Matcher(entry.GetAttributes()[f.Name])
}

func ParseAttributeFilter(str string) (*AttributeFilter, error) {
	matches := attrFilterRegexp.FindAllStringSubmatch(str, -1)
	if len(matches) != 1 || len(matches[0]) != 4 {
		return nil, fmt.Errorf("invalid attribute filter: %s", str)
	}
	f := &AttributeFilter{
		Name: matches[0][1],
	}
	op, val := matches[0][2], matches[0][3]
	if (op == "<" || op == ">") && strings.HasPrefix(val, "=") {
		op += "="
		val = val[1:]
	}
	switch op {
	case "=", "!=", "<", ">", "<=", ">=":
		f.Matcher = ordinalMatcher(val, op)
	case ":":
		f.Matcher = strMatcher(func(s string) bool { return strings.Contains(s, val) })
	case "~":
		re, err := regexp.Compile(val)
		if err != nil {
			return nil, fmt.Errorf("invalid regular expression %q: %w", val, err)
		}
		f.Matcher = strMatcher(func(s string) bool { return re.MatchString(s) })
	default:
		return nil, fmt.Errorf("invalid operator: %s", op)
	}
	return f, nil
}

type strValues struct {
	str  string
	bVal *bool
	fVal *float64
	uVal *uint64
	iVal *int64
}

func parseStrValues(str string) *strValues {
	v := &strValues{str: str}
	switch strings.ToLower(str) {
	case "true":
		bVal := true
		v.bVal = &bVal
		return v
	case "false":
		bVal := false
		v.bVal = &bVal
		return v
	}
	switch {
	case strings.HasPrefix(str, "0x") || strings.HasPrefix(str, "0X"):
		if uVal, err := strconv.ParseUint(str[2:], 16, 64); err == nil {
			v.uVal = &uVal
		}
	case strings.HasPrefix(str, "u") || strings.HasPrefix(str, "U"):
		if uVal, err := strconv.ParseUint(str[1:], 10, 64); err == nil {
			v.uVal = &uVal
		}
	default:
		if fVal, err := strconv.ParseFloat(str, 64); err == nil {
			v.fVal = &fVal
		}
		if iVal, err := strconv.ParseInt(str, 10, 64); err == nil {
			v.iVal = &iVal
		}
	}
	return v
}

func (v *strValues) floatCompare(val float64, op string) bool {
	switch {
	case v.fVal != nil:
		return ordinalCompare(val, *v.fVal, op)
	case v.iVal != nil:
		return ordinalCompare(val, float64(*v.iVal), op)
	case v.uVal != nil:
		return ordinalCompare(val, float64(*v.uVal), op)
	}
	return false
}

func (v *strValues) intCompare(val int64, op string) bool {
	switch {
	case v.iVal != nil:
		return ordinalCompare(val, *v.iVal, op)
	case v.uVal != nil:
		return ordinalCompare(uint64(val), *v.uVal, op)
	case v.fVal != nil:
		return ordinalCompare(float64(val), *v.fVal, op)
	}
	return false
}

func ordinalCompare[T string | float64 | uint64 | int64](v1, v2 T, op string) bool {
	switch op {
	case "<":
		return v1 < v2
	case "<=":
		return v1 <= v2
	case ">":
		return v1 > v2
	case ">=":
		return v1 >= v2
	case "=":
		return v1 == v2
	case "!=":
		return v1 != v2
	}
	return false
}

func ordinalMatcher(str, op string) func(*logspb.Value) bool {
	strVals := parseStrValues(str)
	equalCmp := op == "=" || op == "!="
	return func(v *logspb.Value) bool {
		if v == nil && equalCmp {
			return ordinalCompare("", str, op)
		}
		switch val := v.GetValue().(type) {
		case *logspb.Value_BoolValue:
			if strVals.bVal != nil {
				switch op {
				case "=":
					return val.BoolValue == *strVals.bVal
				case "!=":
					return val.BoolValue != *strVals.bVal
				}
			}
			return false
		case *logspb.Value_DoubleValue:
			return strVals.floatCompare(val.DoubleValue, op)
		case *logspb.Value_FloatValue:
			return strVals.floatCompare(float64(val.FloatValue), op)
		case *logspb.Value_IntValue:
			return strVals.intCompare(val.IntValue, op)
		case *logspb.Value_StrValue:
			return ordinalCompare(val.StrValue, str, op)
		}
		return false
	}
}

func strMatcher(fn func(string) bool) func(*logspb.Value) bool {
	return func(v *logspb.Value) bool {
		if v == nil {
			return fn("")
		}
		if strVal, ok := v.GetValue().(*logspb.Value_StrValue); ok {
			return fn(strVal.StrValue)
		}
		return false
	}
}

// MessageFilter filter logs by matching message content.
type MessageFilter struct {
	Contains string
}

// FilterLogEntry implements LogEntryFilter.
func (f MessageFilter) FilterLogEntry(entry *logspb.LogEntry) bool {
	return strings.Contains(entry.GetMessage(), f.Contains)
}

// MessageContains returns a MessageFilter to match substr in messages.
func MessageContains(substr string) *MessageFilter {
	return &MessageFilter{Contains: substr}
}
