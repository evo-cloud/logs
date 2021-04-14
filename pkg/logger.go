package logs

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

var (
	contextKey = &Logger{}

	idgenLock sync.Mutex
	idgenRand = rand.New(rand.NewSource(time.Now().UnixNano()))
)

// LogEmitter defines the abstraction for emitting logs.
type LogEmitter interface {
	EmitLogEntry(*logspb.LogEntry)
}

// LogEmitterFunc is the func form of LogEmitter.
type LogEmitterFunc func(*logspb.LogEntry)

// EmitLogEntry implements LogEmitter.
func (f LogEmitterFunc) EmitLogEntry(entry *logspb.LogEntry) {
	f(entry)
}

// Logger is the API for emitting logs.
type Logger struct {
	emitter LogEmitter
	parent  *Logger
	span    *SpanInfo
	attrs   map[string]*logspb.Value
}

// LogPrinter prepares and prints a single log message.
type LogPrinter struct {
	logger *Logger
	entry  *logspb.LogEntry
	err    error
}

// SpanInfo provides detailed information of a span.
type SpanInfo struct {
	Name    string
	Kind    logspb.Span_Kind
	Context *logspb.SpanContext
	Parent  *logspb.Link
	Links   []*logspb.Link
}

// Attribute defines a single attribute.
type Attribute struct {
	Name  string
	Value *logspb.Value
}

// Use returns the logger associated with the context.
// The returned logger is mutable.
func Use(ctx context.Context) *Logger {
	logger, ok := ctx.Value(contextKey).(*Logger)
	if !ok {
		return Default()
	}
	return logger
}

// Span starts a new span from current context.
func Span(ctx context.Context, name string, attrs ...*Attribute) (context.Context, *Logger) {
	logger := Use(ctx).StartSpanDepth(1, SpanInfo{Name: name}, attrs...)
	return logger.NewContext(ctx), logger
}

// StartSpan is an alias of Span to be compatible with tracing API.
func StartSpan(ctx context.Context, name string, attrs ...*Attribute) (context.Context, *Logger) {
	logger := Use(ctx).StartSpanDepth(1, SpanInfo{Name: name}, attrs...)
	return logger.NewContext(ctx), logger
}

// StartSpanWith starts a span with detailed SpanInfo.
func StartSpanWith(ctx context.Context, depth int, info SpanInfo, attrs ...*Attribute) (context.Context, *Logger) {
	logger := Use(ctx).StartSpanDepth(depth+1, info, attrs...)
	return logger.NewContext(ctx), logger
}

// Bool creates a boolean attribute.
func Bool(name string, val bool) *Attribute {
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_BoolValue{BoolValue: val}}}
}

// True creates a boolean attribute with true value.
func True(name string) *Attribute {
	return Bool(name, true)
}

// False creates a boolean attribute with false value.
func False(name string) *Attribute {
	return Bool(name, false)
}

// Int creates an integer attribute.
func Int(name string, val int64) *Attribute {
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_IntValue{IntValue: val}}}
}

// Float creates a float32 attribute.
func Float(name string, val float32) *Attribute {
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_FloatValue{FloatValue: val}}}
}

// Double creates a float64 attribute.
func Double(name string, val float64) *Attribute {
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_DoubleValue{DoubleValue: val}}}
}

// Str creates a string attribute.
func Str(name, val string) *Attribute {
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_StrValue{StrValue: val}}}
}

// Proto creates an attribute with encoded proto.
func Proto(name string, msg proto.Message) *Attribute {
	encoded, err := proto.Marshal(msg)
	if err != nil {
		panic(err)
	}
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_Proto{Proto: encoded}}}
}

// ProtoJSON creates an attribute with proto formatted in JSON.
func ProtoJSON(name string, msg proto.Message) *Attribute {
	return &Attribute{
		Name: name,
		Value: &logspb.Value{
			Value: &logspb.Value_Json{
				Json: protojson.MarshalOptions{UseProtoNames: true}.Format(msg),
			},
		},
	}
}

// JSON creates an attribute with value in a JSON string.
func JSON(name string, val interface{}) *Attribute {
	encoded, err := json.Marshal(val)
	if err != nil {
		panic(err)
	}
	return &Attribute{Name: name, Value: &logspb.Value{Value: &logspb.Value_Json{Json: string(encoded)}}}
}

// NewTraceID returns a new trace ID.
func NewTraceID() []byte {
	idgenLock.Lock()
	lo := uint64(idgenRand.Int63())
	hi := uint64(idgenRand.Int63())
	idgenLock.Unlock()
	buf := make([]byte, 16)
	binary.LittleEndian.PutUint64(buf[:8], lo)
	binary.LittleEndian.PutUint64(buf[8:], hi)
	return buf
}

// NewSpanID returns a time based span ID.
func NewSpanID() uint64 {
	return uint64(time.Now().UnixNano())
}

// IsTraceIDValid determines if a trace ID is valid.
func IsTraceIDValid(id []byte) bool {
	return len(id) == 16
}

// CopyTraceID copies a trace ID.
func CopyTraceID(id []byte) []byte {
	id1 := make([]byte, len(id))
	copy(id1, id)
	return id1
}

// ParseTraceID parses a string encoded trace ID.
func ParseTraceID(str string) ([]byte, error) {
	res, err := hex.DecodeString(str)
	if err != nil {
		return nil, err
	}
	if !IsTraceIDValid(res) {
		return nil, fmt.Errorf("invalid trace ID: %s", str)
	}
	// Swap the bytes to be little-endian.
	for i := 0; i < 8; i++ {
		res[i], res[15-i] = res[15-i], res[i]
	}
	return res, nil
}

// ParseSpanID parses a string encoded span ID.
func ParseSpanID(str string) (uint64, error) {
	return strconv.ParseUint(str, 16, 64)
}

// TraceIDStringFrom returns the string encoded TraceID from SpanContext.
func TraceIDStringFrom(ctx *logspb.SpanContext) string {
	id := ctx.GetTraceId()
	if !IsTraceIDValid(id) {
		return ""
	}
	id = CopyTraceID(id)
	// Swap the bytes for encoding.
	for i := 0; i < 8; i++ {
		id[i], id[15-i] = id[15-i], id[i]
	}
	return hex.EncodeToString(id)
}

// SpanIDStringFrom returns the string encoded SpanID from SpanContext.
func SpanIDStringFrom(ctx *logspb.SpanContext) string {
	val := ctx.GetSpanId()
	if val == 0 {
		return ""
	}
	return fmt.Sprintf("%016x", val)
}

// IDStringFrom returns the string encoded traceID/spanID from SpanContext.
func IDStringFrom(ctx *logspb.SpanContext) string {
	tid, sid := TraceIDStringFrom(ctx), SpanIDStringFrom(ctx)
	if tid == "" || sid == "" {
		return ""
	}
	return tid + "/" + sid
}

// BuildSpanInfoFrom string ids.
func BuildSpanInfoFrom(traceID, spanID, parentSpanID string) (info SpanInfo) {
	if traceID == "" {
		return
	}
	ctx := &logspb.SpanContext{}
	var err error
	if ctx.TraceId, err = ParseTraceID(traceID); err != nil {
		return
	}
	if spanID != "" {
		if ctx.SpanId, err = ParseSpanID(spanID); err != nil {
			return
		}
	}
	if parentSpanID != "" {
		spanID, err := ParseSpanID(parentSpanID)
		if err != nil {
			return
		}
		info.Parent = &logspb.Link{
			SpanContext: &logspb.SpanContext{
				TraceId: ctx.GetTraceId(),
				SpanId:  spanID,
			},
			Type: logspb.Link_CHILD_OF,
		}
	}
	info.Context = ctx
	return
}

// TraceID returns the string encoded TraceID.
func (s *SpanInfo) TraceID() string {
	return TraceIDStringFrom(s.Context)
}

// SpanID returns the string encoded SpanID.
func (s *SpanInfo) SpanID() string {
	return SpanIDStringFrom(s.Context)
}

// String returns the string representation of the span.
func (s *SpanInfo) String() string {
	return s.Name + "[" + IDStringFrom(s.Context) + "]"
}

// AllLinks returns combined links including parent link.
func (s *SpanInfo) AllLinks() []*logspb.Link {
	links := make([]*logspb.Link, 0, len(s.Links)+1)
	if s.Parent != nil {
		links = append(links, s.Parent)
	}
	return append(links, s.Links...)
}

// SpanInfo returns the current span information.
func (l *Logger) SpanInfo() SpanInfo {
	if l.span != nil {
		return *l.span
	}
	return SpanInfo{}
}

// New creates a child logger.
func (l *Logger) New(attrs ...*Attribute) *Logger {
	c := &Logger{
		emitter: l.emitter,
		parent:  l,
		span:    l.span,
		attrs:   make(map[string]*logspb.Value),
	}
	for k, v := range l.attrs {
		c.attrs[k] = v
	}
	return c.SetAttrs(attrs...)
}

// SetAttrs adds attributes into the current logger.
func (l *Logger) SetAttrs(attrs ...*Attribute) *Logger {
	for _, attr := range attrs {
		l.attrs[attr.Name] = attr.Value
	}
	return l
}

// StartSpanDepth creates a logger for a new span with specified call stack depth.
func (l *Logger) StartSpanDepth(depth int, info SpanInfo, attrs ...*Attribute) *Logger {
	c := l.New(attrs...)
	c.span = &SpanInfo{
		Name:    info.Name,
		Kind:    info.Kind,
		Context: info.Context,
		Parent:  info.Parent,
		Links:   info.Links,
	}
	if c.span.Parent == nil && l.span != nil {
		c.span.Parent = &logspb.Link{
			SpanContext: proto.Clone(l.span.Context).(*logspb.SpanContext),
			Type:        logspb.Link_CHILD_OF,
		}
	}
	if c.span.Context == nil {
		c.span.Context = &logspb.SpanContext{}
	}
	if !IsTraceIDValid(c.span.Context.GetTraceId()) {
		if c.span.Parent != nil {
			c.span.Context.TraceId = CopyTraceID(c.span.Parent.GetSpanContext().GetTraceId())
		} else {
			c.span.Context.TraceId = NewTraceID()
		}
	}
	if c.span.Context.GetSpanId() == 0 {
		c.span.Context.SpanId = NewSpanID()
	}
	entry := c.makeEntry(depth + 1)
	entry.Trace.Event = &logspb.Trace_SpanStart_{
		SpanStart: &logspb.Trace_SpanStart{
			Name:  c.span.Name,
			Kind:  c.span.Kind,
			Links: c.span.AllLinks(),
		},
	}
	entry.Message = fmt.Sprintf("SPAN_START %s", c.span)
	c.emit(entry)
	return c
}

// EndSpanDepth ends a span and returns the parent logger.
func (l *Logger) EndSpanDepth(depth int) *Logger {
	if l.span == nil {
		return l
	}
	entry := l.makeEntry(depth + 1)
	entry.Trace.Event = &logspb.Trace_SpanEnd_{
		SpanEnd: &logspb.Trace_SpanEnd{},
	}
	entry.Message = fmt.Sprintf("SPAN_END %s", l.span)
	l.emit(entry)
	if l.parent == nil {
		return Default()
	}
	return l.parent
}

// StartSpan starts a new span.
func (l *Logger) StartSpan(info SpanInfo, attrs ...*Attribute) *Logger {
	return l.StartSpanDepth(1, info, attrs...)
}

// EndSpan ends a span and returns the parent logger.
func (l *Logger) EndSpan() *Logger {
	return l.EndSpanDepth(1)
}

// End is an alias of EndSpan to be compatible with standard Span in tracing API.
func (l *Logger) End() {
	l.EndSpanDepth(1)
}

// NewContext creates a context with current logger.
func (l *Logger) NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, contextKey, l)
}

// Printer starts printing a log.
func (l *Logger) Printer(depth int) *LogPrinter {
	return &LogPrinter{logger: l, entry: l.makeEntry(depth + 1)}
}

// With is a shortcut.
func (l *Logger) With(attrs ...*Attribute) *LogPrinter {
	return l.Printer(1).With(attrs...)
}

// Info is a shortcut.
func (l *Logger) Info() *LogPrinter {
	return l.Printer(1).Info()
}

// Warning is a shortcut.
func (l *Logger) Warning(err error) *LogPrinter {
	return l.Printer(1).Warning(err)
}

// Error is a shortcut.
func (l *Logger) Error(err error) *LogPrinter {
	return l.Printer(1).Error(err)
}

// Critical is a shortcut.
func (l *Logger) Critical(err error) *LogPrinter {
	return l.Printer(1).Critical(err)
}

// Fatal is a shortcut.
func (l *Logger) Fatal(err error) *LogPrinter {
	return l.Printer(1).Fatal(err)
}

// Print is a shortcut.
func (l *Logger) Print(message string) {
	l.Printer(1).Print(message)
}

// Printf is a shortcut.
func (l *Logger) Printf(format string, args ...interface{}) {
	l.Printer(1).Printf(format, args...)
}

// Infof is a shortcut.
func (l *Logger) Infof(format string, args ...interface{}) {
	l.Printer(1).Infof(format, args...)
}

// Warningf is a shortcut.
func (l *Logger) Warningf(format string, args ...interface{}) error {
	return l.Printer(1).Warningf(format, args...)
}

// Errorf is a shortcut.
func (l *Logger) Errorf(format string, args ...interface{}) error {
	return l.Printer(1).Errorf(format, args...)
}

// Criticalf is a shortcut.
func (l *Logger) Criticalf(format string, args ...interface{}) error {
	return l.Printer(1).Criticalf(format, args...)
}

// Fatalf is a shortcut.
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.Printer(1).Fatalf(format, args...)
}

// PrintProtoCompact is a shortcut.
func (l *Logger) PrintProtoCompact(prefix string, msg proto.Message) {
	l.Printer(1).PrintProtoCompact(prefix, msg)
}

// PrintProto is a shortcut.
func (l *Logger) PrintProto(prefix string, msg proto.Message) {
	l.Printer(1).PrintProto(prefix, msg)
}

// PrintJSONCompact is a shortcut.
func (l *Logger) PrintJSONCompact(prefix string, obj interface{}) {
	l.Printer(1).PrintJSONCompact(prefix, obj)
}

// PrintJSON is a shortcut.
func (l *Logger) PrintJSON(prefix string, obj interface{}) {
	l.Printer(1).PrintJSON(prefix, obj)
}

// PrintProtoJSONCompact is a shortcut.
func (l *Logger) PrintProtoJSONCompact(prefix string, msg proto.Message) {
	l.Printer(1).PrintProtoJSONCompact(prefix, msg)
}

// PrintProtoJSON is a shortcut.
func (l *Logger) PrintProtoJSON(prefix string, msg proto.Message) {
	l.Printer(1).PrintProtoJSON(prefix, msg)
}

// EmitLogEntry implements LogEmitter and simply passthrough the log entry to the current emitter.
func (l *Logger) EmitLogEntry(entry *logspb.LogEntry) {
	l.emitter.EmitLogEntry(entry)
}

func (l *Logger) makeEntry(depth int) *logspb.LogEntry {
	entry := &logspb.LogEntry{
		NanoTs:     time.Now().UnixNano(),
		Attributes: make(map[string]*logspb.Value),
	}
	if l.span != nil {
		entry.Trace = &logspb.Trace{SpanContext: l.span.Context}
	}
	if _, fn, line, ok := runtime.Caller(depth + 1); ok {
		entry.Location = fn + ":" + strconv.Itoa(line)
	}
	for k, v := range l.attrs {
		entry.Attributes[k] = v
	}
	return entry
}

func (l *Logger) emit(entry *logspb.LogEntry) {
	l.emitter.EmitLogEntry(entry)
	if entry.Level == logspb.LogEntry_FATAL {
		os.Exit(1)
	}
}

// With sets attributes.
func (p *LogPrinter) With(attrs ...*Attribute) *LogPrinter {
	for _, attr := range attrs {
		p.entry.Attributes[attr.Name] = attr.Value
	}
	return p
}

// Info sets info level.
func (p *LogPrinter) Info() *LogPrinter {
	p.entry.Level = logspb.LogEntry_INFO
	return p
}

// Warning set warning level.
func (p *LogPrinter) Warning(err error) *LogPrinter {
	p.setError(logspb.LogEntry_WARNING, err)
	return p
}

// Error sets error level.
func (p *LogPrinter) Error(err error) *LogPrinter {
	p.setError(logspb.LogEntry_ERROR, err)
	return p
}

// Critical sets critical level.
func (p *LogPrinter) Critical(err error) *LogPrinter {
	p.setError(logspb.LogEntry_CRITICAL, err)
	return p
}

// Fatal sets fatal level.
func (p *LogPrinter) Fatal(err error) *LogPrinter {
	p.setError(logspb.LogEntry_FATAL, err)
	return p
}

// Print prints a message.
func (p *LogPrinter) Print(message string) {
	p.entry.Message = message
	p.logger.emit(p.entry)
}

// Printf formats a message and print.
func (p *LogPrinter) Printf(format string, args ...interface{}) {
	p.entry.Message = fmt.Sprintf(format, args...)
	p.logger.emit(p.entry)
}

// Infof sets info level, formats and prints the message.
func (p *LogPrinter) Infof(format string, args ...interface{}) {
	p.Info().Printf(format, args...)
}

// Warningf formats an error, calls Warn(err).Print(err.Error()) and returns the error.
func (p *LogPrinter) Warningf(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	p.Warning(err).Print(err.Error())
	return err
}

// Errorf formats an error, calls Error(err).Print(err.Error()) and returns the error.
func (p *LogPrinter) Errorf(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	p.Error(err).Print(err.Error())
	return err
}

// Criticalf is similar to Errorf but sets level to critical.
func (p *LogPrinter) Criticalf(format string, args ...interface{}) error {
	err := fmt.Errorf(format, args...)
	p.Critical(err).Print(err.Error())
	return err
}

// Fatalf is similar to Errorf but sets level to fatal.
func (p *LogPrinter) Fatalf(format string, args ...interface{}) {
	err := fmt.Errorf(format, args...)
	p.Fatal(err).Print(err.Error())
}

// PrintProtoCompact prints a single line text proto.
func (p *LogPrinter) PrintProtoCompact(prefix string, msg proto.Message) {
	p.Print(prefix + prototext.MarshalOptions{Multiline: false}.Format(msg))
}

// PrintProto prints a multiline text proto.
func (p *LogPrinter) PrintProto(prefix string, msg proto.Message) {
	if prefix != "" {
		prefix += "\n"
	}
	p.Print(prefix + prototext.MarshalOptions{Multiline: true}.Format(msg))
}

// PrintJSONCompact prints a single line JSON.
func (p *LogPrinter) PrintJSONCompact(prefix string, obj interface{}) {
	encoded, err := json.Marshal(obj)
	if err != nil {
		p.Print(prefix + err.Error())
		return
	}
	p.Print(prefix + string(encoded))
}

// PrintJSON prints a multiline JSON.
func (p *LogPrinter) PrintJSON(prefix string, obj interface{}) {
	encoded, err := json.MarshalIndent(obj, "", "  ")
	if err != nil {
		p.Print(prefix + err.Error())
		return
	}
	if prefix != "" {
		prefix += "\n"
	}
	p.Print(prefix + string(encoded))
}

// PrintProtoJSONCompact prints a single line proto in JSON.
func (p *LogPrinter) PrintProtoJSONCompact(prefix string, msg proto.Message) {
	p.Print(prefix + protojson.MarshalOptions{Multiline: false, UseProtoNames: true}.Format(msg))
}

// PrintProtoJSON prints a multiline proto in JSON.
func (p *LogPrinter) PrintProtoJSON(prefix string, msg proto.Message) {
	if prefix != "" {
		prefix += "\n"
	}
	p.Print(prefix + protojson.MarshalOptions{Multiline: true, UseProtoNames: true}.Format(msg))
}

// PrintErr prints the error specified by Warning/Error/Critical/Fatal and returns the error.
// It doesn't print and returns nil if error is not set.
func (p *LogPrinter) PrintErr(prefix string) error {
	if p.err == nil {
		return nil
	}
	p.Print(prefix + p.err.Error())
	return p.err
}

// PrintErrf is similar to PrintErr with prefix formatted.
func (p *LogPrinter) PrintErrf(prefixFormat string, args ...interface{}) error {
	if p.err == nil {
		return nil
	}
	p.Print(fmt.Sprintf(prefixFormat, args...) + p.err.Error())
	return p.err
}

func (p *LogPrinter) setError(level logspb.LogEntry_Level, err error) {
	p.entry.Level = level
	if err != nil {
		p.With(Str("error", err.Error()))
		p.err = err
	}
}
