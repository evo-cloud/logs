package otlp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	cpb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogs "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
	"golang.org/x/oauth2"
	"google.golang.org/protobuf/proto"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

const (
	// bulkThreshold is the number of log records to accumulate before a flush.
	bulkThreshold = 32

	// contentTypeProto is the OpenTelemetry protobuf HTTP content type.
	contentTypeProto = "application/x-protobuf"
)

// Emitter implements logs.BatchEmitter and logs.ChunkedStreamer.
// It exports logs via the OpenTelemetry Protocol (OTLP) logs API as protobuf.
type Emitter struct {
	Endpoint   string
	Token      oauth2.TokenSource
	ClientName string
	Client     *http.Client
	Verbose    bool

	streamName   string
	extraHeaders http.Header
	traceAPI     bool
}

// Option customizes an Emitter.
type Option func(*Emitter)

// WithEndpoint overrides the OTLP endpoint URL (e.g. "http://localhost:4318" or the full API path).
func WithEndpoint(url string) Option {
	return func(e *Emitter) { e.Endpoint = url }
}

// WithToken sets the HTTP Authorization header.
func WithAuthToken(token, tokenType string) Option {
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token, TokenType: tokenType})
	return func(e *Emitter) { e.Token = tokenSource }
}

// WithStreamName sets the optional "stream-name" header (e.g. for OpenObserve).
func WithStreamName(name string) Option {
	return func(e *Emitter) { e.streamName = name }
}

// WithExtraHeaders adds additional headers to every request.
func WithExtraHeaders(h http.Header) Option {
	return func(e *Emitter) {
		if h == nil {
			return
		}
		if e.extraHeaders == nil {
			e.extraHeaders = make(http.Header)
		}
		for k, v := range h {
			e.extraHeaders[k] = append(e.extraHeaders[k], v...)
		}
	}
}

// NewEmitter creates an Emitter.
func NewEmitter(clientName string, opts ...Option) *Emitter {
	e := &Emitter{
		Endpoint:   os.Getenv("OTLP_LOGS_ENDPOINT"),
		ClientName: clientName,
		Client:     http.DefaultClient,
		traceAPI:   os.Getenv("OTLP_TRACE_API") != "",
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// The direct logs.LogEmitter implementation.
func (e *Emitter) EmitLogEntry(entry *logspb.LogEntry) {
	e.emitEntries(context.Background(), []*logspb.LogEntry{entry})
}

// EmitLogEntries implements logs.BatchEmitter.
func (e *Emitter) EmitLogEntries(ctx context.Context, entries []*logspb.LogEntry) error {
	return e.emitEntries(ctx, entries)
}

// StartStreamInChunk implements logs.ChunkedStreamer.
func (e *Emitter) StartStreamInChunk(ctx context.Context, info logs.ChunkInfo) (logs.ChunkedLogStreamer, error) {
	s := &stream{emitter: e}
	return s, nil
}

func (e *Emitter) emitEntries(ctx context.Context, entries []*logspb.LogEntry) error {
	var records []*otlplogs.LogRecord
	for _, entry := range entries {
		records = append(records, entryToLogRecord(entry))
	}
	req := &cpb.ExportLogsServiceRequest{
		ResourceLogs: []*otlplogs.ResourceLogs{
			{
				Resource:  &otlpresource.Resource{Attributes: e.resourceAttrs()},
				ScopeLogs: []*otlplogs.ScopeLogs{{LogRecords: records}},
			},
		},
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	return e.send(ctx, payload)
}

// resourceAttrs returns the resource-level attributes for the emitter.
func (e *Emitter) resourceAttrs() []*otlpcommon.KeyValue {
	var attrs []*otlpcommon.KeyValue
	if e.ClientName != "" {
		attrs = append(attrs, &otlpcommon.KeyValue{Key: "client", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: e.ClientName}}})
	}
	return attrs
}

func (e *Emitter) send(ctx context.Context, payload []byte) error {
	if e.Endpoint == "" {
		return fmt.Errorf("otlp emitter: endpoint not set")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentTypeProto)
	if t := e.Token; t != nil {
		token, err := t.Token()
		if err != nil {
			return err
		}
		token.SetAuthHeader(req)
	}
	if e.streamName != "" {
		req.Header.Set("stream-name", e.streamName)
	}
	for k, v := range e.extraHeaders {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	resp, err := e.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if e.traceAPI {
		logs.Emergent().Infof("OTLP logs reply: %d %s", resp.StatusCode, string(body))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("otlp emitter: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

type stream struct {
	emitter *Emitter
	records []*otlplogs.LogRecord
	// acked is the last nanoTS confirmed delivered to the receiver.
	acked int64
}

func (s *stream) StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	s.records = append(s.records, entryToLogRecord(entry))
	if len(s.records) >= bulkThreshold {
		return s.flush(ctx)
	}
	return nil
}

func (s *stream) StreamEnd(ctx context.Context) (int64, error) {
	if err := s.flush(ctx); err != nil {
		return s.acked, err
	}
	return s.acked, nil
}

func (s *stream) flush(ctx context.Context) error {
	if len(s.records) == 0 {
		return nil
	}
	records := s.records
	var lastTS int64
	for _, r := range records {
		if v := int64(r.GetTimeUnixNano()); v > lastTS {
			lastTS = v
		}
	}
	req := &cpb.ExportLogsServiceRequest{
		ResourceLogs: []*otlplogs.ResourceLogs{
			{
				Resource: &otlpresource.Resource{Attributes: s.emitter.resourceAttrs()},
				ScopeLogs: []*otlplogs.ScopeLogs{
					{LogRecords: records},
				},
			},
		},
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	// Keep the records buffered until the receiver acknowledges them so they
	// are not lost if the request fails.
	if err := s.emitter.send(ctx, payload); err != nil {
		if s.emitter.Verbose {
			logs.Emergent().Error(err).PrintErr("OTLP flush: ")
		}
		return err
	}
	s.records = nil
	if lastTS > s.acked {
		s.acked = lastTS
	}
	return nil
}

// entryToLogRecord converts a logs.LogEntry into an OTLP LogRecord.
func entryToLogRecord(entry *logspb.LogEntry) *otlplogs.LogRecord {
	sev, sevText := levelToSeverity(entry.GetLevel())
	rec := &otlplogs.LogRecord{
		TimeUnixNano:   uint64(entry.GetNanoTs()),
		Body:           &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: entry.GetMessage()}},
		SeverityNumber: sev,
		SeverityText:   sevText,
		Attributes:     attributesFrom(entry.GetAttributes()),
	}
	if tr := entry.GetTrace(); tr != nil {
		if tid := tr.GetSpanContext().GetTraceId(); len(tid) >= 16 {
			rec.TraceId = tid
		}
		if sid := tr.GetSpanContext().GetSpanId(); sid != 0 {
			rec.SpanId = make([]byte, 8)
			for i := 0; i < 8; i++ {
				rec.SpanId[i] = byte(sid >> (8 * (7 - i)))
			}
		}
	}
	return rec
}

// levelToSeverity maps a logs.Level to an OTLP SeverityNumber and severity text.
func levelToSeverity(l logspb.LogEntry_Level) (otlplogs.SeverityNumber, string) {
	switch l {
	case logspb.LogEntry_INFO:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO, "INFO"
	case logspb.LogEntry_WARNING:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_WARN, "WARN"
	case logspb.LogEntry_ERROR:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR, "ERROR"
	case logspb.LogEntry_CRITICAL, logspb.LogEntry_FATAL:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_FATAL, "FATAL"
	default:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED, ""
	}
}

// attributesFrom converts log attributes to OTLP KeyValues.
func attributesFrom(attrs map[string]*logspb.Value) []*otlpcommon.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*otlpcommon.KeyValue, 0, len(attrs))
	for k, v := range attrs {
		if v == nil {
			continue
		}
		switch val := v.GetValue().(type) {
		case *logspb.Value_BoolValue:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_BoolValue{BoolValue: val.BoolValue}}})
		case *logspb.Value_IntValue:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: val.IntValue}}})
		case *logspb.Value_FloatValue:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_DoubleValue{DoubleValue: float64(val.FloatValue)}}})
		case *logspb.Value_DoubleValue:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_DoubleValue{DoubleValue: val.DoubleValue}}})
		case *logspb.Value_StrValue:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: val.StrValue}}})
		case *logspb.Value_Json:
			out = append(out, &otlpcommon.KeyValue{Key: k, Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: val.Json}}})
		default:
			// Skip opaque / unsupported types (proto bytes, etc).
		}
	}
	return out
}
