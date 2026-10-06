package otlp

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	collogs "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	coltrace "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogs "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptrace "go.opentelemetry.io/proto/otlp/trace/v1"
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

	maxResponseBody = 4096
)

// Emitter implements logs.BatchEmitter and logs.ChunkedStreamer.
// It exports logs via the OpenTelemetry Protocol (OTLP) logs API as protobuf.
type Emitter struct {
	// The base endpoint URL.
	Endpoint string

	// The path to be appended to the Endpoint URL for logs.
	// If this is empty, log exporting is disabled.
	// If the path is already included in the Endpoint URL, set this to "/".
	LogsPath string

	// The path to be appended to the Endpoint URL for traces.
	// If this is empty, trace exporting is disabled.
	// If the path is already included in the Endpoint URL, set this to "/".
	TracePath string

	// The authentication for the endpoint.
	Token oauth2.TokenSource

	// The HTTP client for calling the endpoint.
	// If unspecified, http.DefaultClient will be used.
	Client *http.Client

	// Extra headers to be appended.
	ExtraHeaders http.Header

	// If specified, errors are logged.
	ErrorLogger *logs.Logger

	Verbose bool

	resource *otlpresource.Resource
}

// Option customizes an Emitter.
type Option func(*Emitter)

// Specify the OTLP endpoint URL (e.g. "http://localhost:4318" or the full API path).
func WithEndpoint(url string) Option {
	return func(e *Emitter) { e.Endpoint = url }
}

// Set the HTTP Authorization header.
func WithAuthToken(token, tokenType string) Option {
	tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token, TokenType: tokenType})
	return func(e *Emitter) { e.Token = tokenSource }
}

// Specify the HTTP client.
func WithClient(client *http.Client) Option {
	return func(e *Emitter) { e.Client = client }
}

// Add additional headers to every request.
func WithExtraHeaders(h http.Header) Option {
	return func(e *Emitter) {
		if h == nil {
			return
		}
		if e.ExtraHeaders == nil {
			e.ExtraHeaders = make(http.Header)
		}
		for k, v := range h {
			e.ExtraHeaders[k] = append(e.ExtraHeaders[k], v...)
		}
	}
}

// NewEmitter creates an Emitter.
func NewEmitter(clientName string, opts ...Option) *Emitter {
	e := &Emitter{
		Endpoint:  os.Getenv("OTLP_ENDPOINT"),
		LogsPath:  os.Getenv("OTLP_API_LOGS_PATH"),
		TracePath: os.Getenv("OTLP_API_TRACE_PATH"),
		resource: &otlpresource.Resource{
			Attributes: []*otlpcommon.KeyValue{
				{Key: "client", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: clientName}}},
			},
		},
	}
	switch e.LogsPath {
	case "":
		e.LogsPath = "/v1/logs"
	case "-":
		e.LogsPath = ""
	}
	switch e.TracePath {
	case "":
		e.TracePath = "/v1/traces"
	case "-":
		e.TracePath = ""
	}
	if os.Getenv("OTLP_LOG_ERROR") != "" {
		e.ErrorLogger = logs.Emergent()
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
	col := &collector{
		collectLogs:  e.LogsPath != "",
		collectTrace: e.TracePath != "",
	}
	col.collect(entries...)
	return col.flush(ctx, e)
}

func (e *Emitter) send(ctx context.Context, path string, msg proto.Message) error {
	if e.Endpoint == "" {
		return fmt.Errorf("otlp emitter: endpoint not set")
	}
	payload, err := proto.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode payload: %w", err)
	}
	fullURL := strings.TrimRight(e.Endpoint, "/") + "/" + strings.TrimLeft(path, "/")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, fullURL, bytes.NewReader(payload))
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
	for k, v := range e.ExtraHeaders {
		for _, vv := range v {
			req.Header.Add(k, vv)
		}
	}
	client := e.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return err
	}
	if e.Verbose {
		logs.Emergent().Infof("OTLP reply: %d %s", resp.StatusCode, string(body))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err := fmt.Errorf("otlp emitter: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		if l := e.ErrorLogger; l != nil {
			l.Error(err).PrintErr("OTLP send: ")
		}
		return err
	}
	return nil
}

type collector struct {
	collectLogs  bool
	collectTrace bool
	logs         []*otlplogs.LogRecord
	spans        []*otlptrace.Span
	lastTs       int64
}

func (c *collector) collect(entries ...*logspb.LogEntry) {
	for _, entry := range entries {
		tr := entry.GetTrace()
		if start := tr.GetSpanEnd().GetStart(); start != nil && c.collectTrace {
			span := &otlptrace.Span{
				TraceId:           tr.GetSpanContext().GetTraceId(),
				SpanId:            spanIDFrom(tr.GetSpanContext().GetSpanId()),
				Name:              start.GetName(),
				StartTimeUnixNano: uint64(tr.GetSpanEnd().GetStartNs()),
				EndTimeUnixNano:   uint64(entry.GetNanoTs()),
			}
			switch start.GetKind() {
			case logspb.Span_INTERNAL:
				span.Kind = otlptrace.Span_SPAN_KIND_INTERNAL
			case logspb.Span_SERVER:
				span.Kind = otlptrace.Span_SPAN_KIND_SERVER
			case logspb.Span_CLIENT:
				span.Kind = otlptrace.Span_SPAN_KIND_CLIENT
			case logspb.Span_PRODUCER:
				span.Kind = otlptrace.Span_SPAN_KIND_PRODUCER
			case logspb.Span_CONSUMER:
				span.Kind = otlptrace.Span_SPAN_KIND_CONSUMER
			}
			for _, link := range start.GetLinks() {
				if link.GetType() == logspb.Link_CHILD_OF {
					span.ParentSpanId = spanIDFrom(link.GetSpanContext().GetSpanId())
					continue
				}
				span.Links = append(span.Links, &otlptrace.Span_Link{
					TraceId:    link.GetSpanContext().GetTraceId(),
					SpanId:     spanIDFrom(link.GetSpanContext().GetSpanId()),
					Attributes: attributesFrom(link.GetAttributes()),
				})
			}
			c.spans = append(c.spans, span)
		}
		if c.collectLogs {
			sev, sevText := levelToSeverity(entry.GetLevel())
			rec := &otlplogs.LogRecord{
				TraceId:        tr.GetSpanContext().GetTraceId(),
				SpanId:         spanIDFrom(tr.GetSpanContext().GetSpanId()),
				TimeUnixNano:   uint64(entry.GetNanoTs()),
				Body:           &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: entry.GetMessage()}},
				SeverityNumber: sev,
				SeverityText:   sevText,
				Attributes:     attributesFrom(entry.GetAttributes()),
			}
			c.logs = append(c.logs, rec)
		}
		if lastTs := entry.GetNanoTs(); lastTs > c.lastTs {
			c.lastTs = lastTs
		}
	}
}

func (c *collector) flush(ctx context.Context, e *Emitter) error {
	var errs []error
	if len(c.logs) > 0 {
		req := &collogs.ExportLogsServiceRequest{
			ResourceLogs: []*otlplogs.ResourceLogs{
				{
					Resource:  e.resource,
					ScopeLogs: []*otlplogs.ScopeLogs{{LogRecords: c.logs}},
				},
			},
		}
		if err := e.send(ctx, e.LogsPath, req); err != nil {
			errs = append(errs, err)
		} else {
			c.logs = nil
		}
	}
	if len(c.spans) > 0 {
		req := &coltrace.ExportTraceServiceRequest{
			ResourceSpans: []*otlptrace.ResourceSpans{
				{
					Resource:   e.resource,
					ScopeSpans: []*otlptrace.ScopeSpans{{Spans: c.spans}},
				},
			},
		}
		if err := e.send(ctx, e.TracePath, req); err != nil {
			errs = append(errs, err)
		} else {
			c.spans = nil
		}
	}
	return errors.Join(errs...)
}

type stream struct {
	emitter   *Emitter
	collector *collector
}

func (s *stream) StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	s.collector.collect(entry)
	if len(s.collector.logs)+len(s.collector.spans) >= bulkThreshold {
		return s.collector.flush(ctx, s.emitter)
	}
	return nil
}

func (s *stream) StreamEnd(ctx context.Context) (int64, error) {
	if err := s.collector.flush(ctx, s.emitter); err != nil {
		return s.collector.lastTs, err
	}
	return s.collector.lastTs, nil
}

func spanIDFrom(id uint64) []byte {
	if id == 0 {
		return nil
	}
	out := make([]byte, 8)
	binary.Encode(out, binary.BigEndian, id)
	return out
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
	case logspb.LogEntry_CRITICAL:
		return otlplogs.SeverityNumber_SEVERITY_NUMBER_ERROR3, "CRITICAL"
	case logspb.LogEntry_FATAL:
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
