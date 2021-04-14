package stackdriver

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"github.com/golang/glog"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/evo-cloud/logs"
	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
)

var (
	// ErrUnknownProjectID indicate project ID is not provided and can't be determined.
	ErrUnknownProjectID = errors.New("unknown GCP project id")
)

// JSONPayload defines the schema of "jsonPayload" for Stackdriver.
type JSONPayload struct {
	Timestamp      Timestamp              `json:"timestamp"`
	Severity       string                 `json:"severity"`
	Message        string                 `json:"message"`
	Labels         map[string]interface{} `json:"logging.googleapis.com/labels,omitempty"`
	InsertID       string                 `json:"logging.googleapis.com/insertId,omitempty"`
	Operation      string                 `json:"logging.googleapis.com/operation,omitempty"`
	SourceLocation *SourceLocation        `json:"logging.googleapis.com/sourceLocation,omitempty"`
	TraceID        string                 `json:"logging.googleapis.com/trace,omitempty"`
	SpanID         string                 `json:"logging.googleapis.com/spanId,omitempty"`
	Raw            json.RawMessage        `json:"raw"`
}

// SourceLocation defines the Stackdriver source location.
type SourceLocation struct {
	File string `json:"file"`
	Line int    `json:"line"`
}

// Timestamp defines the timestamp of the log.
type Timestamp struct {
	Seconds int64 `json:"seconds"`
	Nanos   int64 `json:"nanos"`
}

// JSONEmitter is a console emitter printing logs in Stackdriver compatible JSON format.
type JSONEmitter struct {
	Out       io.Writer
	ProjectID string
}

// NewJSONEmitter creates a JSONEmitter.
func NewJSONEmitter(out io.Writer, projectID string) (*JSONEmitter, error) {
	if projectID == "" {
		if !metadata.OnGCE() {
			return nil, ErrUnknownProjectID
		}
		id, err := metadata.ProjectID()
		if err != nil {
			return nil, fmt.Errorf("determine GCP project id: %w", err)
		}
		projectID = id
	}
	return &JSONEmitter{Out: out, ProjectID: projectID}, nil
}

// EmitLogEntry implements LogEmitter.
func (e *JSONEmitter) EmitLogEntry(entry *logspb.LogEntry) {
	payload := &JSONPayload{
		Timestamp: timestampFromNanos(entry.GetNanoTs()),
		Severity:  severityFromLevel(entry.GetLevel()),
		Message:   entry.GetMessage(),
		Labels:    labelsFromAttributes(entry.GetAttributes()),
		Raw:       json.RawMessage(protojson.MarshalOptions{UseProtoNames: true}.Format(entry)),
	}
	loc := strings.SplitN(entry.GetLocation(), ":", 2)
	if len(loc) > 1 {
		if line, err := strconv.Atoi(loc[1]); err == nil {
			payload.SourceLocation = &SourceLocation{File: loc[0], Line: line}
		}
	}
	if spanCtx := entry.GetTrace().GetSpanContext(); spanCtx != nil {
		traceID, spanID := logs.TraceIDStringFrom(spanCtx), logs.SpanIDStringFrom(spanCtx)
		if traceID != "" {
			payload.TraceID = "projects/" + e.ProjectID + "/traces/" + traceID
		}
		payload.SpanID = spanID
	}
	out, err := json.Marshal(payload)
	if err != nil {
		glog.Errorf("Marshal LogEntry (nano_ts=%d) error: %v", entry.GetNanoTs(), err)
		return
	}
	fmt.Fprintln(e.Out, string(out))
}

var (
	severityMap = map[logspb.LogEntry_Level]string{
		logspb.LogEntry_INFO:     "NOTICE",
		logspb.LogEntry_WARNING:  "WARNING",
		logspb.LogEntry_ERROR:    "ERROR",
		logspb.LogEntry_CRITICAL: "CRITICAL",
		logspb.LogEntry_FATAL:    "EMERGENCY",
	}
)

func timestampFromNanos(nanos int64) (ts Timestamp) {
	ts.Seconds = nanos / 1e9
	ts.Nanos = nanos % 1e9
	return
}

func severityFromLevel(level logspb.LogEntry_Level) string {
	if s, ok := severityMap[level]; ok {
		return s
	}
	return "DEFAULT"
}

func labelsFromAttributes(attrs map[string]*logspb.Value) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	labels := make(map[string]interface{})
	for key, val := range attrs {
		switch v := val.GetValue().(type) {
		case *logspb.Value_BoolValue:
			labels[key] = v.BoolValue
		case *logspb.Value_IntValue:
			labels[key] = v.IntValue
		case *logspb.Value_FloatValue:
			labels[key] = v.FloatValue
		case *logspb.Value_DoubleValue:
			labels[key] = v.DoubleValue
		case *logspb.Value_StrValue:
			labels[key] = v.StrValue
		case *logspb.Value_Json:
			labels[key] = json.RawMessage(v.Json)
		case *logspb.Value_Proto:
			labels[key] = v.Proto
		default:
			continue
		}
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}
