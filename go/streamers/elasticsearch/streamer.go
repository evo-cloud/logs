package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/golang/glog"
	"google.golang.org/protobuf/encoding/protojson"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

const (
	bulkThreshold = 32
)

// Streamer streams logs to remote server.
type Streamer struct {
	ClientName string
	DataStream string
	ServerURL  string
	Client     *http.Client

	traceAPI bool
}

// NewStreamer creates a Streamer.
func NewStreamer(clientName, dataStream, serverURL string) *Streamer {
	return &Streamer{
		ClientName: clientName,
		DataStream: dataStream,
		ServerURL:  serverURL,
		Client:     http.DefaultClient,
		traceAPI:   os.Getenv("ES_TRACE_API") != "",
	}
}

// Close closes the underlying gRPC connection.
func (s *Streamer) Close() error {
	return nil
}

// StreamLogEntries implements logs.LogStreamer.
func (s *Streamer) StreamLogEntries(ctx context.Context, entries []*logspb.LogEntry) error {
	payload := &bytes.Buffer{}
	encoder := json.NewEncoder(payload)
	for _, entry := range entries {
		payload.WriteString(`{"create":{}}` + "\n")
		rec := entryToRecord(entry)
		rec.Client = s.ClientName
		if err := encoder.Encode(rec); err != nil {
			return err
		}
	}
	if s.traceAPI {
		str := payload.String()
		glog.Infof("ES bulk:\n%s", str)
		payload = bytes.NewBufferString(str)
	}
	return s.bulk(payload)
}

// StartStreamInChunk implements ChunkedStreamer.
func (s *Streamer) StartStreamInChunk(ctx context.Context, info logs.ChunkInfo) (logs.ChunkedLogStreamer, error) {
	st := &stream{streamer: s, info: info}
	st.encoder = json.NewEncoder(&st.payload)
	return st, nil
}

// BulkReply defines the reply of bulk call.
type BulkReply struct {
	Items  []*BulkReplyItem `json:"items"`
	Errors bool             `json:"errors"`
	Took   int              `json:"took"`
}

// BulkReplyItem defines a single reply item of bulk call.
type BulkReplyItem struct {
	Create *CreateReply `json:"create"`
}

// CreateReply defines the single create result in BulkReplyItem.
type CreateReply struct {
	Status int        `json:"status"`
	Error  *BulkError `json:"error,omitempty"`
}

// BulkError defines the error message.
type BulkError struct {
	Type   string `json:"type"`
	Reason string `json:"reason"`
}

func (s *Streamer) bulk(payload io.Reader) error {
	req, err := http.NewRequest(http.MethodPut, s.ServerURL+"/"+s.DataStream+"/_bulk", payload)
	if err != nil {
		return err
	}
	req.Header.Add("Content-type", "application/x-ndjson")
	resp, err := s.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var replyJSON string
	var reply BulkReply
	if data, err := ioutil.ReadAll(resp.Body); err == nil {
		replyJSON = string(data)
		json.Unmarshal(data, &reply)
	}
	if s.traceAPI {
		glog.Infof("ES bulk reply:\n%s", string(replyJSON))
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("bulk error: %d %s", resp.StatusCode, replyJSON)
	}
	if reply.Errors {
		var msgs []string
		for n, item := range reply.Items {
			if item.Create == nil || item.Create.Error == nil {
				continue
			}
			msgs = append(msgs, fmt.Sprintf("[%d] %s: %s", n, item.Create.Error.Type, item.Create.Error.Reason))
		}
		if len(msgs) > 0 {
			return fmt.Errorf("errors: %s", strings.Join(msgs, "\n"))
		}
	}
	return nil
}

type stream struct {
	streamer          *Streamer
	info              logs.ChunkInfo
	payload           bytes.Buffer
	encoder           *json.Encoder
	entryCount        int
	lastNanoTSEncoded int64
	lastNanoTS        int64
}

func (s *stream) StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	s.payload.WriteString(`{"create":{}}` + "\n")
	rec := entryToRecord(entry)
	rec.Client = s.streamer.ClientName
	if err := s.encoder.Encode(rec); err != nil {
		return err
	}
	if ts := entry.GetNanoTs(); ts > s.lastNanoTSEncoded {
		s.lastNanoTSEncoded = ts
	}
	s.entryCount++
	if s.entryCount >= bulkThreshold {
		s.flush()
	}
	return nil
}

func (s *stream) StreamEnd(ctx context.Context) (int64, error) {
	s.flush()
	return s.lastNanoTS, nil
}

func (s *stream) flush() {
	s.entryCount = 0
	encodedLastNanoTS := s.lastNanoTSEncoded
	s.lastNanoTSEncoded = 0
	err := s.streamer.bulk(&s.payload)
	s.payload.Reset()
	if err != nil {
		glog.Errorf("ElasticSearch bulk: %v", err)
	} else if encodedLastNanoTS > s.lastNanoTS {
		s.lastNanoTS = encodedLastNanoTS
	}
}

type record struct {
	Timestamp string                 `json:"@timestamp"`
	TS        *timestamp             `json:"ts"`
	Client    string                 `json:"client"`
	Level     string                 `json:"level,omitempty"`
	Message   string                 `json:"message"`
	Location  string                 `json:"location,omitempty"`
	Attrs     map[string]interface{} `json:"attrs,omitempty"`
	Trace     *traceContext          `json:"trace,omitempty"`
	LogJSON   string                 `json:"log.json"`
}

type traceContext struct {
	TraceID string `json:"id"`
	SpanID  string `json:"span"`
	Name    string `json:"name,omitempty"`
	Event   string `json:"event,omitempty"`
}

type timestamp struct {
	Nanos   int64 `json:"ns"`
	Seconds int64 `json:"s"`
}

func entryToRecord(entry *logspb.LogEntry) *record {
	r := &record{
		Timestamp: time.Unix(0, entry.GetNanoTs()).UTC().Format("2006-01-02T15:04:05.999999999Z"),
		TS:        &timestamp{Nanos: entry.GetNanoTs() % 1e9, Seconds: entry.GetNanoTs() / 1e9},
		Message:   entry.GetMessage(),
		Location:  entry.GetLocation(),
		LogJSON:   protojson.MarshalOptions{UseProtoNames: true}.Format(entry),
	}
	if attrs := entry.GetAttributes(); len(attrs) > 0 {
		r.Attrs = make(map[string]interface{})
		for key, val := range attrs {
			switch v := val.GetValue().(type) {
			case *logspb.Value_BoolValue:
				r.Attrs[key] = v.BoolValue
			case *logspb.Value_IntValue:
				r.Attrs[key] = v.IntValue
			case *logspb.Value_FloatValue:
				r.Attrs[key] = v.FloatValue
			case *logspb.Value_DoubleValue:
				r.Attrs[key] = v.DoubleValue
			case *logspb.Value_StrValue:
				r.Attrs[key] = v.StrValue
			case *logspb.Value_Json:
				r.Attrs[key] = v.Json
			case *logspb.Value_Proto:
				r.Attrs[key] = v.Proto
			default:
				continue
			}
		}
	}
	if level := entry.GetLevel(); level != logspb.LogEntry_NONE {
		r.Level = level.String()
	}
	if tr := entry.GetTrace(); tr != nil {
		r.Trace = &traceContext{
			TraceID: logs.TraceIDStringFrom(tr.GetSpanContext()),
			SpanID:  logs.SpanIDStringFrom(tr.GetSpanContext()),
		}
		if ev := tr.GetSpanStart(); ev != nil {
			r.Trace.Name = ev.GetName()
			r.Trace.Event = "span-start"
		}
		if ev := tr.GetSpanEnd(); ev != nil {
			r.Trace.Event = "span-end"
		}
	}
	return r
}
