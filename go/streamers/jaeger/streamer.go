package jaeger

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"time"

	jaegerpb "github.com/jaegertracing/jaeger-idl/model/v1"
	jaegerapi "github.com/jaegertracing/jaeger-idl/proto-gen/api_v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	logspb "github.com/evo-cloud/logs/go/gen/proto/logs"
	"github.com/evo-cloud/logs/go/logs"
)

// Reporter implements logs.ChunkedStreamer.
type Reporter struct {
	name      string
	conn      *grpc.ClientConn
	assembler logs.SpanAssembler
}

type batchStreamer struct {
	reporter   *Reporter
	lastNanoTS int64
	batch      jaegerpb.Batch
}

// New creates a Reporter with a Jaeger gRPC client.
func New(clientName, serverAddr string, tlsConf *tls.Config) (*Reporter, error) {
	var options []grpc.DialOption
	if tlsConf != nil {
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(tlsConf)))
	} else {
		options = append(options, grpc.WithInsecure())
	}
	conn, err := grpc.Dial(serverAddr, options...)
	if err != nil {
		return nil, err
	}
	return &Reporter{name: clientName, conn: conn}, nil
}

// StartStreamInChunk implements logs.ChunkedStreamer.
func (r *Reporter) StartStreamInChunk(ctx context.Context, info logs.ChunkInfo) (logs.ChunkedLogStreamer, error) {
	return &batchStreamer{
		reporter: r,
		batch: jaegerpb.Batch{
			Process: &jaegerpb.Process{
				ServiceName: r.name,
			},
		},
	}, nil
}

// StreamLogEntry implements logs.ChunkedLogStreamer.
func (s *batchStreamer) StreamLogEntry(ctx context.Context, entry *logspb.LogEntry) error {
	s.lastNanoTS = entry.NanoTs
	span := s.reporter.assembler.AddLogEntry(entry)
	if span != nil {
		tid, sid, err := parseIDs(span.GetContext())
		if err != nil {
			logs.Emergent().Error(err).PrintErr("Jaeger: invalid TraceID or SpanID: ")
			return nil
		}
		jspan := &jaegerpb.Span{
			TraceID:       tid,
			SpanID:        sid,
			OperationName: span.GetName(),
			StartTime:     time.Unix(0, span.StartNs),
			Duration:      time.Duration(span.Duration) * time.Nanosecond,
			Tags:          attrsToKVs(span.Attributes),
		}
		for _, link := range span.Links {
			ltid, lsid, err := parseIDs(link.GetSpanContext())
			if err != nil {
				continue
			}
			ref := jaegerpb.SpanRef{
				TraceID: ltid,
				SpanID:  lsid,
			}
			switch link.GetType() {
			case logspb.Link_CHILD_OF:
				ref.RefType = jaegerpb.SpanRefType_CHILD_OF
			case logspb.Link_FOLLOW:
				ref.RefType = jaegerpb.SpanRefType_FOLLOWS_FROM
			}
			jspan.References = append(jspan.References, ref)
		}
		for _, entry := range span.Logs {
			switch entry.GetTrace().GetEvent().(type) {
			case *logspb.Trace_SpanStart_, *logspb.Trace_SpanEnd_:
				continue
			}
			l := jaegerpb.Log{
				Timestamp: time.Unix(0, entry.NanoTs),
				Fields:    attrsToKVs(entry.Attributes),
			}
			if entry.Level != logspb.LogEntry_NONE {
				l.Fields = append(l.Fields, jaegerpb.KeyValue{
					Key:   "level",
					VType: jaegerpb.ValueType_STRING,
					VStr:  entry.Level.String(),
				})
			}
			if entry.Location != "" {
				l.Fields = append(l.Fields, jaegerpb.KeyValue{
					Key:   "source",
					VType: jaegerpb.ValueType_STRING,
					VStr:  entry.Location,
				})
			}
			l.Fields = append(l.Fields, jaegerpb.KeyValue{
				Key:   "message",
				VType: jaegerpb.ValueType_STRING,
				VStr:  entry.Message,
			})
			jspan.Logs = append(jspan.Logs, l)
		}
		s.batch.Spans = append(s.batch.Spans, jspan)
	}
	return nil
}

// StreamEnd implements logs.ChunkedLogStreamer.
func (s *batchStreamer) StreamEnd(ctx context.Context) (int64, error) {
	if len(s.batch.Spans) > 0 {
		client := jaegerapi.NewCollectorServiceClient(s.reporter.conn)
		if _, err := client.PostSpans(ctx, &jaegerapi.PostSpansRequest{Batch: s.batch}); err != nil {
			logs.Emergent().Error(err).PrintErr("Post: ")
		}
	}
	return s.lastNanoTS, nil
}

func parseIDs(ctx *logspb.SpanContext) (tid jaegerpb.TraceID, sid jaegerpb.SpanID, err error) {
	traceID, spanID := ctx.GetTraceId(), ctx.GetSpanId()
	if !logs.IsTraceIDValid(traceID) {
		err = fmt.Errorf("invalid trace ID")
		return
	}
	if spanID == 0 {
		err = fmt.Errorf("invalid span ID")
		return
	}
	tid.Low = binary.LittleEndian.Uint64(traceID[:8])
	tid.High = binary.LittleEndian.Uint64(traceID[8:])
	sid = jaegerpb.SpanID(spanID)
	return
}

func attrsToKVs(attrs map[string]*logspb.Value) []jaegerpb.KeyValue {
	kvs := make([]jaegerpb.KeyValue, 0, len(attrs))
	for key, attr := range attrs {
		kv := jaegerpb.KeyValue{Key: key}
		switch v := attr.GetValue().(type) {
		case *logspb.Value_BoolValue:
			kv.VType, kv.VBool = jaegerpb.ValueType_BOOL, v.BoolValue
		case *logspb.Value_IntValue:
			kv.VType, kv.VInt64 = jaegerpb.ValueType_INT64, v.IntValue
		case *logspb.Value_FloatValue:
			kv.VType, kv.VFloat64 = jaegerpb.ValueType_FLOAT64, float64(v.FloatValue)
		case *logspb.Value_DoubleValue:
			kv.VType, kv.VFloat64 = jaegerpb.ValueType_FLOAT64, v.DoubleValue
		case *logspb.Value_StrValue:
			kv.VType, kv.VStr = jaegerpb.ValueType_STRING, v.StrValue
		case *logspb.Value_Json:
			kv.VType, kv.VStr = jaegerpb.ValueType_STRING, v.Json
		case *logspb.Value_Proto:
			kv.VType, kv.VBinary = jaegerpb.ValueType_BINARY, v.Proto
		default:
			continue
		}
		kvs = append(kvs, kv)
	}
	return kvs
}
