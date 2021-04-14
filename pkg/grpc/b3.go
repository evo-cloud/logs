package grpc

import (
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
	"github.com/evo-cloud/logs/pkg/logs"
)

// B3 metadata keys for SpanInfo.
const (
	B3TraceIDKey = "x-b3-traceid"
	B3SpanIDKey  = "x-b3-spanid"
)

// B3 extracts B3 span info.
type B3 struct {
}

// ExtractSpanInfo implements SpanInfoExtractor.
func (x *B3) ExtractSpanInfo(md metadata.MD, _ *stats.RPCTagInfo) logs.SpanInfo {
	info := logs.BuildSpanInfoFrom(mdValue(md, B3TraceIDKey), "", mdValue(md, B3SpanIDKey))
	info.Kind = logspb.Span_SERVER
	return info
}

// InjectSpanInfo implements SpanInfoInjector.
func (x *B3) InjectSpanInfo(info logs.SpanInfo, md metadata.MD) metadata.MD {
	traceID, spanID := logs.TraceIDStringFrom(info.Context), logs.SpanIDStringFrom(info.Context)
	if traceID != "" {
		md.Append(B3TraceIDKey, traceID)
	}
	if spanID != "" {
		md.Append(B3SpanIDKey, spanID)
	}
	return md
}

func mdValue(md metadata.MD, key string) string {
	for _, val := range md[key] {
		if val != "" {
			return val
		}
	}
	return ""
}
