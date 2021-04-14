package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/evo-cloud/logs"
)

// SpanInfoExtractor extracts SpanInfo from RPC.
type SpanInfoExtractor interface {
	ExtractSpanInfo(metadata.MD, *stats.RPCTagInfo) logs.SpanInfo
}

// AttributesBuilder injects attributes into context.
type AttributesBuilder interface {
	BuildAttributes(ctx context.Context, md metadata.MD, info *stats.RPCTagInfo) []*logs.Attribute
}

// AttributesBuilderFunc is the func form of AttributesBuilder.
type AttributesBuilderFunc func(ctx context.Context, md metadata.MD, info *stats.RPCTagInfo) []*logs.Attribute

// BuildAttributes implements AttributesBuilder.
func (f AttributesBuilderFunc) BuildAttributes(ctx context.Context, md metadata.MD, info *stats.RPCTagInfo) []*logs.Attribute {
	return f(ctx, md, info)
}

// ServerStatsHandler implements a gRPC stats handler to inject span in the context.
type ServerStatsHandler struct {
	SpanInfoExtractor SpanInfoExtractor
	AttributesBuilder AttributesBuilder
}

// NewServerStatsHandler creates a ServerStatsHandler.
func NewServerStatsHandler() *ServerStatsHandler {
	return &ServerStatsHandler{SpanInfoExtractor: &B3{}}
}

// WithAttributesBuilder sets AttributesBuilder.
func (h *ServerStatsHandler) WithAttributesBuilder(b AttributesBuilder) *ServerStatsHandler {
	h.AttributesBuilder = b
	return h
}

// TagRPC implements stats.Handler.
func (h *ServerStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	md, _ := metadata.FromIncomingContext(ctx)
	spanInfo := h.SpanInfoExtractor.ExtractSpanInfo(md, info)
	var attrs []*logs.Attribute
	if b := h.AttributesBuilder; b != nil {
		attrs = b.BuildAttributes(ctx, md, info)
	}
	spanInfo.Name = rpcSpanName(info)
	ctx, _ = logs.StartSpanWith(ctx, 0, spanInfo, attrs...)
	return ctx
}

// HandleRPC implements stats.Handler.
func (h *ServerStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	logger := logs.Use(ctx)
	switch st := rs.(type) {
	case *stats.InPayload:
		if msg, ok := st.Payload.(proto.Message); ok {
			logger.With(logs.Str("dir", "I"), logs.ProtoJSON("payload", msg)).Printf("Incoming payload: %s", msg.ProtoReflect().Descriptor().FullName())
		}
	case *stats.OutPayload:
		if msg, ok := st.Payload.(proto.Message); ok {
			logger.With(logs.Str("dir", "O"), logs.ProtoJSON("payload", msg)).Printf("Outgoing payload: %s", msg.ProtoReflect().Descriptor().FullName())
		}
	case *stats.End:
		if s, ok := status.FromError(st.Error); ok && s.Code() != codes.OK {
			logger.SetAttrs(
				logs.Int("grpc.status_code", int64(s.Code())),
				logs.Str("grpc.status", s.Code().String()),
				logs.Str("grpc.status_error", s.Err().Error()),
				logs.Proto("grpc.status_proto", s.Proto()),
			)
		}
		logger.EndSpan()
	}
}

// TagConn implements stats.Handler.
func (h *ServerStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	// Do nothing.
	return ctx
}

// HandleConn implements stats.Handler.
func (h *ServerStatsHandler) HandleConn(context.Context, stats.ConnStats) {
	// Do nothing.
}
