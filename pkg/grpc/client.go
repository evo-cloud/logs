package grpc

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"

	"github.com/evo-cloud/logs"
)

// SpanInfoInjector injects SpanInfo into gRPC metadata.
type SpanInfoInjector interface {
	InjectSpanInfo(logs.SpanInfo, metadata.MD) metadata.MD
}

// ClientStatsHandler implements a gRPC stats handler to inject span as outgoing metadata.
type ClientStatsHandler struct {
	SpanInfoInjector SpanInfoInjector
}

// NewClientStatsHandler creates a ClientStatsHandler.
func NewClientStatsHandler() *ClientStatsHandler {
	return &ClientStatsHandler{SpanInfoInjector: &B3{}}
}

// TagRPC implements stats.Handler.
func (h *ClientStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	ctx, log := logs.StartSpan(ctx, rpcSpanName(info))
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		md = metadata.New(nil)
	} else {
		md = md.Copy()
	}
	md = h.SpanInfoInjector.InjectSpanInfo(log.SpanInfo(), md)
	return metadata.NewOutgoingContext(ctx, md)
}

// HandleRPC implements stats.Handler.
func (h *ClientStatsHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	if end, ok := rs.(*stats.End); ok {
		log := logs.Use(ctx)
		if s, ok := status.FromError(end.Error); ok && s.Code() != codes.OK {
			log.SetAttrs(
				logs.Int("grpc.status_code", int64(s.Code())),
				logs.Str("grpc.status", s.Code().String()),
				logs.Str("grpc.status_error", s.Err().Error()),
				logs.Proto("grpc.status_proto", s.Proto()),
			)
		}
		log.EndSpan()
	}
}

// TagConn implements stats.Handler.
func (h *ClientStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	// Do nothing.
	return ctx
}

// HandleConn implements stats.Handler.
func (h *ClientStatsHandler) HandleConn(context.Context, stats.ConnStats) {
	// Do nothing.
}
