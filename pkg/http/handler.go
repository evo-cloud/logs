package http

import (
	"net/http"

	logspb "github.com/evo-cloud/logs/pkg/gen/proto/logs"
	"github.com/evo-cloud/logs/pkg/logs"
)

// B3 HTTP headers.
const (
	B3TraceIDHeader = "X-B3-TraceId"
	B3SpanIDHeader  = "X-B3-SpanId"
)

// SpanInfoExtractor extracts SpanInfo from RPC.
type SpanInfoExtractor interface {
	ExtractSpanInfo(r *http.Request) logs.SpanInfo
}

// AttributesBuilder injects attributes into context.
type AttributesBuilder interface {
	BuildAttributes(r *http.Request) []*logs.Attribute
}

// AttributesBuilderFunc is the func form of AttributesBuilder.
type AttributesBuilderFunc func(r *http.Request) []*logs.Attribute

// BuildAttributes implements AttributesBuilder.
func (f AttributesBuilderFunc) BuildAttributes(r *http.Request) []*logs.Attribute {
	return f(r)
}

// B3Extractor extracts B3 span info.
type B3Extractor struct {
}

// Handler implements http.Handler to inject span into context.
type Handler struct {
	SpanInfoExtractor SpanInfoExtractor
	AttributesBuilder AttributesBuilder
	Next              http.Handler
}

// NewHandler creates a Handler.
func NewHandler(next http.Handler) *Handler {
	return &Handler{SpanInfoExtractor: &B3Extractor{}, Next: next}
}

// WithAttributesBuilder sets AttributesBuilder.
func (h *Handler) WithAttributesBuilder(b AttributesBuilder) *Handler {
	h.AttributesBuilder = b
	return h
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	spanInfo := h.SpanInfoExtractor.ExtractSpanInfo(r)
	spanInfo.Name = requestSpanName(r)
	var attrs []*logs.Attribute
	if b := h.AttributesBuilder; b != nil {
		attrs = b.BuildAttributes(r)
	}
	attrs = append(attrs, logs.HTTPRequest("http", r))
	ctx, span := logs.StartSpanWith(ctx, 0, spanInfo, attrs...)
	defer span.End()
	h.Next.ServeHTTP(w, r.WithContext(ctx))
}

// ExtractSpanInfo implements SpanInfoExtractor.
func (x *B3Extractor) ExtractSpanInfo(r *http.Request) logs.SpanInfo {
	info := logs.BuildSpanInfoFrom(r.Header.Get(B3TraceIDHeader), "", r.Header.Get(B3SpanIDHeader))
	info.Kind = logspb.Span_SERVER
	return info
}

func requestSpanName(r *http.Request) string {
	return r.URL.Path
}
