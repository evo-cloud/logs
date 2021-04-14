package http

import (
	"context"
	"net/http"

	"github.com/evo-cloud/logs/pkg/logs"
)

// UpdateRequest injects span information into request header.
func UpdateRequest(r *http.Request) {
	UpdateHeader(r.Context(), r.Header)
}

// UpdateHeader updates HTTP header.
func UpdateHeader(ctx context.Context, header http.Header) {
	logger := logs.Use(ctx)
	spanInfo := logger.SpanInfo()
	if traceID := spanInfo.TraceID(); traceID != "" {
		header.Add(B3TraceIDHeader, traceID)
	}
	if spanID := spanInfo.SpanID(); spanID != "" {
		header.Add(B3SpanIDHeader, spanID)
	}
}
