package http

import (
	"net/http"

	"github.com/evo-cloud/logs/pkg/logs"
)

// TraceRoundTripper implements http.RoundTripper and logs the request/response.
type TraceRoundTripper struct {
	Next http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t *TraceRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx, log := logs.StartSpan(r.Context(), "RoundTrip", logs.HTTPRequest("http-request", r))
	defer log.EndSpan()
	resp, err := t.Next.RoundTrip(r.WithContext(ctx))
	if err != nil {
		log.Error(err).PrintErr("RoundTrip: ")
	} else {
		log.SetAttrs(logs.HTTPResponse("http-response", resp))
	}
	return resp, err
}
