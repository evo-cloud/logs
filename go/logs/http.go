package logs

import (
	"net/http"
	"strings"
)

// HTTPRequestAttrs extracts information from HTTP request as attributes.
type HTTPRequestAttrs struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

// HTTPResponseAttrs extracts information from HTTP response as attributes.
type HTTPResponseAttrs struct {
	Status     string            `json:"status"`
	StatusCode int               `json:"status-code"`
	Headers    map[string]string `json:"headers"`
}

// HTTPRequest creates an Attribute from an HTTP request.
func HTTPRequest(name string, r *http.Request) AttributeSetter {
	attrs := &HTTPRequestAttrs{Method: r.Method, Path: r.URL.Path, Headers: make(map[string]string)}
	attrs.Headers["Host"] = r.Host
	for name, vals := range r.Header {
		if strings.ToLower(name) == "authorization" {
			var schema string
			if len(vals) > 0 {
				schema = strings.SplitN(strings.TrimSpace(vals[0]), " ", 2)[0]
			}
			attrs.Headers[name] = schema + "***"
			continue
		}
		attrs.Headers[name] = strings.Join(vals, "; ")
	}
	return JSON(name, attrs)
}

// HTTPResponse creates an Attribute from an HTTP response.
func HTTPResponse(name string, r *http.Response) AttributeSetter {
	attrs := &HTTPResponseAttrs{Status: r.Status, StatusCode: r.StatusCode, Headers: make(map[string]string)}
	for name, vals := range r.Header {
		attrs.Headers[name] = strings.Join(vals, "; ")
	}
	return JSON(name, attrs)
}
