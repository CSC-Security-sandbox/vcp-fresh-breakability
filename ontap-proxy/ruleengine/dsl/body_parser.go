package dsl

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
)

// MaxRequestBodySize is the maximum allowed request body size (5MB)
const MaxRequestBodySize = 5 << 20 // 5MB

// parsedBodyKey is the context key for storing parsed request body
type parsedBodyKey struct{}

// ParsedBody holds the parsed request body and any parse error
type ParsedBody struct {
	Data     map[string]interface{}
	RawBody  []byte
	ParseErr string
}

// ParseRequestBody reads, parses, and stores the request body in context.
// This should be called once at the middleware level before any validation.
// Returns the modified request with parsed body in context.
func ParseRequestBody(r *http.Request) *http.Request {
	if r == nil {
		return r
	}

	// Skip if already parsed
	if _, ok := r.Context().Value(parsedBodyKey{}).(*ParsedBody); ok {
		return r
	}

	// Skip for methods that typically don't have bodies
	if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodDelete {
		return r
	}

	parsed := &ParsedBody{}

	if r.Body == nil {
		parsed.ParseErr = "request body is required"
		ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
		return r.WithContext(ctx)
	}

	// Read body with size limit
	limitedReader := io.LimitReader(r.Body, MaxRequestBodySize+1)
	body, err := io.ReadAll(limitedReader)
	if err != nil {
		parsed.ParseErr = "failed to read request body"
		ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
		return r.WithContext(ctx)
	}

	// Check if body exceeds limit
	if len(body) > MaxRequestBodySize {
		parsed.ParseErr = "request body too large (max 5MB)"
		ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
		return r.WithContext(ctx)
	}

	// Restore body for downstream handlers
	r.Body = io.NopCloser(bytes.NewReader(body))
	parsed.RawBody = body

	if len(body) == 0 {
		parsed.ParseErr = "request body is empty"
		ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
		return r.WithContext(ctx)
	}

	// Parse JSON
	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		parsed.ParseErr = "invalid JSON in request body"
		ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
		return r.WithContext(ctx)
	}

	parsed.Data = data
	ctx := context.WithValue(r.Context(), parsedBodyKey{}, parsed)
	return r.WithContext(ctx)
}

// GetParsedBody retrieves the parsed body from context.
// Returns:
//   - (data, "") if parsing succeeded
//   - (nil, "error") if there was a parse error
//   - Falls back to parsing if not in context (backwards compatibility)
func GetParsedBody(r *http.Request) (map[string]interface{}, string) {
	if r == nil {
		return nil, "request is nil"
	}

	// Try to get from context
	if parsed, ok := r.Context().Value(parsedBodyKey{}).(*ParsedBody); ok {
		if parsed.ParseErr != "" {
			return nil, parsed.ParseErr
		}
		return parsed.Data, ""
	}

	// Fallback: parse on demand (for backwards compatibility or if not pre-parsed)
	return parseBodyDirect(r)
}

// parseBodyDirect parses the body directly without using context cache.
// Used as fallback when body wasn't pre-parsed.
func parseBodyDirect(r *http.Request) (map[string]interface{}, string) {
	if r.Body == nil {
		return nil, "request body is required"
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, "failed to read request body"
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	if len(body) == 0 {
		return nil, "request body is empty"
	}

	var data map[string]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, "invalid JSON in request body"
	}

	return data, ""
}

// HasParsedBody checks if there's a parsed body in context.
func HasParsedBody(r *http.Request) bool {
	if r == nil {
		return false
	}
	_, ok := r.Context().Value(parsedBodyKey{}).(*ParsedBody)
	return ok
}

// GetRawBody retrieves the raw body bytes from context if available.
func GetRawBody(r *http.Request) []byte {
	if r == nil {
		return nil
	}
	if parsed, ok := r.Context().Value(parsedBodyKey{}).(*ParsedBody); ok {
		return parsed.RawBody
	}
	return nil
}
