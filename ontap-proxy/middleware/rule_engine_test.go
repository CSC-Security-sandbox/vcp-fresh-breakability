package middleware

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/dsl"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

func TestRuleEngineMiddleware(t *testing.T) {
	t.Run("WhenNoRuleMatch_ShouldPassThrough", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/unknown/path"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest(http.MethodGet, "/ontap/api/unknown/path", nil)
		w := httptest.NewRecorder()

		// Execute
		handler.ServeHTTP(w, req)

		// Verify
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenRuleMatchesAndAllows_ShouldPassThrough", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/storage/aggregates"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest(http.MethodGet, "/ontap/api/storage/aggregates", nil)
		w := httptest.NewRecorder()

		// Execute
		handler.ServeHTTP(w, req)

		// Verify
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenNoRuleForPrivatePath_ShouldPassThrough", func(t *testing.T) {
		// Setup - /api/private/something has no rule; only explicit private CLI paths are configured
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/private/something"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest(http.MethodGet, "/ontap/api/private/something", nil)
		w := httptest.NewRecorder()

		// Execute
		handler.ServeHTTP(w, req)

		// Verify - no rule matches, so request passes through to next handler
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("WhenDenyActionConfigured_ShouldReturnBadRequestWithReason", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/storage/aggregates"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		// POST is denied for aggregates
		req := httptest.NewRequest(http.MethodPost, "/ontap/api/storage/aggregates", nil)
		w := httptest.NewRecorder()

		// Execute
		handler.ServeHTTP(w, req)

		// Verify - POST is configured with Deny action, returns 400 with reason
		assert.Equal(t, http.StatusBadRequest, w.Code)
		assert.Contains(t, w.Body.String(), "Aggregate creation not allowed")
	})

	t.Run("WhenActionInContext_ShouldBeRetrievable", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/storage/aggregates"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		actionFound := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ctx := r.Context().Value(models.RuleContextKey); ctx != nil {
				if _, ok := ctx.(dsl.IAction); ok {
					actionFound = true
				}
			}
			w.WriteHeader(http.StatusOK)
		})

		middleware := RuleEngineMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest(http.MethodGet, "/ontap/api/storage/aggregates", nil)
		w := httptest.NewRecorder()

		// Execute
		handler.ServeHTTP(w, req)

		// Verify
		assert.True(t, actionFound, "Action should be found in context")
	})
}

func TestFindMatchingRule(t *testing.T) {
	t.Run("WhenExactMatch_ShouldReturnRule", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/storage/aggregates"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		logger := util.GetLogger(context.Background())

		// Execute
		rule, path, found := findMatchingRule("/ontap/api/storage/aggregates", logger)

		// Verify
		assert.True(t, found)
		assert.Equal(t, "/api/storage/aggregates", path)
		assert.NotNil(t, rule.GET)
	})

	t.Run("WhenPrivatePathWithNoExactRule_ShouldReturnNotFound", func(t *testing.T) {
		// Setup - no catch-all for /api/private/*; only explicit private CLI paths have rules
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/private/nested/path"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		logger := util.GetLogger(context.Background())

		// Execute
		_, _, found := findMatchingRule("/ontap/api/private/nested/path", logger)

		// Verify - no rule for this path, so not found
		assert.False(t, found)
	})

	t.Run("WhenNoMatch_ShouldReturnNotFound", func(t *testing.T) {
		// Setup
		originalExtract := extractOntapPathUtil
		extractOntapPathUtil = func(fullPath string) string {
			return "/api/unknown/path"
		}
		defer func() { extractOntapPathUtil = originalExtract }()

		logger := util.GetLogger(context.Background())

		// Execute
		_, _, found := findMatchingRule("/ontap/api/unknown/path", logger)

		// Verify
		assert.False(t, found)
	})
}

func TestNormalizeUUIDs(t *testing.T) {
	t.Run("WhenPathContainsUUID_ShouldReplaceWithPlaceholder", func(t *testing.T) {
		path := "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000"

		result := normalizeUUIDs(path)

		assert.Equal(t, "/api/storage/volumes/{uuid}", result)
	})

	t.Run("WhenPathContainsMultipleUUIDs_ShouldReplaceAll", func(t *testing.T) {
		path := "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000/snapshots/660e8400-e29b-41d4-a716-446655440001"

		result := normalizeUUIDs(path)

		assert.Equal(t, "/api/storage/volumes/{uuid}/snapshots/{uuid}", result)
	})

	t.Run("WhenPathHasNoUUID_ShouldReturnUnchanged", func(t *testing.T) {
		path := "/api/storage/volumes"

		result := normalizeUUIDs(path)

		assert.Equal(t, "/api/storage/volumes", result)
	})
}
