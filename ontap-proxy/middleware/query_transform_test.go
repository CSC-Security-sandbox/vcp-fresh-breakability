package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestQueryTransformMiddleware(t *testing.T) {
	t.Run("WhenOntapFieldsPresent_ShouldRenameToFields", func(t *testing.T) {
		var capturedQuery string
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("GET", "/api/storage/volumes?ontap_fields=name,size", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, capturedQuery, "fields=name")
		assert.NotContains(t, capturedQuery, "ontap_fields")
	})

	t.Run("WhenOntapFieldsAndOtherParamsPresent_ShouldOnlyRenameOntapFields", func(t *testing.T) {
		var capturedQuery string
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("GET", "/api/storage/volumes?ontap_fields=name,size&max_records=100", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, capturedQuery, "fields=name")
		assert.Contains(t, capturedQuery, "max_records=100")
		assert.NotContains(t, capturedQuery, "ontap_fields")
	})

	t.Run("WhenNoOntapFields_ShouldPassThrough", func(t *testing.T) {
		var capturedQuery string
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("GET", "/api/storage/volumes?fields=name&max_records=50", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		assert.Contains(t, capturedQuery, "fields=name")
		assert.Contains(t, capturedQuery, "max_records=50")
	})

	t.Run("WhenNoQueryParams_ShouldPassThrough", func(t *testing.T) {
		nextCalled := false
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			nextCalled = true
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("GET", "/api/storage/volumes", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.True(t, nextCalled)
		assert.Equal(t, http.StatusOK, rr.Code)
	})

	t.Run("WhenMultipleOntapFieldsValues_ShouldRenameAll", func(t *testing.T) {
		var capturedReq *http.Request
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedReq = r
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("GET", "/api/storage/volumes?ontap_fields=name&ontap_fields=size", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		fieldValues := capturedReq.URL.Query()["fields"]
		assert.Len(t, fieldValues, 2)
		assert.Contains(t, fieldValues, "name")
		assert.Contains(t, fieldValues, "size")
		assert.Empty(t, capturedReq.URL.Query()["ontap_fields"])
	})

	t.Run("WhenBothOntapFieldsAndFieldsPresent_ShouldOverride", func(t *testing.T) {
		var capturedReq *http.Request
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedReq = r
			w.WriteHeader(http.StatusOK)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		// Both ontap_fields and fields present - ontap_fields values should override fields
		req := httptest.NewRequest("GET", "/api/storage/volumes?ontap_fields=uuid&fields=name", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusOK, rr.Code)
		fieldValues := capturedReq.URL.Query()["fields"]
		assert.Len(t, fieldValues, 1)
		assert.Contains(t, fieldValues, "uuid")
		assert.NotContains(t, fieldValues, "name") // Original fields value should be overridden
		assert.Empty(t, capturedReq.URL.Query()["ontap_fields"])
	})

	t.Run("WhenPOSTRequestWithOntapFields_ShouldRename", func(t *testing.T) {
		var capturedQuery string
		nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			capturedQuery = r.URL.RawQuery
			w.WriteHeader(http.StatusCreated)
		})

		middleware := QueryTransformMiddleware()
		handler := middleware(nextHandler)

		req := httptest.NewRequest("POST", "/api/storage/volumes?ontap_fields=name,uuid", nil)
		rr := httptest.NewRecorder()

		handler.ServeHTTP(rr, req)

		assert.Equal(t, http.StatusCreated, rr.Code)
		assert.Contains(t, capturedQuery, "fields=name")
		assert.NotContains(t, capturedQuery, "ontap_fields")
	})
}

func TestRenameQueryParams(t *testing.T) {
	t.Run("WhenOntapFieldsPresent_ShouldReturnTrue", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test?ontap_fields=name", nil)

		result := renameQueryParams(req)

		assert.True(t, result)
		assert.Equal(t, "name", req.URL.Query().Get("fields"))
		assert.Empty(t, req.URL.Query().Get("ontap_fields"))
	})

	t.Run("WhenNoMatchingParams_ShouldReturnFalse", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test?other=value", nil)
		originalQuery := req.URL.RawQuery

		result := renameQueryParams(req)

		assert.False(t, result)
		assert.Equal(t, originalQuery, req.URL.RawQuery)
	})

	t.Run("WhenEmptyQuery_ShouldReturnFalse", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)

		result := renameQueryParams(req)

		assert.False(t, result)
	})
}
