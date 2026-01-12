package monitoring

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestMetricsMiddlewareWithRecorder(t *testing.T) {
	tests := []struct {
		name           string
		method         string
		path           string
		handlerStatus  int
		expectedStatus int
	}{
		{
			name:           "successful GET request",
			method:         http.MethodGet,
			path:           "/api/v1/volumes",
			handlerStatus:  http.StatusOK,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "POST request with created status",
			method:         http.MethodPost,
			path:           "/api/v1/backups",
			handlerStatus:  http.StatusCreated,
			expectedStatus: http.StatusCreated,
		},
		{
			name:           "request with not found error",
			method:         http.MethodGet,
			path:           "/api/v1/nonexistent",
			handlerStatus:  http.StatusNotFound,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "request with internal server error",
			method:         http.MethodDelete,
			path:           "/api/v1/volumes/123",
			handlerStatus:  http.StatusInternalServerError,
			expectedStatus: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock recorder
			mockRecorder := new(MockMetricsRecorder)

			// Create a test handler that returns the specified status code
			testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.handlerStatus)
			})

			// Setup expectations - we need to match any params since duration varies
			mockRecorder.On("RecordAPILatency", mock.MatchedBy(func(params *MetricRecorderParams) bool {
				return params.EndPoint == tt.path &&
					params.Method == tt.method &&
					params.StatusCode == strconv.Itoa(tt.expectedStatus) &&
					params.LatencyDuration >= 0
			})).Return()

			mockRecorder.On("RecordAPIRequest", mock.MatchedBy(func(params *MetricRecorderParams) bool {
				return params.EndPoint == tt.path &&
					params.Method == tt.method &&
					params.StatusCode == strconv.Itoa(tt.expectedStatus)
			})).Return()

			// Create middleware
			middleware := MetricsMiddlewareWithRecorder(mockRecorder)
			handler := middleware(testHandler)

			// Create test request
			req := httptest.NewRequest(tt.method, tt.path, nil)
			w := httptest.NewRecorder()

			// Execute request
			handler.ServeHTTP(w, req)

			// Assertions
			assert.Equal(t, tt.expectedStatus, w.Code)
			mockRecorder.AssertExpectations(t)
		})
	}
}

func TestResponseWriter_WriteHeader(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
	}{
		{
			name:       "status OK",
			statusCode: http.StatusOK,
		},
		{
			name:       "status created",
			statusCode: http.StatusCreated,
		},
		{
			name:       "status bad request",
			statusCode: http.StatusBadRequest,
		},
		{
			name:       "status internal server error",
			statusCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a response recorder
			recorder := httptest.NewRecorder()

			// Create wrapped response writer
			wrapped := &responseWriter{
				ResponseWriter: recorder,
				statusCode:     http.StatusOK,
			}

			// Write header
			wrapped.WriteHeader(tt.statusCode)

			// Assertions
			assert.Equal(t, tt.statusCode, wrapped.statusCode)
			assert.Equal(t, tt.statusCode, recorder.Code)
		})
	}
}

func TestResponseWriter_DefaultStatusCode(t *testing.T) {
	mockRecorder := new(MockMetricsRecorder)

	testHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Don't call WriteHeader - should default to 200
		_, err := w.Write([]byte("OK"))
		assert.NoError(t, err)
	})

	mockRecorder.On("RecordAPILatency", mock.MatchedBy(func(params *MetricRecorderParams) bool {
		return params.StatusCode == strconv.Itoa(http.StatusOK)
	})).Return()

	mockRecorder.On("RecordAPIRequest", mock.MatchedBy(func(params *MetricRecorderParams) bool {
		return params.StatusCode == strconv.Itoa(http.StatusOK)
	})).Return()

	middleware := MetricsMiddlewareWithRecorder(mockRecorder)
	handler := middleware(testHandler)

	req := httptest.NewRequest(http.MethodGet, "/test", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	mockRecorder.AssertExpectations(t)
}
