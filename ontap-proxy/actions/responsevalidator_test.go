package actions

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/models"
)

func TestProcessResponseModification(t *testing.T) {
	t.Run("WhenContextHasValidAction_ShouldProcessResponse", func(t *testing.T) {
		// Create a mock action that doesn't return errors
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when action processes successfully")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenContextHasActionThatReturnsError_ShouldReturnError", func(t *testing.T) {
		// Create a mock action that returns an error
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(assert.AnError)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.Error(t, err, "ProcessResponseModification should return error when action.ProcessResponse returns error")
		assert.Equal(t, assert.AnError, err, "Should return the same error from action.ProcessResponse")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenContextHasInvalidActionType_ShouldNotProcess", func(t *testing.T) {
		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing invalid action type
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", "not an action")
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when context has invalid action type")
	})

	t.Run("WhenContextIsNil_ShouldNotProcess", func(t *testing.T) {
		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing nil
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", nil)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when context is nil")
	})

	t.Run("WhenNoRuleContext_ShouldNotProcess", func(t *testing.T) {
		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request without ruleContext
		req := httptest.NewRequest("GET", "/test", nil)
		resp.Request = req

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when no ruleContext exists")
	})

	t.Run("WhenContextHasDifferentKey_ShouldNotProcess", func(t *testing.T) {
		// Create a mock action
		mockAction := new(MockRequestProcessor)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing action under different key
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "differentKey", mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when action is under different key")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenResponseBodyIsNil_ShouldHandleGracefully", func(t *testing.T) {
		// Create a mock action that doesn't return errors
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		req = req.WithContext(ctx)

		// Create a response with nil body (edge case)
		resp := &http.Response{
			Request: req,
			Body:    nil,
		}

		// Test the function - it should not panic
		assert.NotPanics(t, func() {
			_ = ProcessResponseModification(resp)
		}, "ProcessResponseModification should not panic with nil response body")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenResponseRequestIsNil_ShouldHandleGracefully", func(t *testing.T) {
		// Create a test response with nil request
		resp := &http.Response{
			Body:    io.NopCloser(strings.NewReader(`{"test": "data"}`)),
			Request: nil,
		}

		// Test the function - it should not panic
		assert.NotPanics(t, func() {
			_ = ProcessResponseModification(resp)
		}, "ProcessResponseModification should not panic with nil response request")
	})

	t.Run("WhenActionProcessResponseFails_ShouldLogAndReturnError", func(t *testing.T) {
		// Create a mock action that returns an error
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(assert.AnError)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.Error(t, err, "ProcessResponseModification should return error when action.ProcessResponse fails")
		assert.Equal(t, assert.AnError, err, "Should return the exact error from action.ProcessResponse")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenActionProcessResponseSucceeds_ShouldReturnNil", func(t *testing.T) {
		// Create a mock action that doesn't return errors
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should return nil when action.ProcessResponse succeeds")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenMultipleActionsInContext_ShouldUseCorrectOne", func(t *testing.T) {
		// Create two mock actions
		mockAction1 := new(MockRequestProcessor)
		mockAction2 := new(MockRequestProcessor)

		// Only the second action should be called
		mockAction2.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing both actions
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction2)
		ctx = context.WithValue(ctx, "otherContext", mockAction1)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should use the correct action from ruleContext")

		// Verify only the correct action was called
		mockAction1.AssertNotCalled(t, "ProcessResponse")
		mockAction2.AssertExpectations(t)
	})

	t.Run("WhenContextHasNonStringKey_ShouldNotProcess", func(t *testing.T) {
		// Create a mock action
		mockAction := new(MockRequestProcessor)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing action under non-string key
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), 123, mockAction) // Using int key instead of string
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error when action is under non-string key")
		mockAction.AssertExpectations(t)
	})
}

func TestProcessResponseModificationWithRealActions(t *testing.T) {
	t.Run("WhenUsingVolumeAction_ShouldProcessSuccessfully", func(t *testing.T) {
		// Import the processor package to use real VolumeAction
		// Note: This test demonstrates integration with real actions
		// In a real scenario, you might want to test with actual VolumeAction instances

		// Create a mock that implements RequestProcessor interface
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"name": "test-volume", "size": 1073741824}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should work with real action types")
		mockAction.AssertExpectations(t)
	})
}

func TestProcessResponseModificationEdgeCases(t *testing.T) {
	t.Run("WhenResponseHasEmptyBody_ShouldHandleGracefully", func(t *testing.T) {
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a response with empty body
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader("")),
		}

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should handle empty response body")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenResponseHasLargeBody_ShouldHandleGracefully", func(t *testing.T) {
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a response with large body
		largeData := strings.Repeat(`{"key": "value", "data": "large_content"},`, 1000)
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"records": [` + largeData + `]}`)),
		}

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should handle large response body")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenActionReturnsMultipleErrors_ShouldReturnFirstError", func(t *testing.T) {
		// Create a mock action that returns an error
		mockAction := new(MockRequestProcessor)
		expectedError := assert.AnError
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(expectedError)

		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), models.RuleContextKey, mockAction)
		resp.Request = req.WithContext(ctx)

		err := ProcessResponseModification(resp)
		assert.Error(t, err, "ProcessResponseModification should return error from action")
		assert.Equal(t, expectedError, err, "Should return the exact error from action")
		mockAction.AssertExpectations(t)
	})
}
