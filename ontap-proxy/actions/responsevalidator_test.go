package actions

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	mock "github.com/stretchr/testify/mock"
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
		ctx := context.WithValue(req.Context(), "ruleContext", mockAction)
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
		ctx := context.WithValue(req.Context(), "ruleContext", mockAction)
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

	t.Run("WhenUsingRealAllowAction_ShouldProcessSuccessfully", func(t *testing.T) {
		// Create a real Allow action
		allowAction := Allow{
			Name:         "TestAllow",
			RemoveFields: []string{"password"},
		}

		// Create a test response with JSON data
		jsonData := `{"name": "test", "password": "secret123", "public": "visible"}`
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(jsonData)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", allowAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error with real Allow action")

		// Verify the response was modified
		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")
		assert.NotContains(t, string(body), "password", "Password field should be removed")
		assert.Contains(t, string(body), "name", "Name field should remain")
		assert.Contains(t, string(body), "public", "Public field should remain")
	})

	t.Run("WhenUsingRealDenyAction_ShouldProcessSuccessfully", func(t *testing.T) {
		// Create a real Deny action
		denyAction := Deny{
			Name: "TestDeny",
		}

		// Create a test response
		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(`{"test": "data"}`)),
		}

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", denyAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should not return error with real Deny action")
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
		ctx := context.WithValue(req.Context(), "ruleContext", mockAction)
		resp.Request = req.WithContext(ctx)

		// Test the function
		err := ProcessResponseModification(resp)
		assert.Error(t, err, "ProcessResponseModification should return error when action.ProcessResponse fails")
		assert.Equal(t, assert.AnError, err, "Should return the exact error from action.ProcessResponse")
		mockAction.AssertExpectations(t)
	})

	t.Run("WhenResponseIsNil_ShouldHandleGracefully", func(t *testing.T) {
		// This test ensures the function doesn't panic with nil response
		// Note: In real usage, this shouldn't happen, but it's good to test edge cases
		mockAction := new(MockRequestProcessor)
		mockAction.On("ProcessResponse", mock.AnythingOfType("*http.Response")).Return(nil)

		// Create a request with context containing the action
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", mockAction)
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
}

func TestProcessResponseModificationWithComplexJSON(t *testing.T) {
	t.Run("WhenProcessingComplexJSON_ShouldWorkCorrectly", func(t *testing.T) {
		allowAction := Allow{
			Name:         "ComplexTest",
			RemoveFields: []string{"sensitive", "password"},
		}

		complexJSON := `{
			"users": [
				{"name": "user1", "password": "pass1", "role": "admin"},
				{"name": "user2", "sensitive": "data", "role": "user"}
			],
			"config": {
				"database": {
					"password": "dbpass",
					"host": "localhost"
				},
				"public": "visible"
			}
		}`

		resp := &http.Response{
			Body: io.NopCloser(strings.NewReader(complexJSON)),
		}

		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.WithValue(req.Context(), "ruleContext", allowAction)
		resp.Request = req.WithContext(ctx)

		err := ProcessResponseModification(resp)
		assert.NoError(t, err, "ProcessResponseModification should handle complex JSON")

		body, err := io.ReadAll(resp.Body)
		assert.NoError(t, err, "Should read response body")
		assert.NotContains(t, string(body), "password", "Password fields should be removed")
		assert.NotContains(t, string(body), "sensitive", "Sensitive fields should be removed")
		assert.Contains(t, string(body), "name", "Name fields should remain")
		assert.Contains(t, string(body), "role", "Role fields should remain")
		assert.Contains(t, string(body), "public", "Public fields should remain")
	})
}
