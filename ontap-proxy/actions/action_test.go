package actions

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRule_GetAction(t *testing.T) {
	t.Run("WhenGETRequest_ShouldReturnGETAction", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		action := rule.GetAction(req)

		assert.Equal(t, mockGET, action, "Should return GET action for GET request")
	})

	t.Run("WhenPOSTRequest_ShouldReturnPOSTAction", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		action := rule.GetAction(req)

		assert.Equal(t, mockPOST, action, "Should return POST action for POST request")
	})

	t.Run("WhenPATCHRequest_ShouldReturnPATCHAction", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		req := httptest.NewRequest(http.MethodPatch, "/test", nil)
		action := rule.GetAction(req)

		assert.Equal(t, mockPATCH, action, "Should return PATCH action for PATCH request")
	})

	t.Run("WhenDELETERequest_ShouldReturnDELETEAction", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		req := httptest.NewRequest(http.MethodDelete, "/test", nil)
		action := rule.GetAction(req)

		assert.Equal(t, mockDELETE, action, "Should return DELETE action for DELETE request")
	})

	t.Run("WhenUnsupportedMethod_ShouldReturnNil", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		testCases := []string{
			http.MethodPut,
			http.MethodHead,
			http.MethodOptions,
			http.MethodTrace,
			"INVALID_METHOD",
		}

		for _, method := range testCases {
			t.Run("Method_"+method, func(t *testing.T) {
				req := httptest.NewRequest(method, "/test", nil)
				action := rule.GetAction(req)

				assert.Nil(t, action, "Should return nil for unsupported method: %s", method)
			})
		}
	})

	t.Run("WhenNilRequest_ShouldReturnNil", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}
		mockPATCH := &MockRequestProcessor{}
		mockDELETE := &MockRequestProcessor{}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
		}

		action := rule.GetAction(nil)

		assert.Nil(t, action, "Should return nil for nil request")
	})

	t.Run("WhenEmptyRule_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		action := rule.GetAction(req)

		assert.Nil(t, action, "Should return nil when rule has no GET action")
	})

	t.Run("WhenPartialRule_ShouldReturnCorrectAction", func(t *testing.T) {
		mockGET := &MockRequestProcessor{}
		mockPOST := &MockRequestProcessor{}

		rule := Rule{
			GET:  mockGET,
			POST: mockPOST,
			// PATCH and DELETE are nil
		}

		// Test GET
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		action := rule.GetAction(req)
		assert.Equal(t, mockGET, action, "Should return GET action")

		// Test POST
		req = httptest.NewRequest(http.MethodPost, "/test", nil)
		action = rule.GetAction(req)
		assert.Equal(t, mockPOST, action, "Should return POST action")

		// Test PATCH (should return nil)
		req = httptest.NewRequest(http.MethodPatch, "/test", nil)
		action = rule.GetAction(req)
		assert.Nil(t, action, "Should return nil for PATCH when not set")

		// Test DELETE (should return nil)
		req = httptest.NewRequest(http.MethodDelete, "/test", nil)
		action = rule.GetAction(req)
		assert.Nil(t, action, "Should return nil for DELETE when not set")
	})
}
