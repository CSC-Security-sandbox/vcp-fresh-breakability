package dsl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRule_GetAction(t *testing.T) {
	t.Run("WhenGETRequest_ShouldReturnGETAction", func(t *testing.T) {
		mockGET := Allow{Name: "GET action"}
		mockPOST := Allow{Name: "POST action"}
		mockPATCH := Allow{Name: "PATCH action"}
		mockDELETE := Allow{Name: "DELETE action"}
		mockPUT := Allow{Name: "PUT action"}
		mockHEAD := Allow{Name: "HEAD action"}

		rule := Rule{
			GET:    mockGET,
			POST:   mockPOST,
			PATCH:  mockPATCH,
			DELETE: mockDELETE,
			PUT:    mockPUT,
			HEAD:   mockHEAD,
		}

		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockGET, action)
	})

	t.Run("WhenPOSTRequest_ShouldReturnPOSTAction", func(t *testing.T) {
		mockGET := Allow{Name: "GET action"}
		mockPOST := Allow{Name: "POST action"}

		rule := Rule{
			GET:  mockGET,
			POST: mockPOST,
		}

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockPOST, action)
	})

	t.Run("WhenPUTRequest_ShouldReturnPUTAction", func(t *testing.T) {
		mockPUT := Allow{Name: "PUT action"}

		rule := Rule{
			PUT: mockPUT,
		}

		req := httptest.NewRequest(http.MethodPut, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockPUT, action)
	})

	t.Run("WhenPATCHRequest_ShouldReturnPATCHAction", func(t *testing.T) {
		mockPATCH := Allow{Name: "PATCH action"}

		rule := Rule{
			PATCH: mockPATCH,
		}

		req := httptest.NewRequest(http.MethodPatch, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockPATCH, action)
	})

	t.Run("WhenDELETERequest_ShouldReturnDELETEAction", func(t *testing.T) {
		mockDELETE := Allow{Name: "DELETE action"}

		rule := Rule{
			DELETE: mockDELETE,
		}

		req := httptest.NewRequest(http.MethodDelete, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockDELETE, action)
	})

	t.Run("WhenHEADRequest_ShouldReturnHEADAction", func(t *testing.T) {
		mockHEAD := Allow{Name: "HEAD action"}

		rule := Rule{
			HEAD: mockHEAD,
		}

		req := httptest.NewRequest(http.MethodHead, "/test", nil)
		action := rule.GetAction(req)

		assert.NotNil(t, action)
		assert.Equal(t, mockHEAD, action)
	})

	t.Run("WhenUnsupportedMethod_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET:  Allow{Name: "GET action"},
			POST: Allow{Name: "POST action"},
		}

		req := httptest.NewRequest(http.MethodOptions, "/test", nil)
		action := rule.GetAction(req)

		assert.Nil(t, action)
	})

	t.Run("WhenMethodNotConfigured_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET: Allow{Name: "GET action"},
		}

		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		action := rule.GetAction(req)

		assert.Nil(t, action)
	})

	t.Run("WhenNilRequest_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET:    Allow{Name: "GET action"},
			POST:   Allow{Name: "POST action"},
			PATCH:  Allow{Name: "PATCH action"},
			DELETE: Allow{Name: "DELETE action"},
		}

		action := rule.GetAction(nil)

		assert.Nil(t, action)
	})
}

