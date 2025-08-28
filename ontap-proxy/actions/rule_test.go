package actions

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockAction struct {
	shouldAllow bool
	processErr  error
}

func (m *mockAction) ShouldAllow(r *http.Request) bool {
	return m.shouldAllow
}

func (m *mockAction) ProcessRequest(r *http.Request, w http.ResponseWriter) error {
	return m.processErr
}

func (m *mockAction) ProcessResponse(resp *http.Response) error {
	return nil
}

func TestRule_GetAction(t *testing.T) {
	t.Run("WhenGETMethod_ShouldReturnGETAction", func(t *testing.T) {
		getAction := &mockAction{shouldAllow: true}
		rule := Rule{
			GET: getAction,
		}

		req, err := http.NewRequest(http.MethodGet, "/test", nil)
		assert.NoError(t, err, "Failed to create GET request")

		action := rule.GetAction(req)
		assert.Equal(t, getAction, action, "Should return GET action for GET method")
	})

	t.Run("WhenPOSTMethod_ShouldReturnPOSTAction", func(t *testing.T) {
		postAction := &mockAction{shouldAllow: false}
		rule := Rule{
			POST: postAction,
		}

		req, err := http.NewRequest(http.MethodPost, "/test", nil)
		assert.NoError(t, err, "Failed to create POST request")

		action := rule.GetAction(req)
		assert.Equal(t, postAction, action, "Should return POST action for POST method")
	})

	t.Run("WhenPATCHMethod_ShouldReturnPATCHAction", func(t *testing.T) {
		patchAction := &mockAction{shouldAllow: true}
		rule := Rule{
			PATCH: patchAction,
		}

		req, err := http.NewRequest(http.MethodPatch, "/test", nil)
		assert.NoError(t, err, "Failed to create PATCH request")

		action := rule.GetAction(req)
		assert.Equal(t, patchAction, action, "Should return PATCH action for PATCH method")
	})

	t.Run("WhenDELETEMethod_ShouldReturnDELETEAction", func(t *testing.T) {
		deleteAction := &mockAction{shouldAllow: false}
		rule := Rule{
			DELETE: deleteAction,
		}

		req, err := http.NewRequest(http.MethodDelete, "/test", nil)
		assert.NoError(t, err, "Failed to create DELETE request")

		action := rule.GetAction(req)
		assert.Equal(t, deleteAction, action, "Should return DELETE action for DELETE method")
	})

	t.Run("WhenUnsupportedMethod_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET:    &mockAction{shouldAllow: true},
			POST:   &mockAction{shouldAllow: true},
			PATCH:  &mockAction{shouldAllow: true},
			DELETE: &mockAction{shouldAllow: true},
		}

		req, err := http.NewRequest(http.MethodPut, "/test", nil)
		assert.NoError(t, err, "Failed to create PUT request")

		action := rule.GetAction(req)
		assert.Nil(t, action, "Should return nil for unsupported HTTP method")
	})

	t.Run("WhenMethodNotSet_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{}

		req, err := http.NewRequest(http.MethodGet, "/test", nil)
		assert.NoError(t, err, "Failed to create GET request")

		action := rule.GetAction(req)
		assert.Nil(t, action, "Should return nil when GET action is not set")
	})

	t.Run("WhenAllMethodsSet_ShouldReturnCorrectActions", func(t *testing.T) {
		getAction := &mockAction{shouldAllow: true}
		postAction := &mockAction{shouldAllow: false}
		patchAction := &mockAction{shouldAllow: true}
		deleteAction := &mockAction{shouldAllow: false}

		rule := Rule{
			GET:    getAction,
			POST:   postAction,
			PATCH:  patchAction,
			DELETE: deleteAction,
		}

		req, err := http.NewRequest(http.MethodGet, "/test", nil)
		assert.NoError(t, err, "Failed to create GET request")
		action := rule.GetAction(req)
		assert.Equal(t, getAction, action, "Should return GET action")

		req, err = http.NewRequest(http.MethodPost, "/test", nil)
		assert.NoError(t, err, "Failed to create POST request")
		action = rule.GetAction(req)
		assert.Equal(t, postAction, action, "Should return POST action")

		req, err = http.NewRequest(http.MethodPatch, "/test", nil)
		assert.NoError(t, err, "Failed to create PATCH request")
		action = rule.GetAction(req)
		assert.Equal(t, patchAction, action, "Should return PATCH action")

		req, err = http.NewRequest(http.MethodDelete, "/test", nil)
		assert.NoError(t, err, "Failed to create DELETE request")
		action = rule.GetAction(req)
		assert.Equal(t, deleteAction, action, "Should return DELETE action")
	})

	t.Run("WhenMethodIsOptions_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET: &mockAction{shouldAllow: true},
		}

		req, err := http.NewRequest(http.MethodOptions, "/test", nil)
		assert.NoError(t, err, "Failed to create OPTIONS request")

		action := rule.GetAction(req)
		assert.Nil(t, action, "Should return nil for OPTIONS method")
	})

	t.Run("WhenMethodIsHead_ShouldReturnNil", func(t *testing.T) {
		rule := Rule{
			GET: &mockAction{shouldAllow: true},
		}

		req, err := http.NewRequest(http.MethodHead, "/test", nil)
		assert.NoError(t, err, "Failed to create HEAD request")

		action := rule.GetAction(req)
		assert.Nil(t, action, "Should return nil for HEAD method")
	})
}
