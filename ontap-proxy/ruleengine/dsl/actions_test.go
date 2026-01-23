package dsl

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllow(t *testing.T) {
	t.Run("ShouldAllow_AlwaysReturnsTrue", func(t *testing.T) {
		action := Allow{Name: "Test Allow"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("ProcessRequest_WithNoModification_ReturnsNameAndNoError", func(t *testing.T) {
		action := Allow{Name: "Test Allow"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		assert.NoError(t, err)
		assert.Equal(t, "Test Allow", name)
	})

	t.Run("ProcessRequest_WithModification_AppliesModification", func(t *testing.T) {
		action := Allow{
			Name: "Test Allow",
			ModifyRequest: SetHeaders{
				Headers: map[string]string{
					"X-Custom-Header": "test-value",
				},
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		assert.NoError(t, err)
		assert.Equal(t, "Test Allow", name)
		assert.Equal(t, "test-value", req.Header.Get("X-Custom-Header"))
	})

	t.Run("ProcessResponse_WithNoModification_ReturnsNameAndNoError", func(t *testing.T) {
		action := Allow{Name: "Test Allow"}
		resp := &http.Response{}

		name, err := action.ProcessResponse(resp)

		assert.NoError(t, err)
		assert.Equal(t, "Test Allow", name)
	})
}

func TestAllowAll(t *testing.T) {
	t.Run("ShouldAllow_AlwaysReturnsTrue", func(t *testing.T) {
		action := AllowAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("ProcessRequest_ReturnsAllowAll", func(t *testing.T) {
		action := AllowAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		assert.NoError(t, err)
		assert.Equal(t, "AllowAll", name)
	})

	t.Run("ProcessResponse_ReturnsAllowAll", func(t *testing.T) {
		action := AllowAll{}
		resp := &http.Response{}

		name, err := action.ProcessResponse(resp)

		assert.NoError(t, err)
		assert.Equal(t, "AllowAll", name)
	})
}

func TestDeny(t *testing.T) {
	t.Run("ShouldAllow_AlwaysReturnsFalseWithReason", func(t *testing.T) {
		action := Deny{Name: "Test Deny"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.False(t, result)
		assert.Equal(t, "Test Deny", reason)
	})

	t.Run("ProcessRequest_ReturnsNameAndNoError", func(t *testing.T) {
		action := Deny{Name: "Test Deny"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		assert.NoError(t, err)
		assert.Equal(t, "Test Deny", name)
	})

	t.Run("ProcessResponse_ReturnsNameAndNoError", func(t *testing.T) {
		action := Deny{Name: "Test Deny"}
		resp := &http.Response{}

		name, err := action.ProcessResponse(resp)

		assert.NoError(t, err)
		assert.Equal(t, "Test Deny", name)
	})
}

func TestDenyAll(t *testing.T) {
	t.Run("ShouldAllow_AlwaysReturnsFalseWithReason", func(t *testing.T) {
		action := DenyAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.False(t, result)
		assert.Equal(t, "Access denied", reason)
	})

	t.Run("ProcessRequest_ReturnsDenyAll", func(t *testing.T) {
		action := DenyAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		assert.NoError(t, err)
		assert.Equal(t, "DenyAll", name)
	})

	t.Run("ProcessResponse_ReturnsDenyAll", func(t *testing.T) {
		action := DenyAll{}
		resp := &http.Response{}

		name, err := action.ProcessResponse(resp)

		assert.NoError(t, err)
		assert.Equal(t, "DenyAll", name)
	})
}

func TestResolveAction(t *testing.T) {
	t.Run("WhenActionIsNil_ReturnsNotAllowedWithReason", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(nil, req)

		assert.Nil(t, resolvedAction)
		assert.False(t, allowed)
		assert.Equal(t, "No action defined", reason)
	})

	t.Run("WhenActionIsWhen_DelegatesToResolve", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Always(),
			IsTrue:    Allow{Name: "Inner Allow"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.True(t, allowed)
		assert.Empty(t, reason)
		// Should return the inner Allow action, not the When wrapper
		allowAction, ok := resolvedAction.(Allow)
		assert.True(t, ok, "Expected Allow action")
		assert.Equal(t, "Inner Allow", allowAction.Name)
	})

	t.Run("WhenActionIsWhenAndConditionFails_ReturnsNotAllowedWithReason", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Never("validation failed"),
			IsTrue:    Allow{Name: "Inner Allow"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.Nil(t, resolvedAction)
		assert.False(t, allowed)
		assert.Equal(t, "validation failed", reason)
	})

	t.Run("WhenActionIsAllow_ReturnsActionAsIs", func(t *testing.T) {
		action := Allow{Name: "Direct Allow"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.True(t, allowed)
		assert.Empty(t, reason)
		allowAction, ok := resolvedAction.(Allow)
		assert.True(t, ok)
		assert.Equal(t, "Direct Allow", allowAction.Name)
	})

	t.Run("WhenActionIsAllowAll_ReturnsActionAsIs", func(t *testing.T) {
		action := AllowAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.True(t, allowed)
		assert.Empty(t, reason)
		_, ok := resolvedAction.(AllowAll)
		assert.True(t, ok)
	})

	t.Run("WhenActionIsDeny_ReturnsNotAllowedWithReason", func(t *testing.T) {
		action := Deny{Name: "Access denied for this resource"}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.Nil(t, resolvedAction)
		assert.False(t, allowed)
		assert.Equal(t, "Access denied for this resource", reason)
	})

	t.Run("WhenActionIsDenyAll_ReturnsNotAllowedWithReason", func(t *testing.T) {
		action := DenyAll{}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.Nil(t, resolvedAction)
		assert.False(t, allowed)
		assert.Equal(t, "Access denied", reason)
	})

	t.Run("WhenNestedWhen_ResolvesToLeafAction", func(t *testing.T) {
		// Outer When -> condition true -> Inner When -> condition true -> Allow
		action := When{
			Name:      "Outer When",
			Condition: Always(),
			IsTrue: When{
				Name:      "Inner When",
				Condition: Always(),
				IsTrue:    Allow{Name: "Deeply Nested Allow"},
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.True(t, allowed)
		assert.Empty(t, reason)
		allowAction, ok := resolvedAction.(Allow)
		assert.True(t, ok, "Expected Allow action from nested resolution")
		assert.Equal(t, "Deeply Nested Allow", allowAction.Name)
	})

	t.Run("WhenNestedWhenWithIsFalse_ResolvesCorrectly", func(t *testing.T) {
		// Outer When -> condition false -> IsFalse (Allow)
		action := When{
			Name:      "Outer When",
			Condition: Never("outer failed"),
			IsTrue:    Deny{Name: "Should not reach"},
			IsFalse:   Allow{Name: "Fallback Allow"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		resolvedAction, allowed, reason := ResolveAction(action, req)

		assert.True(t, allowed)
		assert.Empty(t, reason)
		allowAction, ok := resolvedAction.(Allow)
		assert.True(t, ok)
		assert.Equal(t, "Fallback Allow", allowAction.Name)
	})
}

func TestWhen(t *testing.T) {
	t.Run("ShouldAllow_WhenConditionTrue_ExecutesIsTrueAction", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Always(),
			IsTrue:    Allow{Name: "Allow"},
			IsFalse:   Deny{Name: "Deny"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("ShouldAllow_WhenConditionFalse_ExecutesIsFalseActionWithReason", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Never("condition failed"),
			IsTrue:    Allow{Name: "Allow"},
			IsFalse:   Deny{Name: "Deny: validation failed"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.False(t, result)
		assert.Equal(t, "Deny: validation failed", reason)
	})

	t.Run("ShouldAllow_WhenConditionIsNil_ReturnsFalseWithReason", func(t *testing.T) {
		action := When{
			Name:    "Test When",
			IsTrue:  Allow{Name: "Allow"},
			IsFalse: Deny{Name: "Deny"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.False(t, result)
		assert.Equal(t, "No condition defined", reason)
	})

	t.Run("ShouldAllow_WhenIsTrueIsNil_ReturnsTrue", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Always(),
			IsFalse:   Deny{Name: "Deny"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("ShouldAllow_WhenIsFalseIsNil_UsesConditionReason", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Never("specific failure reason"),
			IsTrue:    Allow{Name: "Allow"},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := action.ShouldAllow(req)

		assert.False(t, result)
		assert.Equal(t, "specific failure reason", reason) // Uses condition's reason
	})

	t.Run("ProcessRequest_WhenConditionTrue_ExecutesIsTrueAction", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Always(),
			IsTrue: Allow{
				Name: "Allow",
				ModifyRequest: SetHeaders{
					Headers: map[string]string{"X-True": "yes"},
				},
			},
			IsFalse: Allow{
				Name: "Allow False",
				ModifyRequest: SetHeaders{
					Headers: map[string]string{"X-False": "yes"},
				},
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		require.NoError(t, err)
		assert.Equal(t, "Allow", name)
		assert.Equal(t, "yes", req.Header.Get("X-True"))
		assert.Empty(t, req.Header.Get("X-False"))
	})

	t.Run("ProcessRequest_WhenConditionFalse_ExecutesIsFalseAction", func(t *testing.T) {
		action := When{
			Name:      "Test When",
			Condition: Never("failed"),
			IsTrue: Allow{
				Name: "Allow True",
				ModifyRequest: SetHeaders{
					Headers: map[string]string{"X-True": "yes"},
				},
			},
			IsFalse: Allow{
				Name: "Allow False",
				ModifyRequest: SetHeaders{
					Headers: map[string]string{"X-False": "yes"},
				},
			},
		}
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		w := httptest.NewRecorder()

		name, err := action.ProcessRequest(req, w)

		require.NoError(t, err)
		assert.Equal(t, "Allow False", name)
		assert.Empty(t, req.Header.Get("X-True"))
		assert.Equal(t, "yes", req.Header.Get("X-False"))
	})

	t.Run("NestedWhen_ShouldWorkCorrectly", func(t *testing.T) {
		isAdmin := HasHeader("X-Admin", "true")
		isOwner := HasHeader("X-Owner", "true")

		action := When{
			Name:      "Admin check",
			Condition: isAdmin,
			IsTrue:    Allow{Name: "Admin allowed"},
			IsFalse: When{
				Name:      "Owner check",
				Condition: isOwner,
				IsTrue:    Allow{Name: "Owner allowed"},
				IsFalse:   Deny{Name: "Access denied: not admin or owner"},
			},
		}

		// Test admin access
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Admin", "true")
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed)
		assert.Empty(t, reason)

		// Test owner access (non-admin)
		req2 := httptest.NewRequest(http.MethodGet, "/test", nil)
		req2.Header.Set("X-Owner", "true")
		allowed2, reason2 := action.ShouldAllow(req2)
		assert.True(t, allowed2)
		assert.Empty(t, reason2)

		// Test denied access
		req3 := httptest.NewRequest(http.MethodGet, "/test", nil)
		allowed3, reason3 := action.ShouldAllow(req3)
		assert.False(t, allowed3)
		assert.Equal(t, "Access denied: not admin or owner", reason3)
	})
}
