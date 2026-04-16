package dsl

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasFields(t *testing.T) {
	t.Run("WhenFieldExists_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFields("size")
		req := createRequestWithBody(`{"size": 1024, "name": "test"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldDoesNotExist_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFields("size")
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "size")
	})

	t.Run("WhenMultipleFieldsExist_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFields("size", "name")
		req := createRequestWithBody(`{"size": 1024, "name": "test"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenOneOfMultipleFieldsMissing_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFields("size", "name", "type")
		req := createRequestWithBody(`{"size": 1024, "name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "type")
	})

	t.Run("WhenNestedFieldExists_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFields("$.space.size")
		req := createRequestWithBody(`{"space": {"size": 1024}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenRequestIsNil_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFields("size")

		result, reason := condition(nil)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenBodyIsEmpty_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFields("size")
		req := createRequestWithBody(``)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenBodyIsInvalidJSON_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFields("size")
		req := createRequestWithBody(`not json`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})

	t.Run("ShouldRestoreRequestBody", func(t *testing.T) {
		condition := HasFields("size")
		originalBody := `{"size": 1024}`
		req := createRequestWithBody(originalBody)

		condition(req)

		// Body should be readable again
		body, err := io.ReadAll(req.Body)
		assert.NoError(t, err)
		assert.Equal(t, originalBody, string(body))
	})
}

func TestHasExactlyOneOf(t *testing.T) {
	missingReason := "missing required field(s): size or space.size"
	bothReason := "cannot specify both 'size' and 'space.size'; use one or the other"

	t.Run("WhenExactlySize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasExactlyOneOf("size", "space.size", missingReason, bothReason)
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenExactlySpaceSize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasExactlyOneOf("size", "space.size", missingReason, bothReason)
		req := createRequestWithBody(`{"space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenBothPresent_ShouldReturnFalseWithBothReason", func(t *testing.T) {
		condition := HasExactlyOneOf("size", "space.size", missingReason, bothReason)
		req := createRequestWithBody(`{"size": 1024, "space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Equal(t, bothReason, reason)
	})

	t.Run("WhenNeitherPresent_ShouldReturnFalseWithMissingReason", func(t *testing.T) {
		condition := HasExactlyOneOf("size", "space.size", missingReason, bothReason)
		req := createRequestWithBody(`{"name": "vol1"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Equal(t, missingReason, reason)
	})
}

func TestHasAtLeastOneOf(t *testing.T) {
	missingReason := "missing required field(s): size or space.size"

	t.Run("WhenOnlySize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtLeastOneOf("size", "space.size", missingReason)
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenOnlySpaceSize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtLeastOneOf("size", "space.size", missingReason)
		req := createRequestWithBody(`{"space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenBothPresent_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtLeastOneOf("size", "space.size", missingReason)
		req := createRequestWithBody(`{"size": 1024, "space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenNeitherPresent_ShouldReturnFalse", func(t *testing.T) {
		condition := HasAtLeastOneOf("size", "space.size", missingReason)
		req := createRequestWithBody(`{"name": "vol1"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Equal(t, missingReason, reason)
	})
}

func TestHasAtMostOneOf(t *testing.T) {
	bothReason := "cannot specify both 'size' and 'space.size'; use one or the other"

	t.Run("WhenNeitherPresent_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtMostOneOf("size", "space.size", bothReason)
		req := createRequestWithBody(`{"name": "vol1"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenOnlySize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtMostOneOf("size", "space.size", bothReason)
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenOnlySpaceSize_ShouldReturnTrue", func(t *testing.T) {
		condition := HasAtMostOneOf("size", "space.size", bothReason)
		req := createRequestWithBody(`{"space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenBothPresent_ShouldReturnFalseWithBothReason", func(t *testing.T) {
		condition := HasAtMostOneOf("size", "space.size", bothReason)
		req := createRequestWithBody(`{"size": 1024, "space": {"size": 2048}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Equal(t, bothReason, reason)
	})
}

func TestCheckTwoFieldsExist(t *testing.T) {
	t.Run("WhenOnlyFirstFieldExists_ReturnsTrueFalse", func(t *testing.T) {
		data := map[string]interface{}{"size": float64(1024)}
		hasA, hasB := checkTwoFieldsExist(data, "size", "space.size")
		assert.True(t, hasA)
		assert.False(t, hasB)
	})
	t.Run("WhenOnlySecondFieldExists_ReturnsFalseTrue", func(t *testing.T) {
		data := map[string]interface{}{"space": map[string]interface{}{"size": float64(2048)}}
		hasA, hasB := checkTwoFieldsExist(data, "size", "space.size")
		assert.False(t, hasA)
		assert.True(t, hasB)
	})
	t.Run("WhenBothExist_ReturnsTrueTrue", func(t *testing.T) {
		data := map[string]interface{}{"size": float64(1024), "space": map[string]interface{}{"size": float64(2048)}}
		hasA, hasB := checkTwoFieldsExist(data, "size", "space.size")
		assert.True(t, hasA)
		assert.True(t, hasB)
	})
	t.Run("WhenNeitherExists_ReturnsFalseFalse", func(t *testing.T) {
		data := map[string]interface{}{"name": "vol1"}
		hasA, hasB := checkTwoFieldsExist(data, "size", "space.size")
		assert.False(t, hasA)
		assert.False(t, hasB)
	})
	t.Run("WhenDataNil_ReturnsFalseFalse", func(t *testing.T) {
		hasA, hasB := checkTwoFieldsExist(nil, "size", "space.size")
		assert.False(t, hasA)
		assert.False(t, hasB)
	})
}

func TestHasFieldValue(t *testing.T) {
	t.Run("WhenFieldHasExpectedValue_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFieldValue("type", "volume")
		req := createRequestWithBody(`{"type": "volume"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldHasDifferentValue_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFieldValue("type", "volume")
		req := createRequestWithBody(`{"type": "aggregate"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "type")
	})

	t.Run("WhenFieldDoesNotExist_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFieldValue("type", "volume")
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenNestedFieldHasExpectedValue_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFieldValue("guarantee.type", "none")
		req := createRequestWithBody(`{"guarantee": {"type": "none"}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenCheckingBooleanValue_ShouldWork", func(t *testing.T) {
		condition := HasFieldValue("enabled", true)
		req := createRequestWithBody(`{"enabled": true}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})
}

func TestHasFieldValueIn(t *testing.T) {
	t.Run("WhenFieldValueIsInAllowedList_ShouldReturnTrue", func(t *testing.T) {
		condition := HasFieldValueIn("type", "volume", "aggregate", "lun")
		req := createRequestWithBody(`{"type": "volume"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldValueNotInAllowedList_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFieldValueIn("type", "volume", "aggregate")
		req := createRequestWithBody(`{"type": "snapshot"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "type")
	})

	t.Run("WhenFieldDoesNotExist_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasFieldValueIn("type", "volume")
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})
}

func TestHasHeader(t *testing.T) {
	t.Run("WhenHeaderHasExpectedValue_ShouldReturnTrue", func(t *testing.T) {
		condition := HasHeader("X-Admin-Token", "secret")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Admin-Token", "secret")

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenHeaderHasDifferentValue_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasHeader("X-Admin-Token", "secret")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)
		req.Header.Set("X-Admin-Token", "wrong")

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "X-Admin-Token")
	})

	t.Run("WhenHeaderDoesNotExist_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := HasHeader("X-Admin-Token", "secret")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})
}

func TestAnd(t *testing.T) {
	t.Run("WhenAllConditionsTrue_ShouldReturnTrue", func(t *testing.T) {
		condition := And(
			HasFields("size"),
			HasFields("name"),
		)
		req := createRequestWithBody(`{"size": 1024, "name": "test"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenOneConditionFalse_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := And(
			HasFields("size"),
			HasFields("type"),
		)
		req := createRequestWithBody(`{"size": 1024, "name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "type") // Returns first failure reason
	})

	t.Run("WhenNoConditions_ShouldReturnTrue", func(t *testing.T) {
		condition := And()
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})
}

func TestOr(t *testing.T) {
	t.Run("WhenOneConditionTrue_ShouldReturnTrue", func(t *testing.T) {
		condition := Or(
			HasFields("size"),
			HasFields("type"),
		)
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenAllConditionsFalse_ShouldReturnFalseWithLastReason", func(t *testing.T) {
		condition := Or(
			HasFields("size"),
			HasFields("type"),
		)
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason) // Returns last failure reason
	})

	t.Run("WhenNoConditions_ShouldReturnFalse", func(t *testing.T) {
		condition := Or()
		req := createRequestWithBody(`{"size": 1024}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Empty(t, reason)
	})
}

func TestNot(t *testing.T) {
	t.Run("WhenConditionTrue_ShouldReturnFalse", func(t *testing.T) {
		condition := Not(HasFields("size"))
		req := createRequestWithBody(`{"size": 1024}`)

		result, _ := condition(req)

		assert.False(t, result)
	})

	t.Run("WhenConditionFalse_ShouldReturnTrue", func(t *testing.T) {
		condition := Not(HasFields("size"))
		req := createRequestWithBody(`{"name": "test"}`)

		result, _ := condition(req)

		assert.True(t, result)
	})
}

func TestIsMethod(t *testing.T) {
	t.Run("WhenMethodMatches_ShouldReturnTrue", func(t *testing.T) {
		condition := IsMethod("POST")
		req := httptest.NewRequest(http.MethodPost, "/test", nil)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenMethodDoesNotMatch_ShouldReturnFalseWithReason", func(t *testing.T) {
		condition := IsMethod("POST")
		req := httptest.NewRequest(http.MethodGet, "/test", nil)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "POST")
	})
}

func TestComplexConditions(t *testing.T) {
	t.Run("WhenComplexAndOrCondition_ShouldWorkCorrectly", func(t *testing.T) {
		// Either admin header OR (size field AND type is volume)
		condition := Or(
			HasHeader("X-Admin", "true"),
			And(
				HasFields("size"),
				HasFieldValue("type", "volume"),
			),
		)

		// Admin header present
		req1 := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(`{"name": "test"}`))
		req1.Header.Set("X-Admin", "true")
		result1, _ := condition(req1)
		assert.True(t, result1)

		// Size and type=volume present
		req2 := createRequestWithBody(`{"size": 1024, "type": "volume"}`)
		result2, _ := condition(req2)
		assert.True(t, result2)

		// Neither condition met
		req3 := createRequestWithBody(`{"name": "test"}`)
		result3, reason3 := condition(req3)
		assert.False(t, result3)
		assert.NotEmpty(t, reason3)
	})
}

func TestIfPresentThenValue(t *testing.T) {
	t.Run("WhenFieldNotPresent_ShouldPass", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none")
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldPresentWithValidValue_ShouldPass", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none", "volume")
		req := createRequestWithBody(`{"guarantee": {"type": "none"}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldPresentWithInvalidValue_ShouldFailWithReason", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none")
		req := createRequestWithBody(`{"guarantee": {"type": "invalid"}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "guarantee.type")
	})

	t.Run("WhenInvalidJSON_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none")
		req := createRequestWithBody(`{invalid json`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "invalid JSON")
	})

	t.Run("WhenEmptyBody_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none")
		req := createRequestWithBody(``)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "empty")
	})

	t.Run("WhenNilBody_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenValue("guarantee.type", "none")
		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Body = nil // Explicitly set to nil

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "required")
	})
}

func TestIfPresentThenEquals(t *testing.T) {
	t.Run("WhenFieldNotPresent_ShouldPass", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := createRequestWithBody(`{"name": "test"}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldPresentWithExpectedValue_ShouldPass", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := createRequestWithBody(`{"space": {"logical_space": {"enforcement": true}}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldPresentWithWrongValue_ShouldFailWithReason", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := createRequestWithBody(`{"space": {"logical_space": {"enforcement": false}}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "space.logical_space.enforcement")
	})

	t.Run("WhenInvalidJSON_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := createRequestWithBody(`{malformed`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "invalid JSON")
	})

	t.Run("WhenEmptyBody_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := createRequestWithBody(``)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "empty")
	})

	t.Run("WhenNilBody_ShouldFailWithParseError", func(t *testing.T) {
		condition := IfPresentThenEquals("space.logical_space.enforcement", true)
		req := httptest.NewRequest(http.MethodPost, "/test", nil)
		req.Body = nil // Explicitly set to nil

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "required")
	})
}

func TestRejectFields(t *testing.T) {
	t.Run("WhenFieldNotPresent_ShouldPass", func(t *testing.T) {
		condition := RejectFields("autosize")
		req := createRequestWithBody(`{"name": "vol1", "size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenFieldPresent_ShouldReject", func(t *testing.T) {
		condition := RejectFields("autosize")
		req := createRequestWithBody(`{"name": "vol1", "autosize": {"mode": "grow"}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "autosize")
		assert.Contains(t, reason, "not allowed")
	})

	t.Run("WhenNestedFieldPresent_ShouldReject", func(t *testing.T) {
		condition := RejectFields("space.snapshot")
		req := createRequestWithBody(`{"space": {"snapshot": {"reserve_percent": 5}}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "space.snapshot")
	})

	t.Run("WhenNestedFieldNotPresent_ShouldPass", func(t *testing.T) {
		condition := RejectFields("space.snapshot")
		req := createRequestWithBody(`{"space": {"size": 1024}}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenMultipleFields_NonePresent_ShouldPass", func(t *testing.T) {
		condition := RejectFields("autosize", "efficiency")
		req := createRequestWithBody(`{"name": "vol1", "size": 1024}`)

		result, reason := condition(req)

		assert.True(t, result)
		assert.Empty(t, reason)
	})

	t.Run("WhenMultipleFields_OnePresent_ShouldReject", func(t *testing.T) {
		condition := RejectFields("autosize", "efficiency")
		req := createRequestWithBody(`{"name": "vol1", "efficiency": {"compaction": "inline"}}`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "efficiency")
	})

	t.Run("WhenInvalidJSON_ShouldReturnParseError", func(t *testing.T) {
		condition := RejectFields("autosize")
		req := createRequestWithBody(`{bad json`)

		result, reason := condition(req)

		assert.False(t, result)
		assert.Contains(t, reason, "invalid JSON")
	})

	t.Run("WhenEmptyBody_ShouldReturnParseError", func(t *testing.T) {
		condition := RejectFields("autosize")
		req := createRequestWithBody(``)

		result, reason := condition(req)

		assert.False(t, result)
		assert.NotEmpty(t, reason)
	})
}

// Helper function
func createRequestWithBody(body string) *http.Request {
	req := httptest.NewRequest(http.MethodPost, "/test", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	return req
}
