package rules_v2

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetProxyRules(t *testing.T) {
	t.Run("ShouldReturnNonEmptyRuleMap", func(t *testing.T) {
		rules := GetProxyRules()
		assert.NotEmpty(t, rules)
	})

	t.Run("ShouldContainPrivateAPIRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/*"]
		assert.True(t, ok, "Should have rule for /api/private/*")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PUT)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
		assert.NotNil(t, rule.HEAD)
	})

	t.Run("ShouldContainStorageVolumesRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/volumes"]
		assert.True(t, ok, "Should have rule for /api/storage/volumes")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
	})

	t.Run("ShouldContainStorageVolumesUUIDRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/volumes/{uuid}"]
		assert.True(t, ok, "Should have rule for /api/storage/volumes/{uuid}")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainStorageAggregatesRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/aggregates"]
		assert.True(t, ok, "Should have rule for /api/storage/aggregates")
		assert.NotNil(t, rule.GET)
	})
}

func TestPrivateAPIRule(t *testing.T) {
	t.Run("WhenGET_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/*"]
		req := httptest.NewRequest(http.MethodGet, "/api/private/test", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "GET should be denied for private API")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/*"]
		req := httptest.NewRequest(http.MethodPost, "/api/private/test", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for private API")
		assert.NotEmpty(t, reason)
	})
}

func TestStorageVolumesRule(t *testing.T) {
	// Save and restore original validator to avoid cross-test pollution
	origValidateVolumeCreation := validateVolumeCreation
	defer func() { validateVolumeCreation = origValidateVolumeCreation }()

	// Force creation validator to pass for these tests
	validateVolumeCreation = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		req := httptest.NewRequest(http.MethodGet, "/api/storage/volumes", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for storage volumes")
	})

	t.Run("WhenPOSTWithRequiredFields_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with required fields should be allowed")
	})

	t.Run("WhenPOSTWithValidGuaranteeType_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume", "guarantee": {"type": "none"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with guarantee.type='none' should be allowed")
	})

	t.Run("WhenPOSTWithInvalidGuaranteeType_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume", "guarantee": {"type": "volume"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with invalid guarantee.type should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithoutSizeField_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"name": "test-volume"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without 'size' field should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithoutNameField_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without 'name' field should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for storage volumes collection")
		assert.NotEmpty(t, reason)
	})
}

func TestStorageVolumesUUIDRule(t *testing.T) {
	// Save and restore originals
	origValidateVolumeModification := validateVolumeModification
	origValidateVolumeDeletion := validateVolumeDeletion
	defer func() {
		validateVolumeModification = origValidateVolumeModification
		validateVolumeDeletion = origValidateVolumeDeletion
	}()

	// Ensure modification validator passes for these tests unless overridden per subtest
	validateVolumeModification = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		req := httptest.NewRequest(http.MethodGet, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for specific volume")
	})

	t.Run("WhenPATCHWithValidBody_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with valid body should be allowed")
	})

	t.Run("WhenPATCHWithValidGuaranteeTypeNone_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "name": "updated-volume", "guarantee": {"type": "none"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with guarantee.type='none' should be allowed")
	})

	t.Run("WhenPATCHWithValidGuaranteeTypeVolume_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "name": "updated-volume", "guarantee": {"type": "volume"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with guarantee.type='volume' should be allowed")
	})

	t.Run("WhenPATCHWithInvalidGuaranteeType_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "name": "updated-volume", "guarantee": {"type": "invalid"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH with invalid guarantee.type should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCHWithOnlyComment_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"comment": "just a comment"}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with only comment should be allowed")
	})

	t.Run("WhenDELETE_ShouldAllow", func(t *testing.T) {
		// Mock deletion validator BEFORE building rules
		validateVolumeDeletion = func(r *http.Request) (bool, string) { return true, "" }
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		req := httptest.NewRequest(http.MethodDelete, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "DELETE should be allowed for specific volume")
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for specific volume")
		assert.NotEmpty(t, reason)
	})
}

func TestStorageAggregatesRule(t *testing.T) {
	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/aggregates"]
		req := httptest.NewRequest(http.MethodGet, "/api/storage/aggregates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for aggregates")
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/aggregates"]
		req := httptest.NewRequest(http.MethodPost, "/api/storage/aggregates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for aggregates")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/aggregates"]
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/aggregates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for aggregates")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/aggregates"]
		req := httptest.NewRequest(http.MethodDelete, "/api/storage/aggregates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for aggregates")
		assert.NotEmpty(t, reason)
	})
}
