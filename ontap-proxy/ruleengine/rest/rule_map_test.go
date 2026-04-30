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

	t.Run("ShouldContainPrivateCLIVolumeRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/cli/volume"]
		assert.True(t, ok, "Should have rule for /api/private/cli/volume")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainPrivateCLIVolumeRenameRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/cli/volume/rename"]
		assert.True(t, ok, "Should have rule for /api/private/cli/volume/rename")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainPrivateCLIVolumeShowFootprintRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/cli/volume/show-footprint"]
		assert.True(t, ok, "Should have rule for /api/private/cli/volume/show-footprint")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainPrivateCLIVolumeCloneRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/cli/volume/clone"]
		assert.True(t, ok, "Should have rule for /api/private/cli/volume/clone")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainPrivateCLIVolumeCloneSplitStartRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/private/cli/volume/clone/split/start"]
		assert.True(t, ok, "Should have rule for /api/private/cli/volume/clone/split/start")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
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

	t.Run("ShouldContainStorageFlexCacheRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/flexcache/flexcaches"]
		assert.True(t, ok, "Should have rule for /api/storage/flexcache/flexcaches")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainStorageFlexCacheUUIDRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/flexcache/flexcaches/{uuid}"]
		assert.True(t, ok, "Should have rule for /api/storage/flexcache/flexcaches/{uuid}")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.DELETE)
	})

	t.Run("ShouldContainStorageAggregatesRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/storage/aggregates"]
		assert.True(t, ok, "Should have rule for /api/storage/aggregates")
		assert.NotNil(t, rule.GET)
	})

	t.Run("ShouldContainClusterCounterTablesRule", func(t *testing.T) {
		rules := GetProxyRules()
		rule, ok := rules["/api/cluster/counter/tables/*"]
		assert.True(t, ok, "Should have rule for /api/cluster/counter/tables/*")
		assert.NotNil(t, rule.GET)
		assert.NotNil(t, rule.POST)
		assert.NotNil(t, rule.PATCH)
		assert.NotNil(t, rule.DELETE)
	})
}

func TestClusterCounterTablesRule(t *testing.T) {
	t.Run("WhenGET_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/cluster/counter/tables/*"]
		req := httptest.NewRequest(http.MethodGet, "/api/cluster/counter/tables", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "GET should be denied for cluster counter tables")
		assert.Contains(t, reason, "Cluster counter tables not allowed")
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/cluster/counter/tables/*"]
		req := httptest.NewRequest(http.MethodPost, "/api/cluster/counter/tables", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for cluster counter tables")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/cluster/counter/tables/*"]
		req := httptest.NewRequest(http.MethodPatch, "/api/cluster/counter/tables/qos_detail", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for cluster counter tables")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/cluster/counter/tables/*"]
		req := httptest.NewRequest(http.MethodDelete, "/api/cluster/counter/tables/wafl/rows/123", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for cluster counter tables")
		assert.NotEmpty(t, reason)
	})
}

func TestPrivateCLIVolumeRule(t *testing.T) {
	origValidateCreation := validatePrivateCLIVolumeCreation
	origValidateModification := validatePrivateCLIVolumeModification
	origValidateDeletion := validatePrivateCLIVolumeDeletion
	defer func() {
		validatePrivateCLIVolumeCreation = origValidateCreation
		validatePrivateCLIVolumeModification = origValidateModification
		validatePrivateCLIVolumeDeletion = origValidateDeletion
	}()

	validatePrivateCLIVolumeCreation = func(r *http.Request) (bool, string) { return true, "" }
	validatePrivateCLIVolumeModification = func(r *http.Request) (bool, string) { return true, "" }
	validatePrivateCLIVolumeDeletion = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		req := httptest.NewRequest(http.MethodGet, "/api/private/cli/volume", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for private CLI volume")
	})

	t.Run("WhenPOSTWithRequiredFields_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"volume":"vol1","vserver":"vs0","size":1073741824}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with required fields should be allowed")
	})

	t.Run("WhenPOSTWithoutVolume_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"vserver":"vs0","size":1073741824}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without volume should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithValidSpaceGuarantee_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"volume":"vol1","vserver":"vs0","size":1073741824,"space_guarantee":"none"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with space_guarantee='none' should be allowed")
	})

	t.Run("WhenPOSTWithInvalidSpaceGuarantee_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"volume":"vol1","vserver":"vs0","size":1073741824,"space_guarantee":"volume"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with invalid space_guarantee should be denied")
		assert.NotEmpty(t, reason)
	})

	// snaplock_type is no longer allowlist-validated; previously-denied values must be allowed (regression guard).
	t.Run("WhenPOSTWithPreviouslyDeniedSnaplockTypeCompliance_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"volume":"vol1","vserver":"vs0","size":1073741824,"snaplock_type":"compliance"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with snaplock_type=compliance should be allowed (snaplock type not validated); got reason: %s", reason)
	})

	t.Run("WhenPATCHWithQueryParams_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"size":2147483648}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/private/cli/volume?vserver=vs1&volume=vol1", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with vserver and volume query params should be allowed")
	})

	// snaplock_type is no longer allowlist-validated; previously-denied values must be allowed (regression guard).
	t.Run("WhenPATCHWithPreviouslyDeniedSnaplockTypeCompliance_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		body := bytes.NewBufferString(`{"snaplock_type":"compliance"}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/private/cli/volume?vserver=vs1&volume=vol1", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with snaplock_type=compliance should be allowed (snaplock type not validated); got reason: %s", reason)
	})

	t.Run("WhenDELETEWithQueryParams_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume"]
		req := httptest.NewRequest(http.MethodDelete, "/api/private/cli/volume?vserver=vs1&volume=vol1", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "DELETE with vserver and volume query params should be allowed")
	})
}

func TestPrivateCLIVolumeRenameRule(t *testing.T) {
	origValidateRename := validatePrivateCLIVolumeRename
	defer func() { validatePrivateCLIVolumeRename = origValidateRename }()

	validatePrivateCLIVolumeRename = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenPATCHWithNewnameAndValidatorSucceeds_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/rename"]
		body := bytes.NewBufferString(`{"newname":"vol_renamed"}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/private/cli/volume/rename?vserver=vs1&volume=vol1", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with newname and passing validator should be allowed")
	})

	t.Run("WhenPATCHWithoutNewname_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/rename"]
		body := bytes.NewBufferString(`{}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/private/cli/volume/rename?vserver=vs1&volume=vol1", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH without newname should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenGET_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/rename"]
		req := httptest.NewRequest(http.MethodGet, "/api/private/cli/volume/rename", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "GET should be denied for volume rename")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/rename"]
		body := bytes.NewBufferString(`{"newname":"vol_renamed"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/rename?vserver=vs1&volume=vol1", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for volume rename")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/rename"]
		req := httptest.NewRequest(http.MethodDelete, "/api/private/cli/volume/rename?vserver=vs1&volume=vol1", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for volume rename")
		assert.NotEmpty(t, reason)
	})
}

func TestPrivateCLIVolumeShowFootprintRule(t *testing.T) {
	t.Run("WhenGET_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/show-footprint"]
		req := httptest.NewRequest(http.MethodGet, "/api/private/cli/volume/show-footprint?vserver=vs1&volume=vol1", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "GET should be denied for private CLI volume show-footprint")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/show-footprint"]
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/show-footprint", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for volume show-footprint")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/show-footprint"]
		req := httptest.NewRequest(http.MethodPatch, "/api/private/cli/volume/show-footprint", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for volume show-footprint")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/show-footprint"]
		req := httptest.NewRequest(http.MethodDelete, "/api/private/cli/volume/show-footprint", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for volume show-footprint")
		assert.NotEmpty(t, reason)
	})
}

func TestPrivateCLIVolumeCloneRule(t *testing.T) {
	origValidateCloneCreate := validatePrivateCLIVolumeCloneCreate
	defer func() { validatePrivateCLIVolumeCloneCreate = origValidateCloneCreate }()
	validatePrivateCLIVolumeCloneCreate = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenPOSTWithRequiredFields_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/clone"]
		body := bytes.NewBufferString(`{"vserver":"vs0","flexclone":"clone1","parent_volume":"src1"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/clone", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with required clone fields should be allowed, reason: %s", reason)
	})

	t.Run("WhenPOSTWithoutParentVolumeOrB_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/clone"]
		body := bytes.NewBufferString(`{"vserver":"vs0","flexclone":"clone1"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/clone", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed)
		assert.Contains(t, reason, "parent_volume or b")
	})

	t.Run("WhenGET_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/clone"]
		req := httptest.NewRequest(http.MethodGet, "/api/private/cli/volume/clone", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed)
		assert.NotEmpty(t, reason)
	})
}

func TestPrivateCLIVolumeCloneSplitStartRule(t *testing.T) {
	origValidateCloneSplit := validatePrivateCLIVolumeCloneSplit
	defer func() { validatePrivateCLIVolumeCloneSplit = origValidateCloneSplit }()
	validatePrivateCLIVolumeCloneSplit = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenPOSTWithRequiredFields_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/clone/split/start"]
		body := bytes.NewBufferString(`{"vserver":"vs0","flexclone":"clone1"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/clone/split/start", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST split start with required fields should be allowed, reason: %s", reason)
	})

	t.Run("WhenPOSTWithoutFlexclone_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/private/cli/volume/clone/split/start"]
		body := bytes.NewBufferString(`{"vserver":"vs0"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/private/cli/volume/clone/split/start", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed)
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

	// snaplock.type is no longer allowlist-validated; previously-denied values must be allowed (regression guard).
	t.Run("WhenPOSTWithPreviouslyDeniedSnaplockTypeCompliance_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume", "snaplock": {"type": "compliance"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with snaplock.type=compliance should be allowed (snaplock type not validated); got reason: %s", reason)
	})

	t.Run("WhenPOSTWithSpaceSizeOnly_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"name": "test-volume", "space": {"size": 1073741824}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with name and space.size (no top-level size) should be allowed")
	})

	t.Run("WhenPOSTWithBothSizeAndSpaceSize_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume", "space": {"size": 2147483648}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with both 'size' and 'space.size' should be allowed (core uses space.size); reason: %s", reason)
	})

	t.Run("WhenPOSTWithoutSizeOrSpaceSize_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"name": "test-volume"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without 'size' or 'space.size' should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithAutosize_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"size": 1073741824, "name": "test-volume", "autosize": {"mode": "grow"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with autosize should be denied")
		assert.Contains(t, reason, "autosize")
		assert.Contains(t, reason, "not allowed")
	})

	t.Run("WhenPOSTCloneWithIsFlexcloneWithoutSize_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes"]
		body := bytes.NewBufferString(`{"name":"clone-volume","clone":{"is_flexclone":true,"parent_volume":{"name":"src-volume"}}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/volumes", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "POST clone with is_flexclone should be allowed without size; reason: %s", reason)
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

	t.Run("WhenPATCHWithInvalidGuaranteeTypeVolume_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "name": "updated-volume", "guarantee": {"type": "volume"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH with guarantee.type='volume' should be denied")
		assert.NotEmpty(t, reason)
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

	// snaplock.type is no longer allowlist-validated; previously-denied values must be allowed (regression guard).
	t.Run("WhenPATCHWithPreviouslyDeniedSnaplockTypeCompliance_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "snaplock": {"type": "compliance"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH with snaplock.type=compliance should be allowed (snaplock type not validated); got reason: %s", reason)
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

	t.Run("WhenPATCHWithAutosize_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"autosize": {"maximum": 2147483648, "mode": "grow"}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH with autosize should be denied")
		assert.Contains(t, reason, "autosize")
		assert.Contains(t, reason, "not allowed")
	})

	t.Run("WhenPATCHWithBothSizeAndSpaceSize_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/volumes/{uuid}"]
		body := bytes.NewBufferString(`{"size": 2147483648, "space": {"size": 3221225472}}`)
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/volumes/550e8400-e29b-41d4-a716-446655440000", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH with both 'size' and 'space.size' should be denied")
		assert.Contains(t, reason, "use one or the other")
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

func TestStorageFlexCacheRule(t *testing.T) {
	origValidateFlexCacheCreation := validateFlexCacheCreation
	defer func() { validateFlexCacheCreation = origValidateFlexCacheCreation }()

	validateFlexCacheCreation = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		req := httptest.NewRequest(http.MethodGet, "/api/storage/flexcache/flexcaches", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for flexcache collection")
	})

	t.Run("WhenPOSTWithRequiredFields_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{"name":"svm1"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with required fields should be allowed")
	})

	t.Run("WhenPOSTWithoutSize_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","svm":{"name":"svm1"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without size should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithoutSvmName_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST without svm.name should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithValidGuaranteeType_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{"name":"svm1"},"guarantee":{"type":"none"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with guarantee.type='none' should be allowed")
	})

	t.Run("WhenPOSTWithInvalidGuaranteeType_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{"name":"svm1"},"guarantee":{"type":"volume"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with invalid guarantee.type should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithRelativeSizeEnabledFalse_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{"name":"svm1"},"relative_size":{"enabled":false}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with relative_size.enabled=false should be allowed")
	})

	t.Run("WhenPOSTWithRelativeSizeEnabledTrue_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		body := bytes.NewBufferString(`{"name":"fc1","size":1073741824,"svm":{"name":"svm1"},"relative_size":{"enabled":true}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with relative_size.enabled=true should be denied")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/flexcache/flexcaches", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for flexcache collection")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches"]
		req := httptest.NewRequest(http.MethodDelete, "/api/storage/flexcache/flexcaches", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for flexcache collection")
		assert.NotEmpty(t, reason)
	})
}

func TestStorageFlexCacheUUIDRule(t *testing.T) {
	origValidateFlexCacheDeletion := validateFlexCacheDeletion
	defer func() { validateFlexCacheDeletion = origValidateFlexCacheDeletion }()
	validateFlexCacheDeletion = func(r *http.Request) (bool, string) { return true, "" }

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches/{uuid}"]
		req := httptest.NewRequest(http.MethodGet, "/api/storage/flexcache/flexcaches/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for specific flexcache")
	})

	t.Run("WhenDELETE_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches/{uuid}"]
		req := httptest.NewRequest(http.MethodDelete, "/api/storage/flexcache/flexcaches/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "DELETE should be allowed for specific flexcache")
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches/{uuid}"]
		req := httptest.NewRequest(http.MethodPost, "/api/storage/flexcache/flexcaches/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for specific flexcache")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/storage/flexcache/flexcaches/{uuid}"]
		req := httptest.NewRequest(http.MethodPatch, "/api/storage/flexcache/flexcaches/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH should be allowed for specific flexcache")
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

func TestSecurityCertificatesRule(t *testing.T) {
	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		req := httptest.NewRequest(http.MethodGet, "/api/security/certificates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for certificates collection")
	})

	t.Run("WhenPOSTWithTypeServer_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		body := bytes.NewBufferString(`{"type": "server", "common_name": "test.example.com"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/security/certificates", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with type=server should be allowed")
	})

	t.Run("WhenPOSTWithTypeClient_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		body := bytes.NewBufferString(`{"type": "client", "common_name": "test.example.com"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/security/certificates", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST with type=client should be allowed")
	})

	t.Run("WhenPOSTWithTypeRootCA_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		body := bytes.NewBufferString(`{"type": "root_ca", "common_name": "test.example.com"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/security/certificates", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST with type=root_ca should be denied")
		assert.Contains(t, reason, "type")
	})

	t.Run("WhenPOSTWithoutType_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		body := bytes.NewBufferString(`{"common_name": "test.example.com"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/security/certificates", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "POST without type field should be allowed (IfPresentThenValue passes)")
	})

	t.Run("WhenPATCH_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		req := httptest.NewRequest(http.MethodPatch, "/api/security/certificates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "PATCH should be denied for certificates collection")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates"]
		req := httptest.NewRequest(http.MethodDelete, "/api/security/certificates", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for certificates collection")
		assert.NotEmpty(t, reason)
	})
}

func TestSecurityCertificatesUUIDRule(t *testing.T) {
	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates/{uuid}"]
		req := httptest.NewRequest(http.MethodGet, "/api/security/certificates/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "GET should be allowed for specific certificate")
	})

	t.Run("WhenPOST_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates/{uuid}"]
		req := httptest.NewRequest(http.MethodPost, "/api/security/certificates/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "POST should be denied for specific certificate")
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPATCH_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates/{uuid}"]
		req := httptest.NewRequest(http.MethodPatch, "/api/security/certificates/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed, "PATCH should be allowed for specific certificate")
	})

	t.Run("WhenDELETE_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/security/certificates/{uuid}"]
		req := httptest.NewRequest(http.MethodDelete, "/api/security/certificates/550e8400-e29b-41d4-a716-446655440000", nil)

		action := rule.GetAction(req)

		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, "DELETE should be denied for specific certificate")
		assert.NotEmpty(t, reason)
	})
}

func TestS3ProtocolsBucketsRules(t *testing.T) {
	t.Run("RulesRegistered", func(t *testing.T) {
		rules := GetProxyRules()
		_, ok := rules["/api/protocols/s3/buckets"]
		assert.True(t, ok)
		_, ok = rules["/api/protocols/s3/services/{uuid}/buckets"]
		assert.True(t, ok)
	})

	t.Run("WhenPOSTBucketsWithNasAndPath_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"nas","nas_path":"/","svm":{"uuid":"550e8400-e29b-41d4-a716-446655440000"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPOSTBucketsWithTypeS3_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"s3","svm":{"uuid":"550e8400-e29b-41d4-a716-446655440000"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed)
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTBucketsWithNasAndEmptyNasPath_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"nas","nas_path":"","svm":{"uuid":"550e8400-e29b-41d4-a716-446655440000"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPOSTBucketsWithNasAndNoNasPathKey_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"nas","svm":{"uuid":"550e8400-e29b-41d4-a716-446655440000"}}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, reason)
		assert.Contains(t, reason, "nas_path")
	})

	t.Run("WhenPOSTServiceBucketsWithNasAndPath_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/services/{uuid}/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"nas","nas_path":"/data"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/services/550e8400-e29b-41d4-a716-446655440000/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPOSTServiceBucketsWithNasAndNoNasPathKey_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/services/{uuid}/buckets"]
		body := bytes.NewBufferString(`{"name":"b1","type":"nas"}`)
		req := httptest.NewRequest(http.MethodPost, "/api/protocols/s3/services/550e8400-e29b-41d4-a716-446655440000/buckets", body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, reason)
		assert.Contains(t, reason, "nas_path")
	})

	t.Run("WhenGETBuckets_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules["/api/protocols/s3/buckets"]
		req := httptest.NewRequest(http.MethodGet, "/api/protocols/s3/buckets", nil)
		action := rule.GetAction(req)
		allowed, _ := action.ShouldAllow(req)
		assert.True(t, allowed)
	})
}

func TestPrivateCliObjectStoreBucketCreateRule(t *testing.T) {
	path := "/api/private/cli/vserver/object-store-server/bucket"

	t.Run("RulesRegistered", func(t *testing.T) {
		rules := GetProxyRules()
		_, ok := rules[path]
		assert.True(t, ok)
	})

	t.Run("WhenPOSTWithNasAndPath_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		body := bytes.NewBufferString(`{"vserver":"vs1","bucket":"b1","type":"nas","nas_path":"/data"}`)
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPOSTWithTypeS3_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		body := bytes.NewBufferString(`{"vserver":"vs1","bucket":"b1","type":"s3"}`)
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed)
		assert.NotEmpty(t, reason)
	})

	t.Run("WhenPOSTWithNasAndEmptyNasPath_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		body := bytes.NewBufferString(`{"vserver":"vs1","bucket":"b1","type":"nas","nas_path":""}`)
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPOSTWithNasAndNoNasPathKey_ShouldDeny", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		body := bytes.NewBufferString(`{"vserver":"vs1","bucket":"b1","type":"nas"}`)
		req := httptest.NewRequest(http.MethodPost, path, body)
		req.Header.Set("Content-Type", "application/json")

		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.False(t, allowed, reason)
		assert.Contains(t, reason, "nas_path")
	})

	t.Run("WhenGET_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		req := httptest.NewRequest(http.MethodGet, path, nil)
		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenPATCH_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		body := bytes.NewBufferString(`{"type":"nas","nas_path":"/vol"}`)
		req := httptest.NewRequest(http.MethodPatch, path, body)
		req.Header.Set("Content-Type", "application/json")
		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})

	t.Run("WhenDELETE_ShouldAllow", func(t *testing.T) {
		rules := GetProxyRules()
		rule := rules[path]
		req := httptest.NewRequest(http.MethodDelete, path, nil)
		action := rule.GetAction(req)
		assert.NotNil(t, action)
		allowed, reason := action.ShouldAllow(req)
		assert.True(t, allowed, reason)
	})
}
