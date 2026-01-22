package middleware

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetIAMRoleFromHeader(t *testing.T) {
	t.Run("WhenIAMRoleHeaderExists_ShouldReturnRole", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "ontap")

		iamRole := GetIAMRoleFromHeader(req)

		assert.Equal(t, "ontap", iamRole)
	})

	t.Run("WhenIAMRoleHeaderIsEmpty_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)

		iamRole := GetIAMRoleFromHeader(req)

		assert.Equal(t, "", iamRole)
	})

	t.Run("WhenIAMRoleHeaderHasWhitespace_ShouldReturnTrimmed", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "  ontap  ")

		iamRole := GetIAMRoleFromHeader(req)

		assert.Equal(t, "ontap", iamRole)
	})

	t.Run("WhenIAMRoleHeaderIsWhitespaceOnly_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "   ")

		iamRole := GetIAMRoleFromHeader(req)

		assert.Equal(t, "", iamRole)
	})

	t.Run("WhenIAMRoleHeaderIsEmptyString_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "")

		iamRole := GetIAMRoleFromHeader(req)

		assert.Equal(t, "", iamRole)
	})
}

func TestMapIAMRoleToRBACUser(t *testing.T) {
	t.Run("WhenIAMRoleInConfig_ShouldReturnMappedUser", func(t *testing.T) {
		// Note: This test depends on the actual IAM_ROLE_TO_USER_MAPPING_CONFIG environment variable
		// If the config has "ontap" mapped, it will return that value, otherwise defaults to gadmin
		ctx := context.Background()
		rbacUser := MapIAMRoleToRBACUser(ctx, "ontap")

		// Should return a non-empty value (either from config or default)
		assert.NotEmpty(t, rbacUser)
	})

	t.Run("WhenIAMRoleNotInConfig_ShouldReturnDefault", func(t *testing.T) {
		// Test with a role that's unlikely to be in config
		ctx := context.Background()
		rbacUser := MapIAMRoleToRBACUser(ctx, "unknownRole12345")

		// Should default to ExpertModeUserSuffix (gadmin)
		assert.Equal(t, "gadmin", rbacUser)
	})

	t.Run("WhenIAMRoleIsEmpty_ShouldReturnDefault", func(t *testing.T) {
		ctx := context.Background()
		rbacUser := MapIAMRoleToRBACUser(ctx, "")

		// Should default to ExpertModeUserSuffix (gadmin)
		assert.Equal(t, "gadmin", rbacUser)
	})

	t.Run("WhenCalledWithValidRole_ShouldNotPanic", func(t *testing.T) {
		// Test that the function doesn't panic with various inputs
		ctx := context.Background()

		testRoles := []string{"ontap", "privOntap", "customRole", ""}
		for _, role := range testRoles {
			rbacUser := MapIAMRoleToRBACUser(ctx, role)
			assert.NotEmpty(t, rbacUser)
		}
	})
}

func TestGetRBACUserFromRequest(t *testing.T) {
	t.Run("WhenIAMRoleHeaderExists_ShouldReturnMappedUser", func(t *testing.T) {
		// Note: This test depends on the actual IAM_ROLE_TO_USER_MAPPING_CONFIG environment variable
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "ontap")
		ctx := context.Background()

		rbacUser := GetRBACUserFromRequest(ctx, req)

		// Should return a non-empty value (either from config or default)
		assert.NotEmpty(t, rbacUser)
	})

	t.Run("WhenNoIAMRoleHeader_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		ctx := context.Background()

		rbacUser := GetRBACUserFromRequest(ctx, req)

		assert.Equal(t, "", rbacUser)
	})

	t.Run("WhenIAMRoleHeaderIsEmpty_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "")
		ctx := context.Background()

		rbacUser := GetRBACUserFromRequest(ctx, req)

		assert.Equal(t, "", rbacUser)
	})

	t.Run("WhenIAMRoleHeaderIsWhitespace_ShouldReturnEmpty", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "   ")
		ctx := context.Background()

		rbacUser := GetRBACUserFromRequest(ctx, req)

		assert.Equal(t, "", rbacUser)
	})

	t.Run("WhenIAMRoleNotInConfig_ShouldReturnDefault", func(t *testing.T) {
		// Test with a role that's unlikely to be in config
		req := httptest.NewRequest("GET", "/test", nil)
		req.Header.Set("x-google-iam-role", "unknownRole12345")
		ctx := context.Background()

		rbacUser := GetRBACUserFromRequest(ctx, req)

		// Should default to ExpertModeUserSuffix (gadmin)
		assert.Equal(t, "gadmin", rbacUser)
	})
}

func TestLoadIAMRoleMappingConfig(t *testing.T) {
	t.Run("WhenCalled_ShouldReturnMap", func(t *testing.T) {
		// Note: The config is loaded from environment variable at package initialization
		// and uses sync.Once, so we can't easily test different configs in the same test run
		// We test that it returns a valid map and doesn't panic
		config := LoadIAMRoleMappingConfig()

		assert.NotNil(t, config)
		// Config may be empty if IAM_ROLE_TO_USER_MAPPING_CONFIG is not set
		// or may contain mappings if it is set
	})

	t.Run("WhenCalledMultipleTimes_ShouldReturnSameConfig", func(t *testing.T) {
		// Test that sync.Once ensures the same config is returned
		config1 := LoadIAMRoleMappingConfig()
		config2 := LoadIAMRoleMappingConfig()

		// Should return the same map instance
		assert.Equal(t, config1, config2)
	})
}
