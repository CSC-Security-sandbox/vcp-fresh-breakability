package endpoints

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/cli"
)

func TestGetPrivilegeLevel(t *testing.T) {
	t.Run("returns default when not set", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume show",
			// Privilege not set
		}
		privilege := getPrivilegeLevel(req)
		assert.Equal(t, "admin", privilege)
	})

	t.Run("returns admin when admin is set", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input:     "volume show",
			Privilege: oasgenserver.NewOptCLIExecuteRequestPrivilege(oasgenserver.CLIExecuteRequestPrivilegeAdmin),
		}
		privilege := getPrivilegeLevel(req)
		assert.Equal(t, "admin", privilege)
	})
}

func TestV1PrivateCli_OperationDisabled(t *testing.T) {
	h := Handler{}
	ctx := context.Background()
	poolId := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenPrivateCliOperationDisabled_ShouldReturn400", func(t *testing.T) {
		original := privateCliOperationEnabled
		privateCliOperationEnabled = false
		defer func() { privateCliOperationEnabled = original }()

		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume show",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)

		require.NoError(t, err, "V1PrivateCli should not return a Go error")
		require.NotNil(t, res, "Response should not be nil")

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected V1PrivateCliBadRequest, got %T", res)
		assert.Equal(t, 400, badReq.Code, "Code should be 400")
		assert.Equal(t, "Private CLI operation is disabled", badReq.Message, "Message should match")
	})
}

func TestV1PrivateCli_Validation(t *testing.T) {
	original := privateCliOperationEnabled
	privateCliOperationEnabled = true
	defer func() { privateCliOperationEnabled = original }()

	h := Handler{}
	ctx := context.Background()
	poolId := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("empty input returns bad request", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "required")
	})

	t.Run("denied command returns bad request", func(t *testing.T) {
		// Use a command that has an explicit deny rule
		req := &oasgenserver.CLIExecuteRequest{
			Input: "security certificate delete -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected V1PrivateCliBadRequest for denied command, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	t.Run("composite commands rejected outright", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; volume create -vserver vs1 -volume vol1 -size 100g",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected V1PrivateCliBadRequest for composite command, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Composite commands are not allowed", badReq.Message)
	})

	t.Run("composite with allowed commands only still rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume show -vserver vs1; volume show -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected V1PrivateCliBadRequest for any composite command, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Equal(t, "Composite commands are not allowed", badReq.Message)
	})
}

func TestV1PrivateCli_RuleMatching(t *testing.T) {
	// Test that rules are properly matched (corresponds to rule_map.go)
	t.Run("volume show matches allowed rule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume show -vserver vs1")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
	})

	t.Run("security certificate show allowed", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("security certificate show")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
	})

	t.Run("security certificate delete denied", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("security certificate delete -vserver vs1")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.False(t, rule.Allow)
	})
}

func TestV1PrivateCli_ResponseFiltering(t *testing.T) {
	t.Run("volume show rule has RemoveFields configured", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume show")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)

		// volume show rule should have fields to remove
		assert.NotEmpty(t, rule.RemoveFields)
		assert.Contains(t, rule.RemoveFields, "Used Size")
		assert.Contains(t, rule.RemoveFields, "Used Percentage")
	})

	t.Run("RemoveFieldsFromCLIOutput removes sensitive fields", func(t *testing.T) {
		output := `Volume Name: vol1
Used Size: 50GB
Available: 100GB`

		filtered := cli.RemoveFieldsFromCLIOutput(output, []string{"Used Size"})

		// Filtered fields are removed but not exposed to caller
		assert.NotContains(t, filtered, "Used Size")
		assert.Contains(t, filtered, "Volume Name")
		assert.Contains(t, filtered, "Available")
	})
}

func TestV1PrivateCli_ParseErrors(t *testing.T) {
	original := privateCliOperationEnabled
	privateCliOperationEnabled = true
	defer func() { privateCliOperationEnabled = original }()

	h := Handler{}
	ctx := context.Background()
	poolId := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenInvalidCommandSyntax_ShouldReturnBadRequest", func(t *testing.T) {
		// Unclosed quote should fail parsing
		req := &oasgenserver.CLIExecuteRequest{
			Input: `volume create -comment "unclosed`,
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for invalid syntax")
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "invalid CLI command")
	})
}

func TestV1PrivateCli_RuleConditions(t *testing.T) {
	original := privateCliOperationEnabled
	privateCliOperationEnabled = true
	defer func() { privateCliOperationEnabled = original }()

	h := Handler{}
	ctx := context.Background()
	poolId := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")

	t.Run("WhenVolumeCreateMissingRequiredArgs_ShouldReturnBadRequest", func(t *testing.T) {
		// volume create requires -vserver, -volume, -size
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume create -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for missing required args, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "Missing required argument")
	})

	t.Run("WhenVolumeCreateWithInvalidSpaceGuarantee_ShouldReturnBadRequest", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume create -vserver vs1 -volume vol1 -size 100g -space-guarantee invalid",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for invalid space-guarantee, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})

	// snaplock-type is no longer allowlist-validated; previously-denied values (e.g. compliance) must not be forbidden by rule conditions.
	t.Run("WhenVolumeCreateWithSnaplockTypeCompliance_ShouldNotReturnForbidden", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume create -vserver vs1 -volume vol1 -size 100g -snaplock-type compliance",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		_, isForbidden := res.(*oasgenserver.V1PrivateCliForbidden)
		assert.False(t, isForbidden, "snaplock-type is not validated; volume create with -snaplock-type compliance should not be forbidden by rule condition (got %T)", res)
	})

	t.Run("WhenVolumeCreateWithWrongSpaceEnforcementValue_ShouldReturnBadRequest", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "volume create -vserver vs1 -volume vol1 -size 100g -is-space-enforcement-logical false",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for wrong is-space-enforcement-logical value, got %T", res)
		assert.Equal(t, 400, badReq.Code)
	})
}

func TestV1PrivateCli_ShorthandCommands(t *testing.T) {
	// Test shorthand command aliases
	t.Run("WhenVolShowCommand_ShouldMatchVolumeShowRule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("vol show -vserver vs1")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
		assert.NotEmpty(t, rule.RemoveFields) // Should have same RemoveFields as "volume show"
	})

	t.Run("WhenVolCreateCommand_ShouldMatchVolumeCreateRule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("vol create -vserver vs1 -volume vol1 -size 100g")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
	})

	t.Run("WhenVolDeleteCommand_ShouldMatchVolumeDeleteRule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("vol delete -vserver vs1 -volume vol1")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
	})

	t.Run("WhenSecCertificateShowCommand_ShouldMatchSecurityCertificateShowRule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("sec certificate show")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.True(t, rule.Allow)
	})

	t.Run("WhenSecCertificateDeleteCommand_ShouldBeDenied", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("sec certificate delete -vserver vs1")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.False(t, rule.Allow)
	})
}

func TestV1PrivateCli_InjectArguments(t *testing.T) {
	t.Run("WhenVolumeCreateRule_ShouldHaveInjectArgumentsConfigured", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume create -vserver vs1 -volume vol1 -size 100g")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)

		assert.True(t, cli.HasInjectArguments(rule))
		assert.NotEmpty(t, rule.InjectArguments)
		assert.Equal(t, "true", rule.InjectArguments["-is-space-enforcement-logical"])
		assert.Equal(t, "true", rule.InjectArguments["-is-space-reporting-logical"])
	})

	t.Run("WhenApplyInjectArguments_ShouldAddMissingArguments", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume create -vserver vs1 -volume vol1 -size 100g")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)

		result := cli.ApplyInjectArguments(cmd, rule)

		assert.Contains(t, result, "-is-space-enforcement-logical true")
		assert.Contains(t, result, "-is-space-reporting-logical true")
	})

	t.Run("WhenGetInjectedArguments_ShouldReturnArgumentsToBeInjected", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume create -vserver vs1 -volume vol1 -size 100g")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)

		injected := cli.GetInjectedArguments(cmd, rule)

		assert.NotNil(t, injected)
		assert.Equal(t, "true", injected["-is-space-enforcement-logical"])
		assert.Equal(t, "true", injected["-is-space-reporting-logical"])
	})

	t.Run("WhenGetInjectedArgumentsWithExisting_ShouldExcludeThem", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume create -vserver vs1 -volume vol1 -size 100g -is-space-enforcement-logical true")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)

		injected := cli.GetInjectedArguments(cmd, rule)

		// Only the missing argument should be returned
		assert.NotNil(t, injected)
		assert.Equal(t, 1, len(injected))
		_, hasEnforcement := injected["-is-space-enforcement-logical"]
		assert.False(t, hasEnforcement, "Should not include already present argument")
		assert.Equal(t, "true", injected["-is-space-reporting-logical"])
	})
}

func TestV1PrivateCli_NoMatchingRule(t *testing.T) {
	// Test that commands without matching rules are allowed by default
	t.Run("WhenUnknownCommand_ShouldHaveNoMatchingRule", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("custom unknown command")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)

		assert.False(t, matched, "Expected no matching rule for unknown command")
		assert.Nil(t, rule, "Expected nil rule for unknown command")
	})
}
