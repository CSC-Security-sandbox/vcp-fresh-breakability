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

func TestPrivilegeFromChain(t *testing.T) {
	t.Run("returns admin by default for simple command", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{Input: "volume show"}
		chain, err := cli.ParseCLIChain(req.Input)
		require.NoError(t, err)
		assert.Equal(t, "admin", privilegeFromChain(chain, req))
	})

	t.Run("returns admin when admin is set explicitly", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input:     "volume show",
			Privilege: oasgenserver.NewOptCLIExecuteRequestPrivilege(oasgenserver.CLIExecuteRequestPrivilegeAdmin),
		}
		chain, err := cli.ParseCLIChain(req.Input)
		require.NoError(t, err)
		assert.Equal(t, "admin", privilegeFromChain(chain, req))
	})

	t.Run("returns diagnostic for set diag chain", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{Input: "set diag; volume show"}
		chain, err := cli.ParseCLIChain(req.Input)
		require.NoError(t, err)
		assert.Equal(t, "diagnostic", privilegeFromChain(chain, req))
	})

	t.Run("returns diagnostic for set -privilege diagnostic chain", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{Input: "set -privilege diagnostic; volume show"}
		chain, err := cli.ParseCLIChain(req.Input)
		require.NoError(t, err)
		assert.Equal(t, "diagnostic", privilegeFromChain(chain, req))
	})

	t.Run("returns advanced for set advanced chain", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{Input: "set advanced; volume show"}
		chain, err := cli.ParseCLIChain(req.Input)
		require.NoError(t, err)
		assert.Equal(t, "advanced", privilegeFromChain(chain, req))
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

	t.Run("diag + allowlisted command passes validation", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; volume check metadata",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)
		badReq, isBadReq := res.(*oasgenserver.V1PrivateCliBadRequest)
		if isBadReq {
			require.False(t, isBadReq,
				"volume check metadata should pass allowlist validation, got BadRequest: %s", badReq.Message)
		}
	})

	t.Run("diag + volume show not in allowlist rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; volume show -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for volume show in diag mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in diagnostic mode")
	})

	t.Run("diag + command not in allowlist rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; security certificate delete -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for command not in diag allowlist, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in diagnostic mode")
	})

	t.Run("diag + volume create not in allowlist rejected", func(t *testing.T) {
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
		require.True(t, ok, "Expected bad request for volume create in diag mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in diagnostic mode")
	})

	t.Run("diag + unknown command rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; system node show",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for unknown command in diag mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in diagnostic mode")
	})

	t.Run("set -privilege diagnostic variant also enforces diag allowlist", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set -privilege diagnostic; snapshot show -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for snapshot show in diag mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in diagnostic mode")
	})

	t.Run("advanced + statistics show passes validation", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; statistics show",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)
		badReq, isBadReq := res.(*oasgenserver.V1PrivateCliBadRequest)
		if isBadReq {
			require.False(t, isBadReq,
				"statistics show should pass allowlist validation, got BadRequest: %s", badReq.Message)
		}
	})

	t.Run("advanced + volume show in allowlist passes validation", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; volume show -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)
		badReq, isBadReq := res.(*oasgenserver.V1PrivateCliBadRequest)
		if isBadReq {
			require.False(t, isBadReq,
				"volume show should pass allowlist validation, got BadRequest: %s", badReq.Message)
		}
	})

	t.Run("advanced + volume check metadata not in allowlist rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; volume check metadata",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for volume check metadata in advanced mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in advanced mode")
	})

	t.Run("advanced + command not in allowlist rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; security certificate delete -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for denied command in advanced mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in advanced mode")
	})

	t.Run("advanced + volume create not in allowlist rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; volume create -vserver vs1 -volume vol1 -size 100g",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for volume create in advanced mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in advanced mode")
	})

	t.Run("set -privilege advanced variant also enforces advanced allowlist", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set -privilege advanced; snapshot show -vserver vs1",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected bad request for snapshot show in advanced mode, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "not allowed in advanced mode")
	})

	t.Run("advanced is not diag mode", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set advanced; statistics show",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		_, isBadReq := res.(*oasgenserver.V1PrivateCliBadRequest)
		if isBadReq {
			badReq := res.(*oasgenserver.V1PrivateCliBadRequest)
			assert.NotContains(t, badReq.Message, "diagnostic mode",
				"advanced mode should not produce diagnostic mode errors")
		}
	})

	t.Run("chained non-set first command rejected", func(t *testing.T) {
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
		require.True(t, ok, "Expected V1PrivateCliBadRequest when first command is not 'set', got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "first command in a chain must be 'set'")
	})

	t.Run("more than 2 chained commands rejected", func(t *testing.T) {
		req := &oasgenserver.CLIExecuteRequest{
			Input: "set diag; volume show; snapshot show",
		}
		params := oasgenserver.V1PrivateCliParams{
			ProjectNumber: "123456789",
			LocationId:    "us-east1",
			PoolId:        poolId,
		}

		res, err := h.V1PrivateCli(ctx, req, params)
		require.NoError(t, err)

		badReq, ok := res.(*oasgenserver.V1PrivateCliBadRequest)
		require.True(t, ok, "Expected V1PrivateCliBadRequest for >2 chained commands, got %T", res)
		assert.Equal(t, 400, badReq.Code)
		assert.Contains(t, badReq.Message, "at most 2 commands")
	})

	t.Run("question mark help token is allowed by input validator", func(t *testing.T) {
		assert.True(t, cliInputAllowedChars.MatchString("?"))
		assert.True(t, cliInputAllowedChars.MatchString("vol create ?"))
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

func TestV1PrivateCli_DiagResponseFiltering(t *testing.T) {
	t.Run("normal volume show has Physical Used Percent in RemoveFields", func(t *testing.T) {
		cmd, err := cli.ParseCLICommand("volume show")
		require.NoError(t, err)

		rule, matched := cli.MatchCLIRule(cmd)
		require.True(t, matched)
		assert.Contains(t, rule.RemoveFields, "Physical Used Percent",
			"Physical Used Percent should be filtered in normal mode to prevent leaking physical usage ratio")
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
