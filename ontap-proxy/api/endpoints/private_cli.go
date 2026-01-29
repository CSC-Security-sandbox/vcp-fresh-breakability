package endpoints

import (
	"context"
	"errors"
	"fmt"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/cli"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// V1PrivateCli executes an ONTAP CLI command through the private CLI API.
func (h Handler) V1PrivateCli(
	ctx context.Context,
	req *oasgenserver.CLIExecuteRequest,
	params oasgenserver.V1PrivateCliParams,
) (oasgenserver.V1PrivateCliRes, error) {
	logger := util.GetLogger(ctx)

	if !privateCliOperationEnabled {
		logger.Debug("V1PrivateCli: operation is disabled")
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: "Private CLI operation is disabled",
		}, nil
	}

	logger.InfoContext(ctx, "Processing CLI execute request",
		"projectNumber", params.ProjectNumber,
		"poolId", params.PoolId.String(),
	)

	if req.Input == "" {
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: "CLI command input is required",
		}, nil
	}

	cliCmd, err := cli.ParseCLICommand(req.Input)
	if err != nil {
		logger.WarnContext(ctx, "Failed to parse CLI command", "input", log.Sanitize(req.Input), "error", err)
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: fmt.Sprintf("invalid CLI command: %s", err.Error()),
		}, nil
	}

	// Match against CLI rules (allow by default if no rule matches)
	rule, matched := cli.MatchCLIRule(cliCmd)
	if matched {
		allowed, reason := cli.EvaluateRule(rule, cliCmd)
		if !allowed {
			logger.WarnContext(ctx, "CLI command denied by rule",
				"command", cliCmd.FullCommand,
				"pattern", rule.Pattern,
				"reason", reason,
			)
			return &oasgenserver.V1PrivateCliForbidden{
				Code:    403,
				Message: reason,
			}, nil
		}
	}

	ctx, err = middleware.SetupCredentialsForHandler(
		ctx,
		params.ProjectNumber,
		params.PoolId.String(),
		middleware.CredentialTypeExpertModeUser,
	)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to setup credentials", "error", err)
		return &oasgenserver.V1PrivateCliUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	if err := middleware.EnsureCertificateOrPassword(ctx); err != nil {
		logger.ErrorContext(ctx, "Failed to setup certificate/password", "error", err)
		return &oasgenserver.V1PrivateCliUnauthorized{
			Code:    401,
			Message: fmt.Sprintf("authentication error: %s", err.Error()),
		}, nil
	}

	// External validators require auth data from context
	if cli.HasExternalValidator(rule) {
		allowed, reason := cli.EvaluateExternalValidator(ctx, rule, cliCmd)
		if !allowed {
			logger.WarnContext(ctx, "CLI command denied by external validator",
				"command", cliCmd.FullCommand,
				"reason", reason,
			)
			return &oasgenserver.V1PrivateCliBadRequest{
				Code:    400,
				Message: reason,
			}, nil
		}
	}

	privilege := getPrivilegeLevel(req)

	commandToExecute := req.Input
	if cli.HasInjectArguments(rule) {
		commandToExecute = cli.ApplyInjectArguments(cliCmd, rule)
		if injectedArgs := cli.GetInjectedArguments(cliCmd, rule); len(injectedArgs) > 0 {
			logger.InfoContext(ctx, "Injected arguments into CLI command",
				"injectedArgs", injectedArgs,
				"modifiedCommand", commandToExecute,
			)
		}
	}

	ontapClient, err := handlers.NewOntapClientFromContext(ctx)
	if err != nil {
		logger.ErrorContext(ctx, "Failed to get ONTAP client", "error", err)
		return &oasgenserver.V1PrivateCliInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("failed to connect to ONTAP: %s", err.Error()),
		}, nil
	}

	logger.InfoContext(ctx, "Executing CLI command on ONTAP",
		"command", cliCmd.FullCommand,
		"privilege", privilege,
	)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, commandToExecute, privilege)
	if err != nil {
		logger.ErrorContext(ctx, "CLI execution failed", "command", cliCmd.FullCommand, "error", log.Sanitize(err.Error()))
		var cliErr *handlers.OntapCLIError
		if errors.As(err, &cliErr) {
			switch cliErr.StatusCode {
			case 403:
				return &oasgenserver.V1PrivateCliForbidden{Code: 403, Message: cliErr.Message}, nil
			case 404:
				return &oasgenserver.V1PrivateCliNotFound{Code: 404, Message: cliErr.Message}, nil
			default:
				return &oasgenserver.V1PrivateCliBadRequest{Code: cliErr.StatusCode, Message: cliErr.Message}, nil
			}
		}
		return &oasgenserver.V1PrivateCliInternalServerError{
			Code:    500,
			Message: fmt.Sprintf("ONTAP CLI error: %s", err.Error()),
		}, nil
	}

	output := cliResponse.Output
	if rule != nil && len(rule.RemoveFields) > 0 {
		output = cli.RemoveFieldsFromCLIOutput(output, rule.RemoveFields)
	}

	logger.InfoContext(ctx, "CLI command executed successfully", "command", cliCmd.FullCommand)

	return &oasgenserver.CLIExecuteResponse{
		Output: oasgenserver.NewOptString(output),
	}, nil
}

func getPrivilegeLevel(req *oasgenserver.CLIExecuteRequest) string {
	return string(req.Privilege.Or(oasgenserver.CLIExecuteRequestPrivilegeAdmin))
}
