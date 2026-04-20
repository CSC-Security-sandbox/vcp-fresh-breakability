package endpoints

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"unicode/utf8"

	oasgenserver "github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/api/ontap-proxy-servergen"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/handlers"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/middleware"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/ontap-proxy/ruleengine/cli"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/utils/middleware/log"
	"github.com/vcp-vsa-control-Plane/vsa-control-plane/workflow_engine/util"
)

// cliInputAllowedChars is the allowlist for CLI command input (OWASP: define allowed characters).
// Covers ONTAP CLI: alphanumeric, hyphen, underscore, path chars, operators, quotes, help token (?), space, tab.
var cliInputAllowedChars = regexp.MustCompile(`^[a-zA-Z0-9\-_.,:;/*><=!@+%'"? \t]+$`)

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

	if !utf8.ValidString(req.Input) {
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: "CLI command input contains invalid UTF-8",
		}, nil
	}

	if !cliInputAllowedChars.MatchString(req.Input) {
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: "CLI command input contains disallowed characters",
		}, nil
	}

	chain, err := cli.ParseCLIChain(req.Input)
	if err != nil {
		logger.WarnContext(ctx, "Failed to parse CLI command", "input", log.Sanitize(req.Input), "error", err)
		return &oasgenserver.V1PrivateCliBadRequest{
			Code:    400,
			Message: fmt.Sprintf("invalid CLI command: %s", err.Error()),
		}, nil
	}
	cliCmd := chain.PrimaryCommand

	// In diagnostic/advanced mode, the primary command must be in the respective allowlist.
	// In normal mode, use standard CLI rules (allow by default if no rule matches).
	var rule *cli.CLIRule
	switch {
	case chain.IsDiagMode():
		diagRule, matched := cli.MatchPrivilegedRule(cliCmd, cli.GetDiagAllowedRules())
		if !matched {
			logger.WarnContext(ctx, "CLI command not in diagnostic mode allowlist",
				"command", cliCmd.FullCommand,
			)
			return &oasgenserver.V1PrivateCliBadRequest{
				Code:    400,
				Message: fmt.Sprintf("Command %q is not allowed in diagnostic mode", cliCmd.FullCommand),
			}, nil
		}
		rule = diagRule
	case chain.IsAdvancedMode():
		advRule, matched := cli.MatchPrivilegedRule(cliCmd, cli.GetAdvancedAllowedRules())
		if !matched {
			logger.WarnContext(ctx, "CLI command not in advanced mode allowlist",
				"command", cliCmd.FullCommand,
			)
			return &oasgenserver.V1PrivateCliBadRequest{
				Code:    400,
				Message: fmt.Sprintf("Command %q is not allowed in advanced mode", cliCmd.FullCommand),
			}, nil
		}
		rule = advRule
	default:
		rule, _ = cli.MatchCLIRule(cliCmd)
	}

	if rule != nil {
		allowed, reason := cli.EvaluateRule(rule, cliCmd)
		if !allowed {
			logger.WarnContext(ctx, "CLI command denied by rule",
				"command", cliCmd.FullCommand,
				"pattern", rule.Pattern,
				"reason", reason,
			)
			return &oasgenserver.V1PrivateCliBadRequest{
				Code:    400,
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

	// Add backend metrics context so OntapClient records to ontap_proxy_backend_* (same as passthrough)
	ctx = middleware.AddBackendMetricsToContext(ctx, params.ProjectNumber, params.PoolId.String(), "/api/private/cli")

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

	privilege := privilegeFromChain(chain, req)

	commandToExecute := cliCmd.RawInput
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

	// Send the full chained command (e.g. "set diag; vol show ...") so ONTAP handles privilege escalation inline rather than relying solely on the privilege field,
	// which requires the authenticated user's role to permit that privilege level directly.
	fullCommand := chain.BuildCommand(commandToExecute)

	cliResponse, err := ontapClient.ExecuteCLI(ctx, fullCommand, privilege)
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

	output := handlers.StripOntapLoginBanner(cliResponse.Output)
	if rule != nil && len(rule.RemoveFields) > 0 {
		output = cli.RemoveFieldsFromCLIOutput(output, rule.RemoveFields)
	}

	logger.InfoContext(ctx, "CLI command executed successfully", "command", cliCmd.FullCommand)

	return &oasgenserver.CLIExecuteResponse{
		Output: oasgenserver.NewOptString(output),
	}, nil
}

// privilegeFromChain returns the ONTAP privilege level for the CLI request.
// When the command chain includes a "set diag" or "set advanced" prefix, the
// privilege is derived from the chain so ONTAP executes at the correct level.
// The set-prefix is not forwarded in the command body because the ONTAP
// private CLI API honours the privilege field directly.
func privilegeFromChain(chain *cli.CLIChain, req *oasgenserver.CLIExecuteRequest) string {
	switch {
	case chain.IsDiagMode():
		return "diagnostic"
	case chain.IsAdvancedMode():
		return "advanced"
	default:
		return string(req.Privilege.Or(oasgenserver.CLIExecuteRequestPrivilegeAdmin))
	}
}
